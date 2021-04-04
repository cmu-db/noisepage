#include "common/error/exception.h"

#include "binder/bind_node_visitor.h"

#include "execution/ast/ast.h"
#include "execution/ast/ast_clone.h"
#include "execution/ast/context.h"

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "execution/exec/execution_settings.h"

#include "catalog/catalog_accessor.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/statistics/stats_storage.h"

#include "traffic_cop/traffic_cop_util.h"

#include "parser/expression/constant_value_expression.h"
#include "parser/postgresparser.h"

#include "execution/ast/udf/udf_ast_nodes.h"
#include "execution/compiler/udf/udf_codegen.h"

#include "planner/plannodes/abstract_plan_node.h"

// TODO(Kyle): Documentation.

namespace noisepage {
namespace execution {
namespace compiler {
namespace udf {

UDFCodegen::UDFCodegen(catalog::CatalogAccessor *accessor, FunctionBuilder *fb,
                       ast::udf::UDFASTContext *udf_ast_context, CodeGen *codegen, catalog::db_oid_t db_oid)
    : accessor_{accessor},
      fb_{fb},
      udf_ast_context_{udf_ast_context},
      codegen_{codegen},
      aux_decls_(codegen->GetAstContext()->GetRegion()),
      db_oid_{db_oid} {
  for (size_t i = 0; fb->GetParameterByPosition(i) != nullptr; i++) {
    auto param = fb->GetParameterByPosition(i);
    const auto &name = param->As<execution::ast::IdentifierExpr>()->Name();
    str_to_ident_.emplace(name.GetString(), name);
  }
}

const char *UDFCodegen::GetReturnParamString() { return "return_val"; }

void UDFCodegen::GenerateUDF(ast::udf::AbstractAST *ast) { ast->Accept(this); }

void UDFCodegen::Visit(ast::udf::AbstractAST *ast) { UNREACHABLE("Not implemented"); }

void UDFCodegen::Visit(ast::udf::DynamicSQLStmtAST *ast) { UNREACHABLE("Not implemented"); }

catalog::type_oid_t UDFCodegen::GetCatalogTypeOidFromSQLType(execution::ast::BuiltinType::Kind type) {
  switch (type) {
    case execution::ast::BuiltinType::Kind::Integer: {
      return accessor_->GetTypeOidFromTypeId(type::TypeId::INTEGER);
    }
    case execution::ast::BuiltinType::Kind::Boolean: {
      return accessor_->GetTypeOidFromTypeId(type::TypeId::BOOLEAN);
    }
    default:
      return accessor_->GetTypeOidFromTypeId(type::TypeId::INVALID);
      NOISEPAGE_ASSERT(false, "Unsupported param type");
  }
}

execution::ast::File *UDFCodegen::Finish() {
  auto fn = fb_->Finish();
  //  util::RegionVector<ast::Decl *> decls_reg_vec{decls->begin(), decls->end(), codegen.Region()};
  execution::util::RegionVector<execution::ast::Decl *> decls({fn}, codegen_->GetAstContext()->GetRegion());
  //    for(auto decl : aux_decls_){
  //      decls.push_back(decl);
  //    }
  decls.insert(decls.begin(), aux_decls_.begin(), aux_decls_.end());
  auto file = codegen_->GetAstContext()->GetNodeFactory()->NewFile({0, 0}, std::move(decls));
  return file;
}

void UDFCodegen::Visit(ast::udf::CallExprAST *ast) {
  auto &args = ast->args;
  std::vector<execution::ast::Expr *> args_ast;
  std::vector<execution::ast::Expr *> args_ast_region_vec;
  std::vector<catalog::type_oid_t> arg_types;

  for (auto &arg : args) {
    arg->Accept(this);
    args_ast.push_back(dst_);
    args_ast_region_vec.push_back(dst_);
    auto *builtin = dst_->GetType()->SafeAs<execution::ast::BuiltinType>();
    NOISEPAGE_ASSERT(builtin != nullptr, "Not builtin parameter");
    NOISEPAGE_ASSERT(builtin->IsSqlValueType(), "Param is not SQL Value Type");
    arg_types.push_back(GetCatalogTypeOidFromSQLType(builtin->GetKind()));
  }
  auto proc_oid = accessor_->GetProcOid(ast->callee, arg_types);
  NOISEPAGE_ASSERT(proc_oid != catalog::INVALID_PROC_OID, "Invalid call");

  auto context = accessor_->GetProcCtxPtr(proc_oid);
  if (context->IsBuiltin()) {
    fb_->Append(codegen_->MakeStmt(codegen_->CallBuiltin(context->GetBuiltin(), std::move(args_ast))));
  } else {
    auto it = str_to_ident_.find(ast->callee);
    execution::ast::Identifier ident_expr;
    if (it != str_to_ident_.end()) {
      ident_expr = it->second;
    } else {
      auto file = reinterpret_cast<execution::ast::File *>(
          execution::ast::AstClone::Clone(context->GetFile(), codegen_->GetAstContext()->GetNodeFactory(), "",
                                          context->GetASTContext(), codegen_->GetAstContext().Get()));
      for (auto decl : file->Declarations()) {
        aux_decls_.push_back(decl);
      }
      ident_expr = codegen_->MakeFreshIdentifier(file->Declarations().back()->Name().GetString());
      str_to_ident_[file->Declarations().back()->Name().GetString()] = ident_expr;
    }
    fb_->Append(codegen_->MakeStmt(codegen_->Call(ident_expr, args_ast_region_vec)));
  }

  // fb_->Append(codegen_->Call)
}

void UDFCodegen::Visit(ast::udf::StmtAST *ast) { UNREACHABLE("Not implemented"); }

void UDFCodegen::Visit(ast::udf::ExprAST *ast) { UNREACHABLE("Not implemented"); }

void UDFCodegen::Visit(ast::udf::DeclStmtAST *ast) {
  if (ast->name == "*internal*") {
    return;
  }
  execution::ast::Identifier ident = codegen_->MakeFreshIdentifier(ast->name);
  str_to_ident_.emplace(ast->name, ident);
  auto prev_type = current_type_;
  execution::ast::Expr *tpl_type = nullptr;
  if (ast->type == type::TypeId::INVALID) {
    // record type
    execution::util::RegionVector<execution::ast::FieldDecl *> fields(codegen_->GetAstContext()->GetRegion());
    for (auto p : udf_ast_context_->GetRecordType(ast->name)) {
      fields.push_back(codegen_->MakeField(codegen_->MakeIdentifier(p.first),
                                           codegen_->TplType(execution::sql::GetTypeId(p.second))));
    }
    auto record_decl = codegen_->DeclareStruct(codegen_->MakeFreshIdentifier("rectype"), std::move(fields));
    aux_decls_.push_back(record_decl);
    tpl_type = record_decl->TypeRepr();
  } else {
    tpl_type = codegen_->TplType(execution::sql::GetTypeId(ast->type));
  }
  current_type_ = ast->type;
  if (ast->initial != nullptr) {
    //      Visit(ast->initial.get());
    ast->initial->Accept(this);
    fb_->Append(codegen_->DeclareVar(ident, tpl_type, dst_));
  } else {
    fb_->Append(codegen_->DeclareVarNoInit(ident, tpl_type));
  }
  current_type_ = prev_type;
}

void UDFCodegen::Visit(ast::udf::FunctionAST *ast) {
  for (size_t i = 0; i < ast->param_types_.size(); i++) {
    //    auto param_type = codegen_->TplType(ast->param_types_[i]);
    str_to_ident_.emplace(ast->param_names_[i], codegen_->MakeFreshIdentifier("udf"));
  }
  ast->body.get()->Accept(this);
}

void UDFCodegen::Visit(ast::udf::VariableExprAST *ast) {
  auto it = str_to_ident_.find(ast->name);
  NOISEPAGE_ASSERT(it != str_to_ident_.end(), "variable not declared");
  dst_ = codegen_->MakeExpr(it->second);
}

void UDFCodegen::Visit(ast::udf::ValueExprAST *ast) {
  auto val = common::ManagedPointer(ast->value_).CastManagedPointerTo<parser::ConstantValueExpression>();
  if (val->IsNull()) {
    dst_ = codegen_->ConstNull(current_type_);
    return;
  }
  auto type_id = execution::sql::GetTypeId(val->GetReturnValueType());
  switch (type_id) {
    case execution::sql::TypeId::Boolean:
      dst_ = codegen_->BoolToSql(val->GetBoolVal().val_);
      break;
    case execution::sql::TypeId::TinyInt:
    case execution::sql::TypeId::SmallInt:
    case execution::sql::TypeId::Integer:
    case execution::sql::TypeId::BigInt:
      dst_ = codegen_->IntToSql(val->GetInteger().val_);
      break;
    case execution::sql::TypeId::Float:
    case execution::sql::TypeId::Double:
      dst_ = codegen_->FloatToSql(val->GetReal().val_);
    case execution::sql::TypeId::Date:
      dst_ = codegen_->DateToSql(val->GetDateVal().val_);
      break;
    case execution::sql::TypeId::Timestamp:
      dst_ = codegen_->TimestampToSql(val->GetTimestampVal().val_);
      break;
    case execution::sql::TypeId::Varchar:
      dst_ = codegen_->StringToSql(val->GetStringVal().StringView());
      break;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION("Unsupported type in UDF codegen");
  }
}

void UDFCodegen::Visit(ast::udf::AssignStmtAST *ast) {
  type::TypeId left_type = type::TypeId::INVALID;
  udf_ast_context_->GetVariableType(ast->lhs->name, &left_type);
  current_type_ = left_type;

  reinterpret_cast<ast::udf::AbstractAST *>(ast->rhs.get())->Accept(this);
  auto rhs_expr = dst_;

  auto it = str_to_ident_.find(ast->lhs->name);
  NOISEPAGE_ASSERT(it != str_to_ident_.end(), "Variable not found");
  auto left_codegen_ident = it->second;

  auto *left_expr = codegen_->MakeExpr(left_codegen_ident);

  //  auto right_type = rhs_expr->GetType()->GetTypeId();

  //  if (left_type == type::TypeId::VARCHAR) {
  fb_->Append(codegen_->Assign(left_expr, rhs_expr));
  //  }
}

void UDFCodegen::Visit(ast::udf::BinaryExprAST *ast) {
  execution::parsing::Token::Type op_token;
  bool compare = false;
  switch (ast->op) {
    case noisepage::parser::ExpressionType::OPERATOR_DIVIDE:
      op_token = execution::parsing::Token::Type::SLASH;
      break;
    case noisepage::parser::ExpressionType::OPERATOR_PLUS:
      op_token = execution::parsing::Token::Type::PLUS;
      break;
    case noisepage::parser::ExpressionType::OPERATOR_MINUS:
      op_token = execution::parsing::Token::Type::MINUS;
      break;
    case noisepage::parser::ExpressionType::OPERATOR_MULTIPLY:
      op_token = execution::parsing::Token::Type::STAR;
      break;
    case noisepage::parser::ExpressionType::OPERATOR_MOD:
      op_token = execution::parsing::Token::Type::PERCENT;
      break;
    case noisepage::parser::ExpressionType::CONJUNCTION_OR:
      op_token = execution::parsing::Token::Type::OR;
      break;
    case noisepage::parser::ExpressionType::CONJUNCTION_AND:
      op_token = execution::parsing::Token::Type::AND;
      break;
    case noisepage::parser::ExpressionType::COMPARE_GREATER_THAN:
      compare = true;
      op_token = execution::parsing::Token::Type::GREATER;
      break;
    case noisepage::parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
      compare = true;
      op_token = execution::parsing::Token::Type::GREATER_EQUAL;
      break;
    case noisepage::parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      compare = true;
      op_token = execution::parsing::Token::Type::LESS_EQUAL;
      break;
    case noisepage::parser::ExpressionType::COMPARE_LESS_THAN:
      compare = true;
      op_token = execution::parsing::Token::Type::LESS;
      break;
    case noisepage::parser::ExpressionType::COMPARE_EQUAL:
      compare = true;
      op_token = execution::parsing::Token::Type::EQUAL_EQUAL;
      break;
    default:
      // TODO(tanujnay112): figure out concatenation operation from expressions?
      UNREACHABLE("Unsupported expression");
  }
  ast->lhs->Accept(this);
  auto lhs_expr = dst_;

  ast->rhs->Accept(this);
  auto rhs_expr = dst_;
  if (compare) {
    dst_ = codegen_->Compare(op_token, lhs_expr, rhs_expr);
  } else {
    dst_ = codegen_->BinaryOp(op_token, lhs_expr, rhs_expr);
  }
}

void UDFCodegen::Visit(ast::udf::IfStmtAST *ast) {
  ast->cond_expr->Accept(this);
  auto cond = dst_;

  If branch(fb_, cond);
  ast->then_stmt->Accept(this);
  if (ast->else_stmt != nullptr) {
    branch.Else();
    ast->else_stmt->Accept(this);
  }
  branch.EndIf();
}

void UDFCodegen::Visit(ast::udf::IsNullExprAST *ast) {
  ast->child_->Accept(this);
  auto chld = dst_;
  dst_ = codegen_->CallBuiltin(execution::ast::Builtin::IsValNull, {chld});
  if (!ast->is_null_check_) {
    dst_ = codegen_->UnaryOp(execution::parsing::Token::Type::BANG, dst_);
  }
}

void UDFCodegen::Visit(ast::udf::SeqStmtAST *ast) {
  for (auto &stmt : ast->stmts) {
    stmt->Accept(this);
  }
}

void UDFCodegen::Visit(ast::udf::WhileStmtAST *ast) {
  ast->cond_expr->Accept(this);
  auto cond = dst_;
  //  cond = codegen_->Compare(execution::parsing::Token::Type::EQUAL_EQUAL, cond, )
  //  cond = codegen_->CallBuiltin(execution::ast::Builtin::SqlToBool, {cond});
  Loop loop(fb_, cond);
  ast->body_stmt->Accept(this);
  loop.EndLoop();
}

// TODO(Kyle): Implement
void UDFCodegen::Visit(ast::udf::ForStmtAST *ast) {
  throw NOT_IMPLEMENTED_EXCEPTION("Visit(ForStmtAst*) Not Implemented");
  // needs_exec_ctx_ = true;
  // const auto query = common::ManagedPointer(ast->query_);
  // auto exec_ctx = fb_->GetParameterByPosition(0);

  // // TODO(Matt): I don't think the binder should need the database name. It's already bound in the ConnectionContext
  // binder::BindNodeVisitor visitor(common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_oid_);

  // auto query_params = visitor.BindAndGetUDFParams(query, common::ManagedPointer(udf_ast_context_));

  // auto stats = optimizer::StatsStorage();

  // std::unique_ptr<planner::AbstractPlanNode> plan = trafficcop::TrafficCopUtil::Optimize(
  //     accessor_->GetTxn(), common::ManagedPointer(accessor_), query, db_oid_, common::ManagedPointer(&stats),
  //     std::make_unique<optimizer::TrivialCostModel>(), 1000000);
  // // make lambda that just writes into this
  // std::vector<execution::ast::Identifier> var_idents;
  // auto lam_var = codegen_->MakeFreshIdentifier("looplamb");
  // execution::util::RegionVector<execution::ast::FieldDecl *> params(codegen_->GetAstContext()->GetRegion());
  // params.push_back(codegen_->MakeField(
  //     exec_ctx->As<execution::ast::IdentifierExpr>()->Name(),
  //     codegen_->PointerType(codegen_->BuiltinType(execution::ast::BuiltinType::Kind::ExecutionContext))));
  // size_t i = 0;
  // for (auto var : ast->vars_) {
  //   var_idents.push_back(str_to_ident_.find(var)->second);
  //   auto var_ident = var_idents.back();
  //   //  NOISEPAGE_ASSERT(plan->GetOutputSchema()->GetColumns().size() == 1, "Can't support non scalars yet!");
  //   auto type = codegen_->TplType(execution::sql::GetTypeId(plan->GetOutputSchema()->GetColumn(i).GetType()));

  //   fb_->Append(codegen_->Assign(codegen_->MakeExpr(var_ident),
  //                                codegen_->ConstNull(plan->GetOutputSchema()->GetColumn(i).GetType())));
  //   auto input = codegen_->MakeFreshIdentifier(var);
  //   params.push_back(codegen_->MakeField(input, type));
  //   i++;
  // }
  // execution::ast::LambdaExpr *lambda_expr;
  // FunctionBuilder fn(codegen_, std::move(params), codegen_->BuiltinType(execution::ast::BuiltinType::Nil));
  // {
  //   size_t j = 1;
  //   for (auto var : var_idents) {
  //     fn.Append(codegen_->Assign(codegen_->MakeExpr(var), fn.GetParameterByPosition(j)));
  //     j++;
  //   }
  //   auto prev_fb = fb_;
  //   fb_ = &fn;
  //   ast->body_stmt_->Accept(this);
  //   fb_ = prev_fb;
  // }

  // execution::util::RegionVector<execution::ast::Expr *> captures(codegen_->GetAstContext()->GetRegion());
  // for (auto it : str_to_ident_) {
  //   if (it.first == "executionCtx") {
  //     continue;
  //   }
  //   captures.push_back(codegen_->MakeExpr(it.second));
  // }

  // lambda_expr = fn.FinishLambda(std::move(captures));
  // lambda_expr->SetName(lam_var);

  // // want to pass something down that will materialize the lambda function for me into lambda_expr and will
  // // also feed in a lambda_expr to the compiler
  // execution::exec::ExecutionSettings exec_settings{};
  // const std::string dummy_query = "";
  // auto exec_query = execution::compiler::CompilationContext::Compile(
  //     *plan, exec_settings, accessor_, execution::compiler::CompilationMode::OneShot,
  //     common::ManagedPointer<const std::string>(&dummy_query), lambda_expr, codegen_->GetAstContext());
  // auto fns = exec_query->GetFunctions();
  // auto decls = exec_query->GetDecls();

  // aux_decls_.insert(aux_decls_.end(), decls.begin(), decls.end());

  // fb_->Append(
  //     codegen_->DeclareVar(lam_var, codegen_->LambdaType(lambda_expr->GetFunctionLitExpr()->TypeRepr()),
  //     lambda_expr));

  // // make query state
  // auto query_state = codegen_->MakeFreshIdentifier("query_state");
  // fb_->Append(codegen_->DeclareVarNoInit(query_state, codegen_->MakeExpr(exec_query->GetQueryStateType()->Name())));
  // // set its execution context to whatever exec context was passed in here
  // fb_->Append(codegen_->CallBuiltin(execution::ast::Builtin::StartNewParams, {exec_ctx}));
  // std::vector<std::unordered_map<std::string, std::pair<std::string, size_t>>::iterator> sorted_vec;
  // for (auto it = query_params.begin(); it != query_params.end(); it++) {
  //   sorted_vec.push_back(it);
  // }

  // std::sort(sorted_vec.begin(), sorted_vec.end(), [](auto x, auto y) { return x->second < y->second; });
  // for (auto entry : sorted_vec) {
  //   // TODO(order these dudes)
  //   type::TypeId type = type::TypeId::INVALID;
  //   udf_ast_context_->GetVariableType(entry->first, &type);
  //   //    NOISEPAGE_ASSERT(ret, "didn't find param in udf ast context");

  //   execution::ast::Builtin builtin;
  //   switch (type) {
  //     case type::TypeId::BOOLEAN:
  //       builtin = execution::ast::Builtin::AddParamBool;
  //       break;
  //     case type::TypeId::TINYINT:
  //       builtin = execution::ast::Builtin::AddParamTinyInt;
  //       break;
  //     case type::TypeId::SMALLINT:
  //       builtin = execution::ast::Builtin::AddParamSmallInt;
  //       break;
  //     case type::TypeId::INTEGER:
  //       builtin = execution::ast::Builtin::AddParamInt;
  //       break;
  //     case type::TypeId::BIGINT:
  //       builtin = execution::ast::Builtin::AddParamBigInt;
  //       break;
  //     case type::TypeId::DECIMAL:
  //       builtin = execution::ast::Builtin::AddParamDouble;
  //       break;
  //     case type::TypeId::DATE:
  //       builtin = execution::ast::Builtin::AddParamDate;
  //       break;
  //     case type::TypeId::TIMESTAMP:
  //       builtin = execution::ast::Builtin::AddParamTimestamp;
  //       break;
  //     case type::TypeId::VARCHAR:
  //       builtin = execution::ast::Builtin::AddParamString;
  //       break;
  //     default:
  //       UNREACHABLE("Unsupported parameter type");
  //   }
  //   fb_->Append(codegen_->CallBuiltin(builtin, {exec_ctx, codegen_->MakeExpr(str_to_ident_[entry->first])}));
  // }
  // // set param 1
  // // set param 2
  // // etc etc
  // fb_->Append(codegen_->Assign(
  //     codegen_->AccessStructMember(codegen_->MakeExpr(query_state), codegen_->MakeIdentifier("execCtx")), exec_ctx));
  // // set its execution context to whatever exec context was passed in here

  // for (auto &sub_fn : fns) {
  //   //    aux_decls_.push_back(c)
  //   if (sub_fn.find("Run") != std::string::npos) {
  //     fb_->Append(codegen_->Call(codegen_->GetAstContext()->GetIdentifier(sub_fn),
  //                                {codegen_->AddressOf(query_state), codegen_->MakeExpr(lam_var)}));
  //   } else {
  //     fb_->Append(codegen_->Call(codegen_->GetAstContext()->GetIdentifier(sub_fn),
  //     {codegen_->AddressOf(query_state)}));
  //   }
  // }

  // fb_->Append(codegen_->CallBuiltin(execution::ast::Builtin::FinishNewParams, {exec_ctx}));

  // return;
}

void UDFCodegen::Visit(ast::udf::RetStmtAST *ast) {
  ast->expr->Accept(reinterpret_cast<ASTNodeVisitor *>(this));
  auto ret_expr = dst_;
  fb_->Append(codegen_->Return(ret_expr));
}

// TODO(Kyle): Implement
void UDFCodegen::Visit(ast::udf::SQLStmtAST *ast) {
  throw NOT_IMPLEMENTED_EXCEPTION("Visit(SQLStmtAST*) Not Implemented");
  // needs_exec_ctx_ = true;
  // auto exec_ctx = fb_->GetParameterByPosition(0);
  // const auto query = common::ManagedPointer(ast->query);

  // // TODO(Matt): I don't think the binder should need the database name. It's already bound in the ConnectionContext
  // binder::BindNodeVisitor visitor(common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_oid_);

  // TODO(Kyle): Implement
  // //  auto query_params = visitor.BindAndGetUDFParams(query, common::ManagedPointer(udf_ast_context_));
  // auto query_params = ast->udf_params;
  // auto stats = optimizer::StatsStorage();

  // std::unique_ptr<planner::AbstractPlanNode> plan = trafficcop::TrafficCopUtil::Optimize(
  //     accessor_->GetTxn(), common::ManagedPointer(accessor_), query, db_oid_, common::ManagedPointer(&stats),
  //     std::make_unique<optimizer::TrivialCostModel>(), 1000000);
  // // make lambda that just writes into this

  // auto lam_var = codegen_->MakeFreshIdentifier("lamb");
  // //  NOISEPAGE_ASSERT(plan->GetOutputSchema()->GetColumns().size() == 1, "Can't support non scalars yet!");
  // auto &cols = plan->GetOutputSchema()->GetColumns();
  // //  auto &col = cols[0];
  // execution::util::RegionVector<execution::ast::FieldDecl *> params(codegen_->GetAstContext()->GetRegion());
  // std::vector<execution::ast::Expr *> assignees;
  // execution::util::RegionVector<execution::ast::Expr *> captures(codegen_->GetAstContext()->GetRegion());
  // size_t i = 0;
  // params.push_back(codegen_->MakeField(
  //     exec_ctx->As<execution::ast::IdentifierExpr>()->Name(),
  //     codegen_->PointerType(codegen_->BuiltinType(execution::ast::BuiltinType::Kind::ExecutionContext))));
  // for (auto &col : cols) {
  //   execution::ast::Expr *capture_var = codegen_->MakeExpr(str_to_ident_.find(ast->var_name)->second);
  //   type::TypeId udf_type;
  //   udf_ast_context_->GetVariableType(ast->var_name, &udf_type);
  //   if (udf_type == type::TypeId::INVALID) {
  //     // record type
  //     auto &struct_vars = udf_ast_context_->GetRecordType(ast->var_name);
  //     if (captures.empty()) {
  //       captures.push_back(capture_var);
  //     }
  //     capture_var = codegen_->AccessStructMember(capture_var, codegen_->MakeIdentifier(struct_vars[i].first));
  //     assignees.push_back(capture_var);
  //   } else {
  //     assignees.push_back(capture_var);
  //     captures.push_back(capture_var);
  //   }
  //   //    auto capture_var = str_to_ident_.find(ast->var_name)->second;
  //   auto type = codegen_->TplType(execution::sql::GetTypeId(col.GetType()));

  //   auto input_param = codegen_->MakeFreshIdentifier("input");
  //   params.push_back(codegen_->MakeField(input_param, type));
  //   i++;
  // }

  // execution::ast::LambdaExpr *lambda_expr;
  // FunctionBuilder fn(codegen_, std::move(params), codegen_->BuiltinType(execution::ast::BuiltinType::Nil));
  // {
  //   for (size_t j = 0; j < assignees.size(); j++) {
  //     auto capture_var = assignees[j];
  //     auto input_param = fn.GetParameterByPosition(j + 1);
  //     fn.Append(codegen_->Assign(capture_var, input_param));
  //   }
  // }

  // lambda_expr = fn.FinishLambda(std::move(captures));
  // lambda_expr->SetName(lam_var);

  // // want to pass something down that will materialize the lambda function for me into lambda_expr and will
  // // also feed in a lambda_expr to the compiler
  // execution::exec::ExecutionSettings exec_settings{};
  // const std::string dummy_query = "";
  // auto exec_query = execution::compiler::CompilationContext::Compile(
  //     *plan, exec_settings, accessor_, execution::compiler::CompilationMode::OneShot,
  //     common::ManagedPointer<const std::string>(&dummy_query), lambda_expr, codegen_->GetAstContext());
  // auto fns = exec_query->GetFunctions();
  // auto decls = exec_query->GetDecls();

  // aux_decls_.insert(aux_decls_.end(), decls.begin(), decls.end());

  // fb_->Append(
  //     codegen_->DeclareVar(lam_var, codegen_->LambdaType(lambda_expr->GetFunctionLitExpr()->TypeRepr()),
  //     lambda_expr));

  // // make query state
  // auto query_state = codegen_->MakeFreshIdentifier("query_state");
  // fb_->Append(codegen_->DeclareVarNoInit(query_state, codegen_->MakeExpr(exec_query->GetQueryStateType()->Name())));
  // // set its execution context to whatever exec context was passed in here
  // fb_->Append(codegen_->CallBuiltin(execution::ast::Builtin::StartNewParams, {exec_ctx}));
  // std::vector<std::unordered_map<std::string, std::pair<std::string, size_t>>::iterator> sorted_vec;
  // for (auto it = query_params.begin(); it != query_params.end(); it++) {
  //   sorted_vec.push_back(it);
  // }

  // std::sort(sorted_vec.begin(), sorted_vec.end(), [](auto x, auto y) { return x->second.second < y->second.second;
  // }); for (auto entry : sorted_vec) {
  //   // TODO(order these dudes)
  //   type::TypeId type = type::TypeId::INVALID;
  //   execution::ast::Expr *expr = nullptr;
  //   if (entry->second.first.length() > 0) {
  //     auto &fields = udf_ast_context_->GetRecordType(entry->second.first);
  //     auto it = std::find_if(fields.begin(), fields.end(), [=](auto p) { return p.first == entry->first; });
  //     type = it->second;
  //     expr = codegen_->AccessStructMember(codegen_->MakeExpr(str_to_ident_[entry->second.first]),
  //                                         codegen_->MakeIdentifier(entry->first));
  //   } else {
  //     udf_ast_context_->GetVariableType(entry->first, &type);
  //     expr = codegen_->MakeExpr(str_to_ident_[entry->first]);
  //   }

  //   //    NOISEPAGE_ASSERT(ret, "didn't find param in udf ast context");
  //   execution::ast::Builtin builtin;
  //   switch (type) {
  //     case type::TypeId::BOOLEAN:
  //       builtin = execution::ast::Builtin::AddParamBool;
  //       break;
  //     case type::TypeId::TINYINT:
  //       builtin = execution::ast::Builtin::AddParamTinyInt;
  //       break;
  //     case type::TypeId::SMALLINT:
  //       builtin = execution::ast::Builtin::AddParamSmallInt;
  //       break;
  //     case type::TypeId::INTEGER:
  //       builtin = execution::ast::Builtin::AddParamInt;
  //       break;
  //     case type::TypeId::BIGINT:
  //       builtin = execution::ast::Builtin::AddParamBigInt;
  //       break;
  //     case type::TypeId::DECIMAL:
  //       builtin = execution::ast::Builtin::AddParamDouble;
  //       break;
  //     case type::TypeId::DATE:
  //       builtin = execution::ast::Builtin::AddParamDate;
  //       break;
  //     case type::TypeId::TIMESTAMP:
  //       builtin = execution::ast::Builtin::AddParamTimestamp;
  //       break;
  //     case type::TypeId::VARCHAR:
  //       builtin = execution::ast::Builtin::AddParamString;
  //       break;
  //     default:
  //       UNREACHABLE("Unsupported parameter type");
  //   }
  //   fb_->Append(codegen_->CallBuiltin(builtin, {exec_ctx, expr}));
  // }
  // // set param 1
  // // set param 2
  // // etc etc
  // fb_->Append(codegen_->Assign(
  //     codegen_->AccessStructMember(codegen_->MakeExpr(query_state), codegen_->MakeIdentifier("execCtx")), exec_ctx));

  // for (auto &col : cols) {
  //   execution::ast::Expr *capture_var = codegen_->MakeExpr(str_to_ident_.find(ast->var_name)->second);
  //   auto lhs = capture_var;
  //   if (cols.size() > 1) {
  //     // record struct type
  //     lhs = codegen_->AccessStructMember(capture_var, codegen_->MakeIdentifier(col.GetName()));
  //   }
  //   fb_->Append(codegen_->Assign(lhs, codegen_->ConstNull(col.GetType())));
  // }
  // // set its execution context to whatever exec context was passed in here

  // for (auto &sub_fn : fns) {
  //   //    aux_decls_.push_back(c)
  //   if (sub_fn.find("Run") != std::string::npos) {
  //     fb_->Append(codegen_->Call(codegen_->GetAstContext()->GetIdentifier(sub_fn),
  //                                {codegen_->AddressOf(query_state), codegen_->MakeExpr(lam_var)}));
  //   } else {
  //     fb_->Append(codegen_->Call(codegen_->GetAstContext()->GetIdentifier(sub_fn),
  //     {codegen_->AddressOf(query_state)}));
  //   }
  // }

  // fb_->Append(codegen_->CallBuiltin(execution::ast::Builtin::FinishNewParams, {exec_ctx}));

  // return;
}

void UDFCodegen::Visit(ast::udf::MemberExprAST *ast) {
  ast->object->Accept(reinterpret_cast<ASTNodeVisitor *>(this));
  auto object = dst_;
  dst_ = codegen_->AccessStructMember(object, codegen_->MakeIdentifier(ast->field));
}

}  // namespace udf
}  // namespace compiler
}  // namespace execution
}  // namespace noisepage
