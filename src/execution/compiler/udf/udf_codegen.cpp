#include "common/error/exception.h"

#include "binder/bind_node_visitor.h"

#include "execution/ast/ast.h"
#include "execution/ast/ast_clone.h"
#include "execution/ast/context.h"
#include "planner/plannodes/output_schema.h"

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

namespace noisepage::execution::compiler::udf {

UdfCodegen::UdfCodegen(catalog::CatalogAccessor *accessor, FunctionBuilder *fb,
                       ast::udf::UdfAstContext *udf_ast_context, CodeGen *codegen, catalog::db_oid_t db_oid)
    : accessor_{accessor},
      fb_{fb},
      udf_ast_context_{udf_ast_context},
      codegen_{codegen},
      db_oid_{db_oid},
      aux_decls_(codegen->GetAstContext()->GetRegion()),
      needs_exec_ctx_{false} {
  for (auto i = 0UL; fb->GetParameterByPosition(i) != nullptr; ++i) {
    auto param = fb->GetParameterByPosition(i);
    const auto &name = param->As<execution::ast::IdentifierExpr>()->Name();
    SymbolTable()[name.GetString()] = name;
  }
}

// Static
execution::ast::File *UdfCodegen::Run(catalog::CatalogAccessor *accessor, FunctionBuilder *function_builder,
                                      ast::udf::UdfAstContext *ast_context, CodeGen *codegen, catalog::db_oid_t db_oid,
                                      ast::udf::FunctionAST *root) {
  UdfCodegen generator{accessor, function_builder, ast_context, codegen, db_oid};
  generator.GenerateUDF(root->Body());
  return generator.Finish();
}

// Static
const char *UdfCodegen::GetReturnParamString() { return "return_val"; }

void UdfCodegen::GenerateUDF(ast::udf::AbstractAST *ast) { ast->Accept(this); }

void UdfCodegen::Visit(ast::udf::AbstractAST *ast) {
  throw NOT_IMPLEMENTED_EXCEPTION("UdfCodegen::Visit(AbstractAST*)");
}

void UdfCodegen::Visit(ast::udf::DynamicSQLStmtAST *ast) {
  throw NOT_IMPLEMENTED_EXCEPTION("UdfCodegen::Visit(DynamicSQLStmtAST*)");
}

catalog::type_oid_t UdfCodegen::GetCatalogTypeOidFromSQLType(execution::ast::BuiltinType::Kind type) {
  switch (type) {
    case execution::ast::BuiltinType::Kind::Integer: {
      return accessor_->GetTypeOidFromTypeId(type::TypeId::INTEGER);
    }
    case execution::ast::BuiltinType::Kind::Boolean: {
      return accessor_->GetTypeOidFromTypeId(type::TypeId::BOOLEAN);
    }
    default:
      return accessor_->GetTypeOidFromTypeId(type::TypeId::INVALID);
      NOISEPAGE_ASSERT(false, "Unsupported parameter type");
  }
}

execution::ast::File *UdfCodegen::Finish() {
  auto fn = fb_->Finish();
  execution::util::RegionVector<execution::ast::Decl *> decls{{fn}, codegen_->GetAstContext()->GetRegion()};
  decls.insert(decls.begin(), aux_decls_.begin(), aux_decls_.end());
  auto file = codegen_->GetAstContext()->GetNodeFactory()->NewFile({0, 0}, std::move(decls));
  return file;
}

void UdfCodegen::Visit(ast::udf::CallExprAST *ast) {
  std::vector<execution::ast::Expr *> args_ast{};
  std::vector<execution::ast::Expr *> args_ast_region_vec{};
  std::vector<catalog::type_oid_t> arg_types{};

  for (auto &arg : ast->Args()) {
    arg->Accept(this);
    args_ast.push_back(dst_);
    args_ast_region_vec.push_back(dst_);
    auto *builtin = dst_->GetType()->SafeAs<execution::ast::BuiltinType>();
    NOISEPAGE_ASSERT(builtin != nullptr, "Parameter must be a built-in type");
    NOISEPAGE_ASSERT(builtin->IsSqlValueType(), "Parameter must be a SQL value type");
    arg_types.push_back(GetCatalogTypeOidFromSQLType(builtin->GetKind()));
  }
  auto proc_oid = accessor_->GetProcOid(ast->Callee(), arg_types);
  NOISEPAGE_ASSERT(proc_oid != catalog::INVALID_PROC_OID, "Invalid call");

  auto context = accessor_->GetProcCtxPtr(proc_oid);
  if (context->IsBuiltin()) {
    fb_->Append(codegen_->MakeStmt(codegen_->CallBuiltin(context->GetBuiltin(), args_ast)));
  } else {
    auto it = SymbolTable().find(ast->Callee());
    execution::ast::Identifier ident_expr;
    if (it != SymbolTable().end()) {
      ident_expr = it->second;
    } else {
      auto file = reinterpret_cast<execution::ast::File *>(
          execution::ast::AstClone::Clone(context->GetFile(), codegen_->GetAstContext()->GetNodeFactory(),
                                          context->GetASTContext(), codegen_->GetAstContext().Get()));
      for (auto decl : file->Declarations()) {
        aux_decls_.push_back(decl);
      }
      ident_expr = codegen_->MakeFreshIdentifier(file->Declarations().back()->Name().GetString());
      SymbolTable()[file->Declarations().back()->Name().GetString()] = ident_expr;
    }
    fb_->Append(codegen_->MakeStmt(codegen_->Call(ident_expr, args_ast_region_vec)));
  }
}

void UdfCodegen::Visit(ast::udf::StmtAST *ast) { UNREACHABLE("Not implemented"); }

void UdfCodegen::Visit(ast::udf::ExprAST *ast) { UNREACHABLE("Not implemented"); }

void UdfCodegen::Visit(ast::udf::DeclStmtAST *ast) {
  if (ast->Name() == "*internal*") {
    return;
  }
  const execution::ast::Identifier ident = codegen_->MakeFreshIdentifier(ast->Name());
  SymbolTable()[ast->Name()] = ident;

  auto prev_type = current_type_;
  execution::ast::Expr *tpl_type = nullptr;
  if (ast->Type() == type::TypeId::INVALID) {
    // record type
    execution::util::RegionVector<execution::ast::FieldDecl *> fields{codegen_->GetAstContext()->GetRegion()};
    for (const auto &p : udf_ast_context_->GetRecordType(ast->Name())) {
      fields.push_back(codegen_->MakeField(codegen_->MakeIdentifier(p.first),
                                           codegen_->TplType(execution::sql::GetTypeId(p.second))));
    }
    auto record_decl = codegen_->DeclareStruct(codegen_->MakeFreshIdentifier("rectype"), std::move(fields));
    aux_decls_.push_back(record_decl);
    tpl_type = record_decl->TypeRepr();
  } else {
    tpl_type = codegen_->TplType(execution::sql::GetTypeId(ast->Type()));
  }
  current_type_ = ast->Type();
  if (ast->Initial() != nullptr) {
    ast->Initial()->Accept(this);
    fb_->Append(codegen_->DeclareVar(ident, tpl_type, dst_));
  } else {
    fb_->Append(codegen_->DeclareVarNoInit(ident, tpl_type));
  }
  current_type_ = prev_type;
}

void UdfCodegen::Visit(ast::udf::FunctionAST *ast) {
  for (size_t i = 0; i < ast->ParameterTypes().size(); i++) {
    SymbolTable()[ast->ParameterNames().at(i)] = codegen_->MakeFreshIdentifier("udf");
  }
  ast->Body()->Accept(this);
}

void UdfCodegen::Visit(ast::udf::VariableExprAST *ast) {
  auto it = SymbolTable().find(ast->Name());
  NOISEPAGE_ASSERT(it != SymbolTable().end(), "Variable not declared");
  dst_ = codegen_->MakeExpr(it->second);
}

void UdfCodegen::Visit(ast::udf::ValueExprAST *ast) {
  auto val = common::ManagedPointer(ast->Value()).CastManagedPointerTo<parser::ConstantValueExpression>();
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

void UdfCodegen::Visit(ast::udf::AssignStmtAST *ast) {
  type::TypeId left_type = type::TypeId::INVALID;
  udf_ast_context_->GetVariableType(ast->Destination()->Name(), &left_type);
  current_type_ = left_type;

  reinterpret_cast<ast::udf::AbstractAST *>(ast->Source())->Accept(this);
  auto rhs_expr = dst_;

  auto it = SymbolTable().find(ast->Destination()->Name());
  NOISEPAGE_ASSERT(it != SymbolTable().end(), "Variable not found");
  auto left_codegen_ident = it->second;

  auto *left_expr = codegen_->MakeExpr(left_codegen_ident);
  fb_->Append(codegen_->Assign(left_expr, rhs_expr));
}

void UdfCodegen::Visit(ast::udf::BinaryExprAST *ast) {
  execution::parsing::Token::Type op_token;
  bool compare = false;
  switch (ast->Op()) {
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
      // TODO(Kyle): Figure out concatenation operation from expressions?
      UNREACHABLE("Unsupported expression");
  }
  ast->Left()->Accept(this);
  auto lhs_expr = dst_;

  ast->Right()->Accept(this);
  auto rhs_expr = dst_;
  if (compare) {
    dst_ = codegen_->Compare(op_token, lhs_expr, rhs_expr);
  } else {
    dst_ = codegen_->BinaryOp(op_token, lhs_expr, rhs_expr);
  }
}

void UdfCodegen::Visit(ast::udf::IfStmtAST *ast) {
  ast->Condition()->Accept(this);
  auto cond = dst_;

  If branch(fb_, cond);
  ast->Then()->Accept(this);
  if (ast->Else() != nullptr) {
    branch.Else();
    ast->Else()->Accept(this);
  }
  branch.EndIf();
}

void UdfCodegen::Visit(ast::udf::IsNullExprAST *ast) {
  ast->Child()->Accept(this);
  auto chld = dst_;
  dst_ = codegen_->CallBuiltin(execution::ast::Builtin::IsValNull, {chld});
  if (!ast->IsNullCheck()) {
    dst_ = codegen_->UnaryOp(execution::parsing::Token::Type::BANG, dst_);
  }
}

void UdfCodegen::Visit(ast::udf::SeqStmtAST *ast) {
  for (auto &stmt : ast->Statements()) {
    stmt->Accept(this);
  }
}

void UdfCodegen::Visit(ast::udf::WhileStmtAST *ast) {
  ast->Condition()->Accept(this);
  auto cond = dst_;
  Loop loop(fb_, cond);
  ast->Body()->Accept(this);
  loop.EndLoop();
}

void UdfCodegen::Visit(ast::udf::ForStmtAST *ast) {
  // Once we encounter a For-statement we know we need an execution context
  needs_exec_ctx_ = true;

  const auto query = common::ManagedPointer(ast->Query());
  auto exec_ctx = fb_->GetParameterByPosition(0);

  binder::BindNodeVisitor visitor{common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_oid_};
  auto query_params = visitor.BindAndGetUDFParams(query, common::ManagedPointer(udf_ast_context_));

  auto stats = optimizer::StatsStorage();
  const uint64_t optimizer_timeout = 1000000;
  auto optimizer_result = trafficcop::TrafficCopUtil::Optimize(
      accessor_->GetTxn(), common::ManagedPointer(accessor_), query, db_oid_, common::ManagedPointer(&stats),
      std::make_unique<optimizer::TrivialCostModel>(), optimizer_timeout, nullptr);
  auto plan = optimizer_result->GetPlanNode();

  // Make a lambda that just writes into this
  std::vector<execution::ast::Identifier> var_idents;
  auto lam_var = codegen_->MakeFreshIdentifier("looplamb");
  execution::util::RegionVector<execution::ast::FieldDecl *> params(codegen_->GetAstContext()->GetRegion());
  params.push_back(codegen_->MakeField(
      exec_ctx->As<execution::ast::IdentifierExpr>()->Name(),
      codegen_->PointerType(codegen_->BuiltinType(execution::ast::BuiltinType::Kind::ExecutionContext))));
  std::size_t i{0};
  for (const auto &var : ast->Variables()) {
    var_idents.push_back(SymbolTable().find(var)->second);
    auto var_ident = var_idents.back();
    NOISEPAGE_ASSERT(plan->GetOutputSchema()->GetColumns().size() == 1,
                     "UDF support for non-scalars is not implemented");
    auto type = codegen_->TplType(execution::sql::GetTypeId(plan->GetOutputSchema()->GetColumn(i).GetType()));

    fb_->Append(codegen_->Assign(codegen_->MakeExpr(var_ident),
                                 codegen_->ConstNull(plan->GetOutputSchema()->GetColumn(i).GetType())));
    auto input = codegen_->MakeFreshIdentifier(var);
    params.push_back(codegen_->MakeField(input, type));
    i++;
  }

  execution::ast::LambdaExpr *lambda_expr{};
  FunctionBuilder fn{codegen_, std::move(params), codegen_->BuiltinType(execution::ast::BuiltinType::Nil)};
  {
    std::size_t j{1};
    for (auto var : var_idents) {
      fn.Append(codegen_->Assign(codegen_->MakeExpr(var), fn.GetParameterByPosition(j)));
      j++;
    }
    auto prev_fb = fb_;
    fb_ = &fn;
    ast->Body()->Accept(this);
    fb_ = prev_fb;
  }

  execution::util::RegionVector<execution::ast::Expr *> captures{codegen_->GetAstContext()->GetRegion()};
  for (const auto &[name, identifier] : SymbolTable()) {
    // TODO(Kyle): Why do we skip this particular identifier?
    if (name == "executionCtx") {
      continue;
    }
    captures.push_back(codegen_->MakeExpr(identifier));
  }

  lambda_expr = fn.FinishLambda(std::move(captures));
  lambda_expr->SetName(lam_var);

  // We want to pass something down that will materialize the lambda
  // function into lambda_expr and will also feed in a lambda_expr to the compiler
  // TODO(Kyle): Using a NULL plan metatdata here...
  execution::exec::ExecutionSettings exec_settings{};
  const std::string dummy_query{};
  auto exec_query = execution::compiler::CompilationContext::Compile(
      *plan, exec_settings, accessor_, execution::compiler::CompilationMode::OneShot, std::nullopt,
      common::ManagedPointer<planner::PlanMetaData>{}, common::ManagedPointer<const std::string>{&dummy_query},
      lambda_expr, codegen_->GetAstContext());

  auto decls = exec_query->GetDecls();
  aux_decls_.insert(aux_decls_.end(), decls.begin(), decls.end());

  fb_->Append(codegen_->DeclareVar(lam_var, codegen_->LambdaType(lambda_expr->GetFunctionLiteralExpr()->TypeRepr()),
                                   lambda_expr));

  auto query_state = codegen_->MakeFreshIdentifier("query_state");
  fb_->Append(codegen_->DeclareVarNoInit(query_state, codegen_->MakeExpr(exec_query->GetQueryStateType()->Name())));

  // Set its execution context to whatever exec context was passed in here
  fb_->Append(codegen_->CallBuiltin(execution::ast::Builtin::StartNewParams, {exec_ctx}));
  std::vector<std::unordered_map<std::string, std::pair<std::string, size_t>>::iterator> sorted_vec;
  for (auto it = query_params.begin(); it != query_params.end(); it++) {
    sorted_vec.push_back(it);
  }

  std::sort(sorted_vec.begin(), sorted_vec.end(), [](auto x, auto y) { return x->second < y->second; });
  for (auto entry : sorted_vec) {
    // TODO(Kyle): Order these
    type::TypeId type = type::TypeId::INVALID;
    udf_ast_context_->GetVariableType(entry->first, &type);
    execution::ast::Builtin builtin{};
    switch (type) {
      case type::TypeId::BOOLEAN:
        builtin = execution::ast::Builtin::AddParamBool;
        break;
      case type::TypeId::TINYINT:
        builtin = execution::ast::Builtin::AddParamTinyInt;
        break;
      case type::TypeId::SMALLINT:
        builtin = execution::ast::Builtin::AddParamSmallInt;
        break;
      case type::TypeId::INTEGER:
        builtin = execution::ast::Builtin::AddParamInt;
        break;
      case type::TypeId::BIGINT:
        builtin = execution::ast::Builtin::AddParamBigInt;
        break;
      case type::TypeId::DECIMAL:
        builtin = execution::ast::Builtin::AddParamDouble;
        break;
      case type::TypeId::DATE:
        builtin = execution::ast::Builtin::AddParamDate;
        break;
      case type::TypeId::TIMESTAMP:
        builtin = execution::ast::Builtin::AddParamTimestamp;
        break;
      case type::TypeId::VARCHAR:
        builtin = execution::ast::Builtin::AddParamString;
        break;
      default:
        UNREACHABLE("Unsupported parameter type");
    }
    fb_->Append(codegen_->CallBuiltin(builtin, {exec_ctx, codegen_->MakeExpr(SymbolTable().at(entry->first))}));
  }

  fb_->Append(codegen_->Assign(
      codegen_->AccessStructMember(codegen_->MakeExpr(query_state), codegen_->MakeIdentifier("execCtx")), exec_ctx));

  auto fns = exec_query->GetFunctionNames();
  for (auto &sub_fn : fns) {
    if (sub_fn.find("Run") != std::string::npos) {
      fb_->Append(codegen_->Call(codegen_->GetAstContext()->GetIdentifier(sub_fn),
                                 {codegen_->AddressOf(query_state), codegen_->MakeExpr(lam_var)}));
    } else {
      fb_->Append(codegen_->Call(codegen_->GetAstContext()->GetIdentifier(sub_fn), {codegen_->AddressOf(query_state)}));
    }
  }

  fb_->Append(codegen_->CallBuiltin(execution::ast::Builtin::FinishNewParams, {exec_ctx}));
}

void UdfCodegen::Visit(ast::udf::RetStmtAST *ast) {
  ast->Return()->Accept(reinterpret_cast<ASTNodeVisitor *>(this));
  auto ret_expr = dst_;
  fb_->Append(codegen_->Return(ret_expr));
}

void UdfCodegen::Visit(ast::udf::SQLStmtAST *ast) {
  // As soon as we encounter an embedded SQL statement,
  // we know we need an execution context
  needs_exec_ctx_ = true;
  auto exec_ctx = fb_->GetParameterByPosition(0);
  const auto query = common::ManagedPointer(ast->Query());

  binder::BindNodeVisitor visitor(common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_oid_);

  const auto &query_params = ast->Parameters();

  // NOTE(Kyle): Assumptions:
  //  - This is a valid optimizer timeout
  //  - No parameters are required for the call to Optimize()

  auto stats = optimizer::StatsStorage();
  const std::uint64_t optimizer_timeout = 1000000;
  auto optimize_result = trafficcop::TrafficCopUtil::Optimize(
      accessor_->GetTxn(), common::ManagedPointer(accessor_), query, db_oid_, common::ManagedPointer(&stats),
      std::make_unique<optimizer::TrivialCostModel>(), optimizer_timeout, nullptr);

  // Make a lambda that just writes into this
  auto lam_var = codegen_->MakeFreshIdentifier("lamb");

  auto plan = optimize_result->GetPlanNode();
  auto &cols = plan->GetOutputSchema()->GetColumns();

  execution::util::RegionVector<execution::ast::FieldDecl *> params{codegen_->GetAstContext()->GetRegion()};
  params.push_back(codegen_->MakeField(
      exec_ctx->As<execution::ast::IdentifierExpr>()->Name(),
      codegen_->PointerType(codegen_->BuiltinType(execution::ast::BuiltinType::Kind::ExecutionContext))));

  std::size_t i{0};
  std::vector<execution::ast::Expr *> assignees{};
  execution::util::RegionVector<execution::ast::Expr *> captures{codegen_->GetAstContext()->GetRegion()};
  for (auto &col : cols) {
    execution::ast::Expr *capture_var = codegen_->MakeExpr(SymbolTable().find(ast->Name())->second);
    type::TypeId udf_type{};
    udf_ast_context_->GetVariableType(ast->Name(), &udf_type);
    if (udf_type == type::TypeId::INVALID) {
      // Record type
      auto &struct_vars = udf_ast_context_->GetRecordType(ast->Name());
      if (captures.empty()) {
        captures.push_back(capture_var);
      }
      capture_var = codegen_->AccessStructMember(capture_var, codegen_->MakeIdentifier(struct_vars[i].first));
      assignees.push_back(capture_var);
    } else {
      assignees.push_back(capture_var);
      captures.push_back(capture_var);
    }
    auto *type = codegen_->TplType(execution::sql::GetTypeId(col.GetType()));

    auto input_param = codegen_->MakeFreshIdentifier("input");
    params.push_back(codegen_->MakeField(input_param, type));
    i++;
  }

  execution::ast::LambdaExpr *lambda_expr{};
  FunctionBuilder fn{codegen_, std::move(params), codegen_->BuiltinType(execution::ast::BuiltinType::Nil)};
  {
    for (auto j = 0UL; j < assignees.size(); ++j) {
      auto capture_var = assignees[j];
      auto input_param = fn.GetParameterByPosition(j + 1);
      fn.Append(codegen_->Assign(capture_var, input_param));
    }
  }

  lambda_expr = fn.FinishLambda(std::move(captures));
  lambda_expr->SetName(lam_var);

  // We want to pass something down that will materialize the lambda function
  // into lambda_expr and will also feed in a lambda_expr to the compiler
  execution::exec::ExecutionSettings exec_settings{};
  const std::string dummy_query{};
  auto exec_query = execution::compiler::CompilationContext::Compile(
      *plan, exec_settings, accessor_, execution::compiler::CompilationMode::OneShot, std::nullopt,
      common::ManagedPointer<planner::PlanMetaData>{}, common::ManagedPointer<const std::string>(&dummy_query),
      lambda_expr, codegen_->GetAstContext());

  auto decls = exec_query->GetDecls();
  aux_decls_.insert(aux_decls_.end(), decls.begin(), decls.end());

  fb_->Append(codegen_->DeclareVar(lam_var, codegen_->LambdaType(lambda_expr->GetFunctionLiteralExpr()->TypeRepr()),
                                   lambda_expr));

  // Make query state
  auto query_state = codegen_->MakeFreshIdentifier("query_state");
  fb_->Append(codegen_->DeclareVarNoInit(query_state, codegen_->MakeExpr(exec_query->GetQueryStateType()->Name())));

  // Set its execution context to whatever exec context was passed in here
  fb_->Append(codegen_->CallBuiltin(execution::ast::Builtin::StartNewParams, {exec_ctx}));
  std::vector<std::unordered_map<std::string, std::pair<std::string, size_t>>::const_iterator> sorted_vec{};
  for (auto it = query_params.begin(); it != query_params.end(); it++) {
    sorted_vec.push_back(it);
  }

  std::sort(sorted_vec.begin(), sorted_vec.end(), [](auto x, auto y) { return x->second.second < y->second.second; });
  for (auto entry : sorted_vec) {
    // TODO(Kyle): Order these
    type::TypeId type = type::TypeId::INVALID;
    execution::ast::Expr *expr = nullptr;
    if (entry->second.first.length() > 0) {
      auto &fields = udf_ast_context_->GetRecordType(entry->second.first);
      auto it = std::find_if(fields.begin(), fields.end(), [=](auto p) { return p.first == entry->first; });
      type = it->second;
      expr = codegen_->AccessStructMember(codegen_->MakeExpr(SymbolTable().at(entry->second.first)),
                                          codegen_->MakeIdentifier(entry->first));
    } else {
      udf_ast_context_->GetVariableType(entry->first, &type);
      expr = codegen_->MakeExpr(SymbolTable().at(entry->first));
    }

    execution::ast::Builtin builtin{};
    switch (type) {
      case type::TypeId::BOOLEAN:
        builtin = execution::ast::Builtin::AddParamBool;
        break;
      case type::TypeId::TINYINT:
        builtin = execution::ast::Builtin::AddParamTinyInt;
        break;
      case type::TypeId::SMALLINT:
        builtin = execution::ast::Builtin::AddParamSmallInt;
        break;
      case type::TypeId::INTEGER:
        builtin = execution::ast::Builtin::AddParamInt;
        break;
      case type::TypeId::BIGINT:
        builtin = execution::ast::Builtin::AddParamBigInt;
        break;
      case type::TypeId::DECIMAL:
        builtin = execution::ast::Builtin::AddParamDouble;
        break;
      case type::TypeId::DATE:
        builtin = execution::ast::Builtin::AddParamDate;
        break;
      case type::TypeId::TIMESTAMP:
        builtin = execution::ast::Builtin::AddParamTimestamp;
        break;
      case type::TypeId::VARCHAR:
        builtin = execution::ast::Builtin::AddParamString;
        break;
      default:
        UNREACHABLE("Unsupported parameter type");
    }
    fb_->Append(codegen_->CallBuiltin(builtin, {exec_ctx, expr}));
  }

  fb_->Append(codegen_->Assign(
      codegen_->AccessStructMember(codegen_->MakeExpr(query_state), codegen_->MakeIdentifier("execCtx")), exec_ctx));

  for (auto &col : cols) {
    execution::ast::Expr *capture_var = codegen_->MakeExpr(SymbolTable().find(ast->Name())->second);
    auto *lhs = capture_var;
    if (cols.size() > 1) {
      // Record struct type
      lhs = codegen_->AccessStructMember(capture_var, codegen_->MakeIdentifier(col.GetName()));
    }
    fb_->Append(codegen_->Assign(lhs, codegen_->ConstNull(col.GetType())));
  }

  auto fns = exec_query->GetFunctionNames();
  for (auto &sub_fn : fns) {
    if (sub_fn.find("Run") != std::string::npos) {
      fb_->Append(codegen_->Call(codegen_->GetAstContext()->GetIdentifier(sub_fn),
                                 {codegen_->AddressOf(query_state), codegen_->MakeExpr(lam_var)}));
    } else {
      fb_->Append(codegen_->Call(codegen_->GetAstContext()->GetIdentifier(sub_fn), {codegen_->AddressOf(query_state)}));
    }
  }

  fb_->Append(codegen_->CallBuiltin(execution::ast::Builtin::FinishNewParams, {exec_ctx}));
}

void UdfCodegen::Visit(ast::udf::MemberExprAST *ast) {
  ast->Object()->Accept(reinterpret_cast<ASTNodeVisitor *>(this));
  auto object = dst_;
  dst_ = codegen_->AccessStructMember(object, codegen_->MakeIdentifier(ast->FieldName()));
}

}  // namespace noisepage::execution::compiler::udf
