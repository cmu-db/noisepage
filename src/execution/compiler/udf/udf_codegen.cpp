#include "execution/compiler/udf/udf_codegen.h"

#include "binder/bind_node_visitor.h"
#include "catalog/catalog_accessor.h"
#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "execution/ast/ast.h"
#include "execution/ast/ast_clone.h"
#include "execution/ast/context.h"
#include "execution/ast/udf/udf_ast_nodes.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "execution/exec/execution_settings.h"
#include "execution/vm/bytecode_function_info.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/statistics/stats_storage.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/postgresparser.h"
#include "parser/udf/variable_ref.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "traffic_cop/traffic_cop_util.h"

namespace noisepage::execution::compiler::udf {

/** The identifier for the pipeline `RunAll` function */
constexpr static const char RUN_ALL_IDENTIFIER[] = "RunAll";

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

catalog::type_oid_t UdfCodegen::GetCatalogTypeOidFromSQLType(execution::ast::BuiltinType::Kind type) {
  switch (type) {
    case ast::BuiltinType::Kind::Integer: {
      return accessor_->GetTypeOidFromTypeId(type::TypeId::INTEGER);
    }
    case ast::BuiltinType::Kind::Boolean: {
      return accessor_->GetTypeOidFromTypeId(type::TypeId::BOOLEAN);
    }
    default:
      return accessor_->GetTypeOidFromTypeId(type::TypeId::INVALID);
      NOISEPAGE_ASSERT(false, "Unsupported parameter type");
  }
}

ast::File *UdfCodegen::Finish() {
  ast::FunctionDecl *fn = fb_->Finish();
  util::RegionVector<ast::Decl *> decls{{fn}, codegen_->GetAstContext()->GetRegion()};
  decls.insert(decls.begin(), aux_decls_.cbegin(), aux_decls_.cend());
  auto file = codegen_->GetAstContext()->GetNodeFactory()->NewFile({0, 0}, std::move(decls));
  return file;
}

/* ----------------------------------------------------------------------------
  Code Generation: "Simple" Constructs
---------------------------------------------------------------------------- */

void UdfCodegen::Visit(ast::udf::AbstractAST *ast) {
  throw NOT_IMPLEMENTED_EXCEPTION("UdfCodegen::Visit(AbstractAST*)");
}

void UdfCodegen::Visit(ast::udf::DynamicSQLStmtAST *ast) {
  throw NOT_IMPLEMENTED_EXCEPTION("UdfCodegen::Visit(DynamicSQLStmtAST*)");
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
  if (ast->Name() == INTERNAL_DECL_ID) {
    return;
  }

  const execution::ast::Identifier identifier = codegen_->MakeFreshIdentifier(ast->Name());
  SymbolTable()[ast->Name()] = identifier;

  auto prev_type = current_type_;
  execution::ast::Expr *tpl_type = nullptr;
  if (ast->Type() == type::TypeId::INVALID) {
    // Record type
    execution::util::RegionVector<execution::ast::FieldDecl *> fields{codegen_->GetAstContext()->GetRegion()};

    // TODO(Kyle): Handle unbound record types
    const auto record_type = udf_ast_context_->GetRecordType(ast->Name());
    if (!record_type.has_value()) {
      // Unbound record type
      throw NOT_IMPLEMENTED_EXCEPTION("Unbound RECORD types not supported");
    }

    for (const auto &p : record_type.value()) {
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
    fb_->Append(codegen_->DeclareVar(identifier, tpl_type, dst_));
  } else {
    fb_->Append(codegen_->DeclareVarNoInit(identifier, tpl_type));
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
  const type::TypeId left_type = GetVariableType(ast->Destination()->Name());
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

void UdfCodegen::Visit(ast::udf::RetStmtAST *ast) {
  ast->Return()->Accept(reinterpret_cast<ASTNodeVisitor *>(this));
  auto ret_expr = dst_;
  fb_->Append(codegen_->Return(ret_expr));
}

void UdfCodegen::Visit(ast::udf::MemberExprAST *ast) {
  ast->Object()->Accept(reinterpret_cast<ASTNodeVisitor *>(this));
  auto object = dst_;
  dst_ = codegen_->AccessStructMember(object, codegen_->MakeIdentifier(ast->FieldName()));
}

/* ----------------------------------------------------------------------------
  Code Generation: Integer-Variant For-Loops
---------------------------------------------------------------------------- */

void UdfCodegen::Visit(ast::udf::ForIStmtAST *ast) { throw NOT_IMPLEMENTED_EXCEPTION("ForIStmtAST Not Implemented"); }

/* ----------------------------------------------------------------------------
  Code Generation: Query-Variant For-Loops
---------------------------------------------------------------------------- */

void UdfCodegen::Visit(ast::udf::ForSStmtAST *ast) {
  // Executing a SQL query requires an execution context
  needs_exec_ctx_ = true;
  execution::ast::Expr *exec_ctx = fb_->GetParameterByPosition(0);

  // Bind the embedded query; must do this prior to attempting
  // to optimize to ensure correctness
  const auto variable_refs = BindQueryAndGetVariableRefs(ast->Query());

  // Optimize the embedded query
  auto optimize_result = OptimizeEmbeddedQuery(ast->Query());
  auto plan = optimize_result->GetPlanNode();

  // Start construction of the lambda expression
  auto builder = StartLambda(plan, ast->Variables());

  // Generate code for variable initialization
  CodegenBoundVariableInit(plan, ast->Variables());

  // Generate code for the loop body
  {
    auto cached_builder = fb_;
    fb_ = builder.get();
    ast->Body()->Accept(this);
    fb_ = cached_builder;
  }

  ast::LambdaExpr *lambda_expr = builder->FinishClosure();
  const ast::Identifier lambda_identifier = codegen_->MakeFreshIdentifier("udfLambda");
  lambda_expr->SetName(lambda_identifier);

  // Materialize the lambda into the lambda expression
  execution::exec::ExecutionSettings exec_settings{};
  const std::string dummy_query{};
  auto exec_query = execution::compiler::CompilationContext::Compile(
      *plan, exec_settings, accessor_, execution::compiler::CompilationMode::OneShot, std::nullopt,
      common::ManagedPointer<planner::PlanMetaData>{}, lambda_expr, codegen_->GetAstContext());

  // Append all of the declarations from the compiled query
  auto decls = exec_query->GetDecls();
  aux_decls_.insert(aux_decls_.end(), decls.cbegin(), decls.cend());

  // Declare the closure and the query state in the current function
  auto query_state = codegen_->MakeFreshIdentifier("query_state");
  fb_->Append(codegen_->DeclareVarNoInit(query_state, codegen_->MakeExpr(exec_query->GetQueryStateType()->Name())));
  fb_->Append(codegen_->DeclareVar(
      lambda_identifier, codegen_->LambdaType(lambda_expr->GetFunctionLiteralExpr()->TypeRepr()), lambda_expr));

  // Set its execution context to whatever execution context was passed in here
  fb_->Append(codegen_->CallBuiltin(execution::ast::Builtin::StartNewParams, {exec_ctx}));

  CodegenAddParameters(exec_ctx, variable_refs);

  fb_->Append(codegen_->Assign(
      codegen_->AccessStructMember(codegen_->MakeExpr(query_state), codegen_->MakeIdentifier("execCtx")), exec_ctx));

  // Manually append calls to each function from the compiled
  // executable query (implementing the closure) to the builder
  CodegenTopLevelCalls(exec_query.get(), query_state, lambda_identifier);

  fb_->Append(codegen_->CallBuiltin(execution::ast::Builtin::FinishNewParams, {exec_ctx}));
}

std::unique_ptr<FunctionBuilder> UdfCodegen::StartLambda(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                                         const std::vector<std::string> &variables) {
  return GetVariableType(variables.front()) == type::TypeId::INVALID ? StartLambdaBindingToRecord(plan, variables)
                                                                     : StartLambdaBindingToScalars(plan, variables);
}

std::unique_ptr<FunctionBuilder> UdfCodegen::StartLambdaBindingToRecord(
    common::ManagedPointer<planner::AbstractPlanNode> plan, const std::vector<std::string> &variables) {
  // bind results to a single RECORD variable
  NOISEPAGE_ASSERT(variables.size() == 1, "Broken invariant");

  const std::string &record_name = variables.front();
  const auto record_type = GetRecordType(record_name);

  const auto n_fields = record_type.size();
  const auto n_columns = plan->GetOutputSchema()->GetColumns().size();
  if (n_fields != n_columns) {
    throw EXECUTION_EXCEPTION(
        fmt::format("Attempt to bind {} query outputs to record type with {} fields", n_columns, n_fields),
        common::ErrorCode::ERRCODE_PLPGSQL_ERROR);
  }

  // The lambda accepts all columns of the query output schema as parameters
  util::RegionVector<ast::FieldDecl *> parameters{codegen_->GetAstContext()->GetRegion()};

  // The first parameter is always the execution context
  ast::Expr *exec_ctx = fb_->GetParameterByPosition(0);
  parameters.push_back(
      codegen_->MakeField(exec_ctx->As<ast::IdentifierExpr>()->Name(),
                          codegen_->PointerType(codegen_->BuiltinType(ast::BuiltinType::Kind::ExecutionContext))));

  // The lambda captures all variables in the symbol table
  // NOTE(Kyle): It might be possible / preferable to make this more conservative
  util::RegionVector<ast::Expr *> captures{codegen_->GetAstContext()->GetRegion()};
  for (const auto &[name, identifier] : SymbolTable()) {
    if (name != "executionCtx") {
      captures.push_back(codegen_->MakeExpr(identifier));
    }
  }

  // While the closure only captures a single variable, we still need
  // to generate code for an assignment to each field memeber
  std::vector<ast::Expr *> assignees{};
  assignees.reserve(n_columns);

  ast::Expr *record = codegen_->MakeExpr(SymbolTable().find(record_name)->second);
  for (std::size_t i = 0UL; i < n_columns; ++i) {
    const auto &column = plan->GetOutputSchema()->GetColumn(i);
    assignees.push_back(codegen_->AccessStructMember(record, codegen_->MakeIdentifier(record_type[i].first)));
    parameters.push_back(codegen_->MakeField(codegen_->MakeFreshIdentifier("input"),
                                             codegen_->TplType(sql::GetTypeId(column.GetType()))));
  }

  auto builder = std::make_unique<FunctionBuilder>(codegen_, std::move(parameters), std::move(captures),
                                                   codegen_->BuiltinType(ast::BuiltinType::Nil));
  for (std::size_t i = 0UL; i < assignees.size(); ++i) {
    auto *assignee = assignees.at(i);
    auto input_parameter = builder->GetParameterByPosition(i + 1);
    builder->Append(codegen_->Assign(assignee, input_parameter));
  }
  return builder;
}

std::unique_ptr<FunctionBuilder> UdfCodegen::StartLambdaBindingToScalars(
    common::ManagedPointer<planner::AbstractPlanNode> plan, const std::vector<std::string> &variables) {
  // bind results to one or more non-RECORD variables
  const auto n_variables = variables.size();
  const auto n_columns = plan->GetOutputSchema()->GetColumns().size();
  if (n_variables != n_columns) {
    throw EXECUTION_EXCEPTION(fmt::format("Attempt to bind {} query outputs to {} variables", n_columns, n_variables),
                              common::ErrorCode::ERRCODE_PLPGSQL_ERROR);
  }

  // The lambda accepts all columns of the query output schema as parameters
  util::RegionVector<ast::FieldDecl *> parameters{codegen_->GetAstContext()->GetRegion()};

  // The lambda captures all variables in the symbol table
  // NOTE(Kyle): It might be possible / preferable to make this more conservative
  util::RegionVector<ast::Expr *> captures{codegen_->GetAstContext()->GetRegion()};
  for (const auto &[name, identifier] : SymbolTable()) {
    if (name != "executionCtx") {
      captures.push_back(codegen_->MakeExpr(identifier));
    }
  }

  // The first parameter is always the execution context
  ast::Expr *exec_ctx = fb_->GetParameterByPosition(0);
  parameters.push_back(codegen_->MakeField(
      exec_ctx->As<ast::IdentifierExpr>()->Name(),
      codegen_->PointerType(codegen_->BuiltinType(execution::ast::BuiltinType::Kind::ExecutionContext))));

  // Assignees are those captures that are written in the closure
  std::vector<ast::Expr *> assignees{};
  assignees.reserve(n_columns);

  // Populate the parameters and capture assignees
  for (std::size_t i = 0UL; i < n_columns; ++i) {
    const auto &variable = variables.at(i);
    const auto &column = plan->GetOutputSchema()->GetColumn(i);
    assignees.push_back(codegen_->MakeExpr(SymbolTable().find(variable)->second));
    parameters.push_back(codegen_->MakeField(codegen_->MakeFreshIdentifier("input"),
                                             codegen_->TplType(sql::GetTypeId(column.GetType()))));
  }

  // Begin construction of the function that implements the closure
  auto builder = std::make_unique<FunctionBuilder>(codegen_, std::move(parameters), std::move(captures),
                                                   codegen_->BuiltinType(execution::ast::BuiltinType::Nil));

  // Generate an assignment from each input parameter to the associated capture
  for (std::size_t i = 0UL; i < assignees.size(); ++i) {
    ast::Expr *capture = assignees.at(i);
    auto input_parameter = builder->GetParameterByPosition(i + 1);
    builder->Append(codegen_->Assign(capture, input_parameter));
  }
  return builder;
}

/* ----------------------------------------------------------------------------
  Code Generation: SQL Statements
---------------------------------------------------------------------------- */

void UdfCodegen::Visit(ast::udf::SQLStmtAST *ast) {
  // Executing a SQL query requires an execution context
  needs_exec_ctx_ = true;
  ast::Expr *exec_ctx = fb_->GetParameterByPosition(0);

  // Bind the embedded query; must do this prior to attempting
  // to optimize to ensure correctness
  const auto variable_refs = BindQueryAndGetVariableRefs(ast->Query());

  // Optimize the query and generate get a reference to the plan
  auto optimize_result = OptimizeEmbeddedQuery(ast->Query());
  auto plan = optimize_result->GetPlanNode();

  // Construct a lambda that writes the output of the query
  // into the bound variables, as defined by the function body
  ast::LambdaExpr *lambda_expr = MakeLambda(plan, ast->Variables());
  const ast::Identifier lambda_identifier = codegen_->MakeFreshIdentifier("udfLambda");
  lambda_expr->SetName(lambda_identifier);

  // Generate code for the embedded query, utilizing the generated closure as the output callback
  exec::ExecutionSettings exec_settings{};
  auto exec_query = compiler::CompilationContext::Compile(
      *plan, exec_settings, accessor_, compiler::CompilationMode::OneShot, std::nullopt,
      common::ManagedPointer<planner::PlanMetaData>{}, lambda_expr, codegen_->GetAstContext());

  // Append all declarations from the compiled query
  auto decls = exec_query->GetDecls();
  aux_decls_.insert(aux_decls_.end(), decls.cbegin(), decls.cend());

  // Declare the closure and the query state in the current function
  auto query_state = codegen_->MakeFreshIdentifier("query_state");
  fb_->Append(codegen_->DeclareVarNoInit(query_state, codegen_->MakeExpr(exec_query->GetQueryStateType()->Name())));
  fb_->Append(codegen_->DeclareVar(
      lambda_identifier, codegen_->LambdaType(lambda_expr->GetFunctionLiteralExpr()->TypeRepr()), lambda_expr));

  // Set its execution context to whatever execution context was passed in here
  fb_->Append(codegen_->CallBuiltin(ast::Builtin::StartNewParams, {exec_ctx}));

  // Determine the column references in the query (if any)
  // that depend on variables in the UDF definition
  CodegenAddParameters(exec_ctx, variable_refs);

  // Load the execution context member of the query state
  fb_->Append(codegen_->Assign(
      codegen_->AccessStructMember(codegen_->MakeExpr(query_state), codegen_->MakeIdentifier("execCtx")), exec_ctx));

  // Initialize the captures
  CodegenBoundVariableInit(plan, ast->Variables());

  // Manually append calls to each function from the compiled
  // executable query (implementing the closure) to the builder
  CodegenTopLevelCalls(exec_query.get(), query_state, lambda_identifier);

  fb_->Append(codegen_->CallBuiltin(ast::Builtin::FinishNewParams, {exec_ctx}));
}

ast::LambdaExpr *UdfCodegen::MakeLambda(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                        const std::vector<std::string> &variables) {
  return GetVariableType(variables.front()) == type::TypeId::INVALID ? MakeLambdaBindingToRecord(plan, variables)
                                                                     : MakeLambdaBindingToScalars(plan, variables);
}

ast::LambdaExpr *UdfCodegen::MakeLambdaBindingToRecord(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                                       const std::vector<std::string> &variables) {
  // bind results to a single RECORD variable
  NOISEPAGE_ASSERT(variables.size() == 1, "Broken invariant");

  const std::string &record_name = variables.front();
  const auto record_type = GetRecordType(record_name);

  const auto n_fields = record_type.size();
  const auto n_columns = plan->GetOutputSchema()->GetColumns().size();
  if (n_fields != n_columns) {
    throw EXECUTION_EXCEPTION(
        fmt::format("Attempt to bind {} query outputs to record type with {} fields", n_columns, n_fields),
        common::ErrorCode::ERRCODE_PLPGSQL_ERROR);
  }

  // The lambda accepts all columns of the query output schema as parameters
  util::RegionVector<ast::FieldDecl *> parameters{codegen_->GetAstContext()->GetRegion()};

  // The first parameter is always the execution context
  ast::Expr *exec_ctx = fb_->GetParameterByPosition(0);
  parameters.push_back(
      codegen_->MakeField(exec_ctx->As<ast::IdentifierExpr>()->Name(),
                          codegen_->PointerType(codegen_->BuiltinType(ast::BuiltinType::Kind::ExecutionContext))));

  // The lambda only captures the RECORD variable to which all results are bound
  ast::Expr *capture = codegen_->MakeExpr(SymbolTable().find(record_name)->second);
  util::RegionVector<ast::Expr *> captures{codegen_->GetAstContext()->GetRegion()};

  // While the closure only captures a single variable, we still need
  // to generate code for an assignment to each field memeber
  std::vector<ast::Expr *> assignees{};
  assignees.reserve(n_columns);

  for (std::size_t i = 0; i < n_columns; ++i) {
    const auto &column = plan->GetOutputSchema()->GetColumn(i);
    assignees.push_back(codegen_->AccessStructMember(capture, codegen_->MakeIdentifier(record_type[i].first)));
    parameters.push_back(codegen_->MakeField(codegen_->MakeFreshIdentifier("input"),
                                             codegen_->TplType(sql::GetTypeId(column.GetType()))));
  }

  FunctionBuilder builder{codegen_, std::move(parameters), std::move(captures),
                          codegen_->BuiltinType(ast::BuiltinType::Nil)};
  for (std::size_t i = 0UL; i < assignees.size(); ++i) {
    auto *assignee = assignees.at(i);
    auto input_parameter = builder.GetParameterByPosition(i + 1);
    builder.Append(codegen_->Assign(assignee, input_parameter));
  }

  return builder.FinishClosure();
}

ast::LambdaExpr *UdfCodegen::MakeLambdaBindingToScalars(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                                        const std::vector<std::string> &variables) {
  // bind results to one or more non-RECORD variables
  const auto n_variables = variables.size();
  const auto n_columns = plan->GetOutputSchema()->GetColumns().size();
  if (n_variables != n_columns) {
    throw EXECUTION_EXCEPTION(fmt::format("Attempt to bind {} query outputs to {} variables", n_columns, n_variables),
                              common::ErrorCode::ERRCODE_PLPGSQL_ERROR);
  }

  // The lambda accepts all columns of the query output schema as parameters
  util::RegionVector<ast::FieldDecl *> parameters{codegen_->GetAstContext()->GetRegion()};
  // The lambda captures the variables to which results are bound from the enclosing scope
  util::RegionVector<ast::Expr *> captures{codegen_->GetAstContext()->GetRegion()};

  // The first parameter is always the execution context
  ast::Expr *exec_ctx = fb_->GetParameterByPosition(0);
  parameters.push_back(codegen_->MakeField(
      exec_ctx->As<ast::IdentifierExpr>()->Name(),
      codegen_->PointerType(codegen_->BuiltinType(execution::ast::BuiltinType::Kind::ExecutionContext))));

  // Populate the remainder of the parameters and captures
  for (std::size_t i = 0; i < n_columns; ++i) {
    const auto &variable = variables.at(i);
    const auto &column = plan->GetOutputSchema()->GetColumn(i);
    captures.push_back(codegen_->MakeExpr(SymbolTable().find(variable)->second));
    parameters.push_back(codegen_->MakeField(codegen_->MakeFreshIdentifier("input"),
                                             codegen_->TplType(sql::GetTypeId(column.GetType()))));
  }

  // Clone the captures for assignment within the closure body
  const std::vector<ast::Expr *> assignees{captures.cbegin(), captures.cend()};

  // Begin construction of the function that implements the closure
  FunctionBuilder builder{codegen_, std::move(parameters), std::move(captures),
                          codegen_->BuiltinType(execution::ast::BuiltinType::Nil)};

  // Generate an assignment from each input parameter to the associated capture
  for (std::size_t i = 0UL; i < assignees.size(); ++i) {
    ast::Expr *capture = assignees.at(i);
    auto input_parameter = builder.GetParameterByPosition(i + 1);
    builder.Append(codegen_->Assign(capture, input_parameter));
  }
  return builder.FinishClosure();
}

/* ----------------------------------------------------------------------------
  Common Code Generation Helpers
---------------------------------------------------------------------------- */

void UdfCodegen::CodegenAddParameters(ast::Expr *exec_ctx, const std::vector<parser::udf::VariableRef> &variable_refs) {
  for (const auto &variable_ref : variable_refs) {
    if (variable_ref.IsScalar()) {
      CodegenAddScalarParameter(exec_ctx, variable_ref);
    } else {
      CodegenAddTableParameter(exec_ctx, variable_ref);
    }
  }
}

void UdfCodegen::CodegenAddScalarParameter(ast::Expr *exec_ctx, const parser::udf::VariableRef &variable_ref) {
  NOISEPAGE_ASSERT(variable_ref.IsScalar(), "Broken invariant");
  const auto &name = variable_ref.ColumnName();
  const type::TypeId type = GetVariableType(name);
  ast::Expr *expr = codegen_->MakeExpr(SymbolTable().at(name));
  fb_->Append(codegen_->CallBuiltin(AddParamBuiltinForParameterType(type), {exec_ctx, expr}));
}

void UdfCodegen::CodegenAddTableParameter(ast::Expr *exec_ctx, const parser::udf::VariableRef &variable_ref) {
  NOISEPAGE_ASSERT(!variable_ref.IsScalar(), "Broken invariant");

  const auto &record_name = variable_ref.TableName();
  const auto &field_name = variable_ref.ColumnName();

  const auto fields = GetRecordType(record_name);
  auto it = std::find_if(
      fields.cbegin(), fields.cend(),
      [&field_name](const std::pair<std::string, type::TypeId> &field) -> bool { return field.first == field_name; });
  if (it == fields.cend()) {
    throw EXECUTION_EXCEPTION(fmt::format("Field '{}' not found in record '{}'", field_name, record_name),
                              common::ErrorCode::ERRCODE_PLPGSQL_ERROR);
  }

  const type::TypeId type = it->second;
  ast::Expr *expr = codegen_->AccessStructMember(codegen_->MakeExpr(SymbolTable().at(record_name)),
                                                 codegen_->MakeIdentifier(field_name));
  fb_->Append(codegen_->CallBuiltin(AddParamBuiltinForParameterType(type), {exec_ctx, expr}));
}

void UdfCodegen::CodegenBoundVariableInit(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                          const std::vector<std::string> &bound_variables) {
  if (bound_variables.empty()) {
    // Nothing to do
    return;
  }

  if (GetVariableType(bound_variables.front()) == type::TypeId::INVALID) {
    CodegenBoundVariableInitForRecord(plan, bound_variables.front());
  } else {
    CodegenBoundVariableInitForScalars(plan, bound_variables);
  }
}

void UdfCodegen::CodegenBoundVariableInitForScalars(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                                    const std::vector<std::string> &bound_variables) {
  const auto n_columns = plan->GetOutputSchema()->GetColumns().size();
  const auto n_variables = bound_variables.size();
  if (n_columns != n_variables) {
    throw EXECUTION_EXCEPTION(
        fmt::format("Attempt to bind {} query results to {} scalar variables", n_columns, n_variables),
        common::ErrorCode::ERRCODE_PLPGSQL_ERROR);
  }

  for (std::size_t i = 0; i < n_columns; ++i) {
    const auto &column = plan->GetOutputSchema()->GetColumn(i);
    const auto &variable = bound_variables.at(i);
    execution::ast::Expr *capture = codegen_->MakeExpr(SymbolTable().find(variable)->second);
    fb_->Append(codegen_->Assign(capture, codegen_->ConstNull(column.GetType())));
  }
}

void UdfCodegen::CodegenBoundVariableInitForRecord(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                                   const std::string &record_name) {
  NOISEPAGE_ASSERT(GetVariableType(record_name) == type::TypeId::INVALID, "Broken invariant");
  const auto n_columns = plan->GetOutputSchema()->GetColumns().size();
  const auto fields = GetRecordType(record_name);
  const auto n_fields = fields.size();
  if (n_columns != n_fields) {
    // NOTE(Kyle): This should be impossible, the structure of the
    // record type is derived from the output schema of the query
    throw EXECUTION_EXCEPTION(
        fmt::format("Attempt to bind {} query results to record with {} fields", n_columns, n_fields),
        common::ErrorCode::ERRCODE_PLPGSQL_ERROR);
  }

  ast::Expr *record = codegen_->MakeExpr(SymbolTable().find(record_name)->second);
  for (std::size_t i = 0; i < n_columns; ++i) {
    const auto &column = plan->GetOutputSchema()->GetColumn(i);
    const auto &field = fields.at(i);
    NOISEPAGE_ASSERT(column.GetName() == field.first, "Broken invariant");
    ast::Expr *capture = codegen_->AccessStructMember(record, codegen_->MakeIdentifier(field.first));
    fb_->Append(codegen_->Assign(capture, codegen_->ConstNull(column.GetType())));
  }
}

void UdfCodegen::CodegenTopLevelCalls(const ExecutableQuery *exec_query, ast::Identifier query_state_id,
                                      ast::Identifier lambda_id) {
  /**
   * We don't inject the lambda parameter into every "Run" function,
   * and instead only add it as an additional parameter for those
   * pipelines that require it. This is parsimonious, but makes the
   * process of injecting calls to each function slightly more complex.
   *
   * Pipelines with output callbacks are wrapped in a top-level `RunAll`
   * function which accepts the lambda as a parameter. This `RunAll` function
   * then assumes responsibility for calling the other top-level functions
   * of which the pipeline is composed, in the proper order. In pipelines
   * with output callbacks, this `RunAll` function is the only one registered
   * for which a step is registered with the ExecutableQueryFragmentBuilder,
   * so it is the only function returned by `GetFunctionMetadata()` for this
   * pipeline.
   *
   * Pipelines without output callbacks are generated as normal, without
   * the output callback added as an additional parameter. Therefore, we
   * must inject calls to these functions with the regular signature.
   */

  for (const auto *metadata : exec_query->GetFunctionMetadata()) {
    const auto &function_name = metadata->GetName();
    if (IsRunAllFunction(function_name)) {
      NOISEPAGE_ASSERT(metadata->GetParamsCount() == 2, "Unexpected arity for RunAll function");
      fb_->Append(codegen_->Call(codegen_->GetAstContext()->GetIdentifier(function_name),
                                 {codegen_->AddressOf(query_state_id), codegen_->MakeExpr(lambda_id)}));
    } else {
      NOISEPAGE_ASSERT(metadata->GetParamsCount() == 1, "Unexpected arity for top-level pipeline function");
      fb_->Append(codegen_->Call(codegen_->GetAstContext()->GetIdentifier(function_name),
                                 {codegen_->AddressOf(query_state_id)}));
    }
  }
}

/* ----------------------------------------------------------------------------
  General Utilities
---------------------------------------------------------------------------- */

type::TypeId UdfCodegen::GetVariableType(const std::string &name) const {
  auto type = udf_ast_context_->GetVariableType(name);
  if (!type.has_value()) {
    throw EXECUTION_EXCEPTION(fmt::format("Failed to resolve type for variable '{}'", name),
                              common::ErrorCode::ERRCODE_PLPGSQL_ERROR);
  }
  return type.value();
}

std::vector<std::pair<std::string, type::TypeId>> UdfCodegen::GetRecordType(const std::string &name) const {
  auto type = udf_ast_context_->GetRecordType(name);
  if (!type.has_value()) {
    throw EXECUTION_EXCEPTION(fmt::format("Failed to resolve type for record variable '{}'", name),
                              common::ErrorCode::ERRCODE_PLPGSQL_ERROR);
  }
  return type.value();
}

std::vector<parser::udf::VariableRef> UdfCodegen::BindQueryAndGetVariableRefs(parser::ParseResult *query) {
  binder::BindNodeVisitor visitor{common::ManagedPointer{accessor_}, db_oid_};
  return visitor.BindAndGetUDFVariableRefs(common::ManagedPointer{query}, common::ManagedPointer{udf_ast_context_});
}

std::unique_ptr<optimizer::OptimizeResult> UdfCodegen::OptimizeEmbeddedQuery(parser::ParseResult *parsed_query) {
  optimizer::StatsStorage stats{};
  const std::uint64_t optimizer_timeout = 1000000;
  return trafficcop::TrafficCopUtil::Optimize(
      accessor_->GetTxn(), common::ManagedPointer(accessor_), common::ManagedPointer(parsed_query), db_oid_,
      common::ManagedPointer(&stats), std::make_unique<optimizer::TrivialCostModel>(), optimizer_timeout, nullptr);
}

// Static
bool UdfCodegen::IsRunAllFunction(const std::string &name) {
  return name.find(RUN_ALL_IDENTIFIER) != std::string::npos;
}

// Static
ast::Builtin UdfCodegen::AddParamBuiltinForParameterType(type::TypeId parameter_type) {
  // TODO(Kyle): Could accomplish this same thing with a compile-time
  // dispatch table, but honestly that would be overkill at this point
  switch (parameter_type) {
    case type::TypeId::BOOLEAN:
      return execution::ast::Builtin::AddParamBool;
    case type::TypeId::TINYINT:
      return execution::ast::Builtin::AddParamTinyInt;
    case type::TypeId::SMALLINT:
      return execution::ast::Builtin::AddParamSmallInt;
    case type::TypeId::INTEGER:
      return execution::ast::Builtin::AddParamInt;
    case type::TypeId::BIGINT:
      return execution::ast::Builtin::AddParamBigInt;
    case type::TypeId::DECIMAL:
      return execution::ast::Builtin::AddParamDouble;
    case type::TypeId::DATE:
      return execution::ast::Builtin::AddParamDate;
    case type::TypeId::TIMESTAMP:
      return execution::ast::Builtin::AddParamTimestamp;
    case type::TypeId::VARCHAR:
      return execution::ast::Builtin::AddParamString;
    default:
      UNREACHABLE("Unsupported parameter type");
  }
}

// Static
std::vector<std::string> UdfCodegen::ParametersSortedByIndex(
    const std::unordered_map<std::string, std::pair<std::string, std::size_t>> &parameter_map) {
  // TODO(Kyle): This temporary data structure is gross
  std::unordered_map<std::string, std::size_t> parameters{};
  for (const auto &entry : parameter_map) {
    // Column Name -> (Parameter Name, Parameter Index)
    parameters[entry.second.first] = entry.second.second;
  }
  std::vector<std::string> result{};
  result.reserve(parameters.size());
  std::transform(parameters.cbegin(), parameters.cend(), std::back_inserter(result),
                 [](const std::pair<std::string, std::size_t> &entry) -> std::string { return entry.first; });
  std::sort(result.begin(), result.end(), [&parameters](const std::string &a, const std::string &b) -> bool {
    return parameters.at(a) < parameters.at(b);
  });
  return result;
}

// Static
std::vector<std::string> UdfCodegen::ColumnsSortedByIndex(
    const std::unordered_map<std::string, std::pair<std::string, std::size_t>> &parameter_map) {
  std::vector<std::string> result{};
  result.reserve(parameter_map.size());
  std::transform(parameter_map.cbegin(), parameter_map.cend(), std::back_inserter(result),
                 [](const std::pair<std::string, std::pair<std::string, std::size_t>> &entry) -> std::string {
                   return entry.first;
                 });
  std::sort(result.begin(), result.end(), [&parameter_map](const std::string &a, const std::string &b) -> bool {
    return parameter_map.at(a).second < parameter_map.at(b).second;
  });
  return result;
}

}  // namespace noisepage::execution::compiler::udf
