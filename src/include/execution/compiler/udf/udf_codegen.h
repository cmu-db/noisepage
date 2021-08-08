#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/ast/udf/udf_ast_context.h"
#include "execution/ast/udf/udf_ast_node_visitor.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"
#include "execution/functions/function_context.h"
#include "planner/plannodes/abstract_join_plan_node.h"

namespace noisepage::catalog {
class CatalogAccessor;
}  // namespace noisepage::catalog

namespace noisepage::optimizer {
class OptimizeResult;
}  // namespace noisepage::optimizer

namespace noisepage::parser::udf {
class VariableRef;
}  // namespace noisepage::parser::udf

namespace noisepage::execution {

namespace compiler {
class ExecutableQuery;
}  // namespace compiler

namespace vm {
class FunctionInfo;
}  // namespace vm

// Forward declarations
namespace ast::udf {
class AbstractAST;
class StmtAST;
class ExprAST;
class ValueExprAST;
class VariableExprAST;
class BinaryExprAST;
class CallExprAST;
class MemberExprAST;
class SeqStmtAST;
class DeclStmtAST;
class IfStmtAST;
class WhileStmtAST;
class RetStmtAST;
class AssignStmtAST;
class SQLStmtAST;
class FunctionAST;
class IsNullExprAST;
class DynamicSQLStmtAST;
class ForSStmtAST;
}  // namespace ast::udf

namespace compiler::udf {

class ExpressionResultScope;

/**
 * The UdfCodegen class implements a visitor for UDF AST
 * nodes and encapsulates all of the logic required to generate
 * code from the UDF abstract syntax tree.
 */
class UdfCodegen : ast::udf::ASTNodeVisitor {
 public:
  /**
   * Construct a new UdfCodegen instance.
   * @param accessor The catalog accessor used in code generation
   * @param fb The function builder instance used for the UDF
   * @param udf_ast_context The AST context for the UDF
   * @param codegen The codegen instance
   * @param db_oid The OID for the relevant database
   */
  UdfCodegen(catalog::CatalogAccessor *accessor, FunctionBuilder *fb, ast::udf::UdfAstContext *udf_ast_context,
             CodeGen *codegen, catalog::db_oid_t db_oid);

  /** Destroy the UDF code generation context. */
  ~UdfCodegen() override = default;

  /**
   * Run UDF code generation.
   * @param accessor The catalog accessor
   * @param function_builder The function builder to use during code generation
   * @param ast_context The UDF AST context
   * @param codegen The code generation instance
   * @param db_oid The database OID
   * @param root The root of the UDF AST for which code is generated
   * @return The file containing the generated code
   */
  static execution::ast::File *Run(catalog::CatalogAccessor *accessor, FunctionBuilder *function_builder,
                                   ast::udf::UdfAstContext *ast_context, CodeGen *codegen, catalog::db_oid_t db_oid,
                                   ast::udf::FunctionAST *root);

 private:
  /**
   * Generate a UDF from the given abstract syntax tree.
   * @param ast The AST from which to generate the UDF
   */
  void GenerateUDF(ast::udf::AbstractAST *ast);

  /**
   * Visit an AbstractAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::AbstractAST *ast) override;

  /**
   * Visit a FunctionAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::FunctionAST *ast) override;

  /**
   * Visit a StmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::StmtAST *ast) override;

  /**
   * Visit an ExprAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::ExprAST *ast) override;

  /**
   * Visit a ValueExprAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::ValueExprAST *ast) override;

  /**
   * Visit a VariableExprAST node.
   */
  void Visit(ast::udf::VariableExprAST *ast) override;

  /**
   * Visit a BinaryExprAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::BinaryExprAST *ast) override;

  /**
   * Visit a CallExprAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::CallExprAST *ast) override;

  /**
   * Visit an IsNullExprAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::IsNullExprAST *ast) override;

  /**
   * Visit a SeqStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::SeqStmtAST *ast) override;

  /**
   * Visit a DeclStmtNode node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::DeclStmtAST *ast) override;

  /**
   * Visit a IfStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::IfStmtAST *ast) override;

  /**
   * Visit a WhileStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::WhileStmtAST *ast) override;

  /**
   * Visit a RetStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::RetStmtAST *ast) override;

  /**
   * Visit an AssignStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::AssignStmtAST *ast) override;

  /**
   * Visit a SQLStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::SQLStmtAST *ast) override;

  /**
   * Visit a DynamicSQLStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::DynamicSQLStmtAST *ast) override;

  /**
   * Visit a ForIStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::ForIStmtAST *ast) override;

  /**
   * Visit a ForSStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::ForSStmtAST *ast) override;

  /**
   * Visit a MemberExprAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::MemberExprAST *ast) override;

  /**
   * Complete UDF code generation.
   * @return The result of code generation as a file
   */
  execution::ast::File *Finish();

  /**
   * Return the string that represents the return value.
   * @return The string that represents the return value
   */
  static const char *GetReturnParamString();

 private:
  /* --------------------------------------------------------------------------
    Code Generation: For-S Loops
  -------------------------------------------------------------------------- */

  /**
   * Begin construction of a lambda that writes the output of the query
   * represented by `plan` into the variables identified by `variables`.
   * @param plan The query plan
   * @param variables The names of the variables to which results are bound
   * @return The unfinished function builder for the lambda
   */
  std::unique_ptr<FunctionBuilder> StartLambda(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                               const std::vector<std::string> &variables);

  /**
   * Begin construction of a lambda that writes the output of the query
   * represented by `plan` into a single RECORD-type variable.
   * @param plan The query plan
   * @param variables The names of the variables to which results are bound
   * @return The unfinished function builder for the lambda
   */
  std::unique_ptr<FunctionBuilder> StartLambdaBindingToRecord(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                                              const std::vector<std::string> &variables);

  /**
   * Begin construction of a lambda that writes the output of the query
   * represented by `plan` into one or more non-RECORD variables.
   * @param plan The query plan
   * @param variables The names of the variables to which results are bound
   * @return The unfinished function builder for the lambda
   */
  std::unique_ptr<FunctionBuilder> StartLambdaBindingToScalars(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                                               const std::vector<std::string> &variables);

  /* --------------------------------------------------------------------------
    Code Generation: SQL Statements
  -------------------------------------------------------------------------- */

  /**
   * Construct a lambda expression that writes the output of the query
   * represented by `plan` into the variables identified by `variables`.
   * @param plan The query plan
   * @param variables The names of the variables to which results are bound
   * @return The finished lambda expression
   */
  ast::LambdaExpr *MakeLambda(common::ManagedPointer<planner::AbstractPlanNode> plan,
                              const std::vector<std::string> &variables);

  /**
   * Construct a lambda expression that writes the output of the query
   * represented by `plan` into a single RECORD-type variable.
   * @param plan The query plan
   * @param variables The names of the variables to which results are bound
   * @return The finished lambda expression
   */
  ast::LambdaExpr *MakeLambdaBindingToRecord(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                             const std::vector<std::string> &variables);

  /**
   * Construct a lambda expression that writes the output of the query
   * represented by `plan` into one or more non-RECORD variables.
   * @param plan The query plan
   * @param variables The names of the variables to which results are bound
   * @return The finished lambda expression
   */
  ast::LambdaExpr *MakeLambdaBindingToScalars(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                              const std::vector<std::string> &variables);

  /* --------------------------------------------------------------------------
    Code Generation: Common
  -------------------------------------------------------------------------- */

  /**
   * Generate code to add query parameters to the execution context.
   * @param exec_ctx The execution context expression
   * @param variable_refs The collection of variable references
   */
  void CodegenAddParameters(ast::Expr *exec_ctx, const std::vector<parser::udf::VariableRef> &variable_refs);

  /**
   * Generate code to add a scalar parameter to the execution context.
   * @param exec_ctx The execution context
   * @param variable_ref The variable reference
   */
  void CodegenAddScalarParameter(ast::Expr *exec_ctx, const parser::udf::VariableRef &variable_ref);

  /**
   * Generate code to add a non-scalar parameter to the execution context.
   * @param exec_ctx The execution context
   * @param variable_ref The variable reference
   */
  void CodegenAddTableParameter(ast::Expr *exec_ctx, const parser::udf::VariableRef &variable_ref);

  /**
   * Generate code to initialize bound variables.
   * @param plan The query plan
   * @param bound_variables The variables to which results of the query are bound
   */
  void CodegenBoundVariableInit(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                const std::vector<std::string> &bound_variables);

  /**
   * Generate code to initialize bound scalar variables.
   * @param plan The query plan
   * @param bound_variables The name(s) of the scalar variables to which results of the query are bound
   */
  void CodegenBoundVariableInitForScalars(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                          const std::vector<std::string> &bound_variables);

  /**
   * Generate code to initialize a bound record variable.
   * @param plan The query plan
   * @param record_name The name of the record variable to which results of the query are bound
   */
  void CodegenBoundVariableInitForRecord(common::ManagedPointer<planner::AbstractPlanNode> plan,
                                         const std::string &record_name);

  /**
   * Generate code to invoke each top-level function in the executable query.
   * @param exec_query The executable query for which calls are generated
   * @param query_state_id The identifier for the query state
   * @param lambda_id The identifier for the lambda expression that
   * is used as an output callback in the query
   */
  void CodegenTopLevelCalls(const ExecutableQuery *exec_query, ast::Identifier query_state_id,
                            ast::Identifier lambda_id);

  /**
   * Translate a SQL type to its corresponding catalog type.
   * @param type The SQL type of interest
   * @return The corresponding catalog type
   */
  catalog::type_oid_t GetCatalogTypeOidFromSQLType(execution::ast::BuiltinType::Kind type);

  /** @return A mutable reference to the symbol table */
  std::unordered_map<std::string, execution::ast::Identifier> &SymbolTable() { return symbol_table_; }

  /** @return An immutable reference to the symbol table */
  const std::unordered_map<std::string, execution::ast::Identifier> &SymbolTable() const { return symbol_table_; }

  /**
   * Get the type of the variable identified by `name`.
   * @param name The name of the variable
   * @return The type of the variable identified by `name`
   * @throw EXECUTION_EXCEPTION on failure to resolve type
   */
  type::TypeId GetVariableType(const std::string &name) const;

  /**
   * Get the type of the record variable identified by `name`.
   * @param name The name of the variable
   * @return The type of the record variable identified by `name`
   * @throw EXECUTION_EXCEPTION on failure to resolve type
   */
  std::vector<std::pair<std::string, type::TypeId>> GetRecordType(const std::string &name) const;

  /**
   * Bind the query and return the variable references.
   * @param query The parsed query
   * @return The collection of variable references
   */
  std::vector<parser::udf::VariableRef> BindQueryAndGetVariableRefs(parser::ParseResult *query);

  /**
   * Run the optimizer on an embedded SQL query.
   * @param parsed_query The result of parsing the query
   * @return The optimized result
   */
  std::unique_ptr<optimizer::OptimizeResult> OptimizeEmbeddedQuery(parser::ParseResult *parsed_query);

  /**
   * Determine if the function described by the given metdata is a
   * top-level run function that accepts an output callback argument.
   * @param function_metatdata The function metadata
   * @return `true` if the function meets the above criteria, `false` otherwise
   */
  static bool IsRunAllFunction(const std::string &name);

  /**
   * Get the builtin parameter-add function for the specified parameter type.
   * @param parameter_type The parameter type
   * @return The builtin function to add this parameter
   */
  static ast::Builtin AddParamBuiltinForParameterType(type::TypeId parameter_type);

  /**
   * TODO(Kyle): this
   */
  static std::vector<std::string> ParametersSortedByIndex(
      const std::unordered_map<std::string, std::pair<std::string, std::size_t>> &parameter_map);

  /**
   * TODO(Kyle): this
   */
  static std::vector<std::string> ColumnsSortedByIndex(
      const std::unordered_map<std::string, std::pair<std::string, std::size_t>> &parameter_map);

  /** @return The execution context provided to the function */
  ast::Expr *GetExecutionContext();

  /** @return The current execution result expression */
  ast::Expr *GetExecutionResult();

  /**
   * Set the current execution result expression.
   * @param The execution result expression
   */
  void SetExecutionResult(ast::Expr *result);

  /**
   * Stage evaluation of the expression `expr` by generating
   * code to perform the evaluation (at runtime).
   * @param expr The expression to evaluate
   * @return The result of evaluating the expression
   */
  ast::Expr *EvaluateExpression(ast::udf::ExprAST *expr);

 private:
  /** The string identifier for internal declarations */
  constexpr static const char INTERNAL_DECL_ID[] = "*internal*";

  /** The catalog access used during code generation */
  catalog::CatalogAccessor *accessor_;

  /** The function builder used during code generation */
  FunctionBuilder *fb_;

  /** The AST context for the UDF */
  ast::udf::UdfAstContext *udf_ast_context_;

  /** The code generation instance */
  CodeGen *codegen_;

  /** The OID of the relevant database */
  catalog::db_oid_t db_oid_;

  /** Auxiliary declarations */
  execution::util::RegionVector<execution::ast::Decl *> aux_decls_;

  /** The current type during code generation */
  type::TypeId current_type_{type::TypeId::INVALID};

  /** The current execution result expression */
  execution::ast::Expr *execution_result_;

  /** Map from human-readable string identifier to internal identifier */
  std::unordered_map<std::string, execution::ast::Identifier> symbol_table_;
};

}  // namespace compiler::udf
}  // namespace noisepage::execution
