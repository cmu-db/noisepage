#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "libpg_query/pg_query.h"
#include "parser/parsenodes.h"
#include "parser/statements.h"

namespace terrier {
namespace parser {

/**
 * PostgresParser obtains and transforms the Postgres parse tree into our Terrier parse tree.
 * In the future, we may want to replace this with our own parser.
 */
class PostgresParser {
  /*
   * To modify this file, examine:
   *    List and ListCell in pg_list.h,
   *    Postgres types in nodes.h.
   *
   * To add new Statement support, find the parsenode in:
   *    third_party/libpg_query/src/postgres/include/nodes/parsenodes.h,
   *    third_party/libpg_query/src/postgres/include/nodes/primnodes.h,
   * then copy to src/include/parser/parsenodes.h and add the corresponding helper function.
   */

 public:
  PostgresParser();
  ~PostgresParser();

  /**
   * Builds the parse tree for the given query string.
   * @param query_string query string to be parsed
   * @return unique pointer to parse tree
   */
  std::vector<std::unique_ptr<parser::SQLStatement>> BuildParseTree(const std::string &query_string);

 private:
  static FKConstrActionType CharToActionType(const char &type) {
    switch (type) {
      case 'a':
        return FKConstrActionType::NOACTION;
      case 'r':
        return FKConstrActionType::RESTRICT_;
      case 'c':
        return FKConstrActionType::CASCADE;
      case 'n':
        return FKConstrActionType::SETNULL;
      case 'd':
        return FKConstrActionType::SETDEFAULT;
      default:
        return FKConstrActionType::NOACTION;
    }
  }

  static FKConstrMatchType CharToMatchType(const char &type) {
    switch (type) {
      case 'f':
        return FKConstrMatchType::FULL;
      case 'p':
        return FKConstrMatchType::PARTIAL;
      case 's':
        return FKConstrMatchType::SIMPLE;
      default:
        return FKConstrMatchType::SIMPLE;
    }
  }

  static bool IsAggregateFunction(const std::string &fun_name) {
    return (fun_name == "min" || fun_name == "max" || fun_name == "count" || fun_name == "avg" || fun_name == "sum");
  }

  /**
   * Transforms the entire parsed nodes list into a corresponding SQLStatementList.
   * @param root list of parsed nodes
   * @return SQLStatementList corresponding to the parsed node list
   */
  static std::vector<std::unique_ptr<parser::SQLStatement>> ListTransform(List *root);

  /**
   * Transforms a single node in the parse list into a terrier SQLStatement object.
   * @param node parsed node
   * @return SQLStatement corresponding to the parsed node
   */
  static std::unique_ptr<parser::SQLStatement> NodeTransform(Node *node);

  // expressions
  static std::unique_ptr<AbstractExpression> ExprTransform(Node *node);
  static std::unique_ptr<AbstractExpression> ExprTransform(Node *node, char *alias);
  static ExpressionType StringToExpressionType(const std::string &parser_str);
  static std::unique_ptr<AbstractExpression> AExprTransform(A_Expr *root);
  static std::unique_ptr<AbstractExpression> BoolExprTransform(BoolExpr *root);
  static std::unique_ptr<AbstractExpression> CaseExprTransform(CaseExpr *root);
  static std::unique_ptr<AbstractExpression> ColumnRefTransform(ColumnRef *root, char *alias);
  static std::unique_ptr<AbstractExpression> ConstTransform(A_Const *root);
  static std::unique_ptr<AbstractExpression> FuncCallTransform(FuncCall *root);
  static std::unique_ptr<AbstractExpression> NullTestTransform(NullTest *root);
  static std::unique_ptr<AbstractExpression> ParamRefTransform(ParamRef *root);
  static std::unique_ptr<AbstractExpression> SubqueryExprTransform(SubLink *node);
  static std::unique_ptr<AbstractExpression> TypeCastTransform(TypeCast *root);
  static std::unique_ptr<AbstractExpression> ValueTransform(value val);

  // SELECT statements
  static std::unique_ptr<SelectStatement> SelectTransform(SelectStmt *root);
  // SELECT helpers
  static std::vector<std::shared_ptr<AbstractExpression>> TargetTransform(List *root);
  static std::unique_ptr<TableRef> FromTransform(SelectStmt *select_root);
  static std::unique_ptr<GroupByDescription> GroupByTransform(List *group, Node *having_node);
  static std::unique_ptr<OrderByDescription> OrderByTransform(List *order);
  static std::unique_ptr<AbstractExpression> WhereTransform(Node *root);

  // FromTransform helpers
  static std::unique_ptr<JoinDefinition> JoinTransform(JoinExpr *root);
  static std::string AliasTransform(Alias *root);
  static std::unique_ptr<TableRef> RangeVarTransform(RangeVar *root);
  static std::unique_ptr<TableRef> RangeSubselectTransform(RangeSubselect *root);

  // COPY statements
  static std::unique_ptr<CopyStatement> CopyTransform(CopyStmt *root);

  // CREATE statements
  static std::unique_ptr<SQLStatement> CreateTransform(CreateStmt *root);
  static std::unique_ptr<SQLStatement> CreateDatabaseTransform(CreateDatabaseStmt *root);
  static std::unique_ptr<SQLStatement> CreateFunctionTransform(CreateFunctionStmt *root);
  static std::unique_ptr<SQLStatement> CreateIndexTransform(IndexStmt *root);
  static std::unique_ptr<SQLStatement> CreateSchemaTransform(CreateSchemaStmt *root);
  static std::unique_ptr<SQLStatement> CreateTriggerTransform(CreateTrigStmt *root);
  static std::unique_ptr<SQLStatement> CreateViewTransform(ViewStmt *root);

  // CREATE helpers
  using ColumnDefTransResult = struct {
    std::unique_ptr<ColumnDefinition> col;
    std::vector<std::shared_ptr<ColumnDefinition>> fks;
  };
  static ColumnDefTransResult ColumnDefTransform(ColumnDef *root);

  // CREATE FUNCTION helpers
  static std::unique_ptr<FuncParameter> FunctionParameterTransform(FunctionParameter *root);
  static std::unique_ptr<ReturnType> ReturnTypeTransform(TypeName *root);

  // CREATE TRIGGER helpers
  static std::unique_ptr<AbstractExpression> WhenTransform(Node *root);

  // DELETE statements
  static std::unique_ptr<DeleteStatement> DeleteTransform(DeleteStmt *root);

  // DELETE helpers
  // static parser::DeleteStatement *TruncateTransform(TruncateStmt *root);

  // DROP statements
  static std::unique_ptr<DropStatement> DropTransform(DropStmt *root);
  static std::unique_ptr<DropStatement> DropDatabaseTransform(DropDatabaseStmt *root);
  static std::unique_ptr<DropStatement> DropIndexTransform(DropStmt *root);
  static std::unique_ptr<DropStatement> DropSchemaTransform(DropStmt *root);
  static std::unique_ptr<DropStatement> DropTableTransform(DropStmt *root);
  static std::unique_ptr<DropStatement> DropTriggerTransform(DropStmt *root);

  // EXECUTE statements
  static std::unique_ptr<ExecuteStatement> ExecuteTransform(ExecuteStmt *root);

  // EXECUTE helpers
  static std::vector<std::shared_ptr<AbstractExpression>> ParamListTransform(List *root);

  // EXPLAIN statements
  static std::unique_ptr<ExplainStatement> ExplainTransform(ExplainStmt *root);

  // INSERT statements
  static std::unique_ptr<InsertStatement> InsertTransform(InsertStmt *root);

  // INSERT helpers
  static std::unique_ptr<std::vector<std::string>> ColumnNameTransform(List *root);
  static std::unique_ptr<std::vector<std::vector<std::shared_ptr<AbstractExpression>>>> ValueListsTransform(List *root);

  // PREPARE statements
  static std::unique_ptr<PrepareStatement> PrepareTransform(PrepareStmt *root);

  static std::unique_ptr<DeleteStatement> TruncateTransform(TruncateStmt *truncate_stmt);

  /**
   * Converts a TRANSACTION statement from postgres parser form to internal form
   *
   * @param transaction_stmt from the postgres parser
   * @return converted to parser::TransactionStatement
   */
  static std::unique_ptr<TransactionStatement> TransactionTransform(TransactionStmt *transaction_stmt);

  // VACUUM statements as ANALYZE statements
  static std::unique_ptr<AnalyzeStatement> VacuumTransform(VacuumStmt *root);

  // VARIABLE SET statements
  static std::unique_ptr<VariableSetStatement> VariableSetTransform(VariableSetStmt *root);

  /**
   * Converts the target of an update clause, i.e. one or more column = expression
   * statements, from postgres parser form to internal form
   * @param root list of targets
   * @return vector of update clauses
   */
  static std::vector<std::shared_ptr<parser::UpdateClause>> UpdateTargetTransform(List *root);

  /**
   * Converts an UPDATE statement from postgres parser form to our internal form.
   * @param update_stmt from the postgres parser
   * @return converted to a parser::UpdateStatement
   *
   * TODO: Does not support:
   * - with clause
   * - from clause
   * - returning a list
   */
  static std::unique_ptr<UpdateStatement> UpdateTransform(UpdateStmt *update_stmt);
};

}  // namespace parser
}  // namespace terrier
