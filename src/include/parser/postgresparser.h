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

// General guidelines for working with this file
// If it takes in a parse result, or returns a managed pointer,
// it will add all it creates to the parse result.

struct ParseResult {
  /* Statements to be executed. */
  std::vector<SQLStatement *> stmts;
  /* Auxiliary statements that we saw while parsing, e.g. nested statements. */
  std::vector<SQLStatement *> aux_stmts;
  /* Expressions. */
  std::vector<AbstractExpression *> exprs;
  /* Table location information. */
  std::vector<TableInfo *> table_infos;
  /* Table locator (i.e. contains table info or select statement. */
  std::vector<TableRef *> table_refs;
  /* Group by description. */
  std::vector<GroupByDescription *> group_bys;
  /* Order by description. */
  std::vector<OrderByDescription *> order_bys;
  /* Limit descriptions. */
  std::vector<LimitDescription *> limit_descriptions;
  /* Update clauses. */
  std::vector<UpdateClause *> update_clauses;
  /* Join definitions. */
  std::vector<JoinDefinition *> join_definitions;
  /* Index attributes. */
  std::vector<IndexAttr *> index_attrs;
  /* Column definitions. */
  std::vector<ColumnDefinition *> column_definitions;
  /* Function parameters. */
  std::vector<FuncParameter *> func_parameters;
  /* Return types. */
  std::vector<ReturnType *> return_types;

  void AddStatement(bool is_top_level, SQLStatement *stmt) {
    if (is_top_level) {
      this->stmts.emplace_back(stmt);
    } else {
      this->aux_stmts.emplace_back(stmt);
    }
  }

  void AddExpression(bool is_top_level, AbstractExpression *expr) {
    // TODO(WAN): get rid of this. formerly, expressions would own their subexpressions, but
    // with the removal of .get() it seems that we're headed deeper into managed pointer land.
    // The alternative I see would be to have raw pointers flying around this already messy file.
    this->exprs.emplace_back(expr);
  }
};

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
  ParseResult BuildParseTree(const std::string &query_string);

 private:
  static FKConstrActionType CharToActionType(const char &type) {
    switch (type) {
      case 'a':
        return FKConstrActionType::NOACTION;
      case 'r':
        return FKConstrActionType::RESTRICT;
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
  static ParseResult ListTransform(List *root);

  /**
   * Transforms a single node in the parse list into a terrier SQLStatement object.
   * @param result result context to be updated
   * @param is_top_level whether this parse node is a top-level node
   * @param node parsed node
   * @return SQLStatement corresponding to the parsed node
   */
  static common::ManagedPointer<SQLStatement> NodeTransform(ParseResult *result, bool is_top_level, Node *node);

  // expressions
  static common::ManagedPointer<AbstractExpression> ExprTransform(ParseResult *result, bool is_top_level, Node *node, char *alias);
  static ExpressionType StringToExpressionType(const std::string &parser_str);

  static common::ManagedPointer<AbstractExpression> AExprTransform(ParseResult *result, bool is_top_level,
                                                                   A_Expr *root);
  static common::ManagedPointer<AbstractExpression> BoolExprTransform(ParseResult *result, bool is_top_level,
                                                                      BoolExpr *root);
  static common::ManagedPointer<AbstractExpression> CaseExprTransform(ParseResult *result, bool is_top_level,
                                                                      CaseExpr *root);
  static common::ManagedPointer<AbstractExpression> ColumnRefTransform(ParseResult *result, bool is_top_level,
                                                                       ColumnRef *root, char *alias);
  static common::ManagedPointer<AbstractExpression> ConstTransform(ParseResult *result, bool is_top_level,
                                                                   A_Const *root);
  static common::ManagedPointer<AbstractExpression> FuncCallTransform(ParseResult *result, bool is_top_level,
                                                                      FuncCall *root);
  static common::ManagedPointer<AbstractExpression> NullTestTransform(ParseResult *result, bool is_top_level,
                                                                      NullTest *root);
  static common::ManagedPointer<AbstractExpression> ParamRefTransform(ParseResult *result, bool is_top_level,
                                                                      ParamRef *root);
  static common::ManagedPointer<AbstractExpression> SubqueryExprTransform(ParseResult *result, bool is_top_level,
                                                                          SubLink *node);
  static common::ManagedPointer<AbstractExpression> TypeCastTransform(ParseResult *result, bool is_top_level,
                                                                      TypeCast *root);
  static common::ManagedPointer<AbstractExpression> ValueTransform(ParseResult *result, bool is_top_level, value val);

  // SELECT statements
  static common::ManagedPointer<SQLStatement> SelectTransform(ParseResult *result, bool is_top_level, SelectStmt *root);
  // SELECT helpers
  static std::vector<common::ManagedPointer<AbstractExpression>> TargetTransform(ParseResult *result, List *root);
  static common::ManagedPointer<TableRef> FromTransform(ParseResult *result, SelectStmt *select_root);
  static common::ManagedPointer<GroupByDescription> GroupByTransform(ParseResult *result, List *group,
                                                                     Node *having_node);
  static common::ManagedPointer<OrderByDescription> OrderByTransform(ParseResult *result, List *order);
  static common::ManagedPointer<AbstractExpression> WhereTransform(ParseResult *result, Node *root);

  // FromTransform helpers
  static common::ManagedPointer<JoinDefinition> JoinTransform(ParseResult *result, JoinExpr *root);
  static std::string AliasTransform(Alias *root);
  static common::ManagedPointer<TableRef> RangeVarTransform(ParseResult *result, RangeVar *root);
  static common::ManagedPointer<TableRef> RangeSubselectTransform(ParseResult *result, RangeSubselect *root);

  // COPY statements
  static common::ManagedPointer<SQLStatement> CopyTransform(ParseResult *result, bool is_top_level, CopyStmt *root);

  // CREATE statements
  static common::ManagedPointer<SQLStatement> CreateTransform(ParseResult *result, bool is_top_level, CreateStmt *root);
  static common::ManagedPointer<SQLStatement> CreateDatabaseTransform(ParseResult *result, bool is_top_level,
                                                                      CreateDatabaseStmt *root);
  static common::ManagedPointer<SQLStatement> CreateFunctionTransform(ParseResult *result, bool is_top_level,
                                                                      CreateFunctionStmt *root);
  static common::ManagedPointer<SQLStatement> CreateIndexTransform(ParseResult *result, bool is_top_level,
                                                                   IndexStmt *root);
  static common::ManagedPointer<SQLStatement> CreateSchemaTransform(ParseResult *result, bool is_top_level,
                                                                    CreateSchemaStmt *root);
  static common::ManagedPointer<SQLStatement> CreateTriggerTransform(ParseResult *result, bool is_top_level,
                                                                     CreateTrigStmt *root);
  static common::ManagedPointer<SQLStatement> CreateViewTransform(ParseResult *result, bool is_top_level,
                                                                  ViewStmt *root);

  // CREATE helpers
  using ColumnDefTransResult = struct {
    common::ManagedPointer<ColumnDefinition> col;
    std::vector<common::ManagedPointer<ColumnDefinition>> fks;
  };
  static ColumnDefTransResult ColumnDefTransform(ParseResult *result, ColumnDef *root);

  // CREATE FUNCTION helpers
  static common::ManagedPointer<FuncParameter> FunctionParameterTransform(ParseResult *result, FunctionParameter *root);
  static common::ManagedPointer<ReturnType> ReturnTypeTransform(ParseResult *result, TypeName *root);

  // CREATE TRIGGER helpers
  static common::ManagedPointer<AbstractExpression> WhenTransform(ParseResult *result, Node *root);

  // DELETE statements
  static common::ManagedPointer<SQLStatement> DeleteTransform(ParseResult *result, bool is_top_level, DeleteStmt *root);

  // DELETE helpers
  // static parser::DeleteStatement *TruncateTransform(TruncateStmt *root);

  // DROP statements
  static common::ManagedPointer<SQLStatement> DropTransform(ParseResult *result, bool is_top_level, DropStmt *root);
  static common::ManagedPointer<SQLStatement> DropDatabaseTransform(ParseResult *result, bool is_top_level,
                                                                    DropDatabaseStmt *root);
  static common::ManagedPointer<SQLStatement> DropIndexTransform(ParseResult *result, bool is_top_level,
                                                                 DropStmt *root);
  static common::ManagedPointer<SQLStatement> DropSchemaTransform(ParseResult *result, bool is_top_level,
                                                                  DropStmt *root);
  static common::ManagedPointer<SQLStatement> DropTableTransform(ParseResult *result, bool is_top_level,
                                                                 DropStmt *root);
  static common::ManagedPointer<SQLStatement> DropTriggerTransform(ParseResult *result, bool is_top_level,
                                                                   DropStmt *root);

  // EXECUTE statements
  static common::ManagedPointer<SQLStatement> ExecuteTransform(ParseResult *result, bool is_top_level,
                                                               ExecuteStmt *root);

  // EXECUTE helpers
  static std::vector<common::ManagedPointer<AbstractExpression>> ParamListTransform(ParseResult *result, List *root);

  // EXPLAIN statements
  static common::ManagedPointer<SQLStatement> ExplainTransform(ParseResult *result, bool is_top_level,
                                                               ExplainStmt *root);

  // INSERT statements
  static common::ManagedPointer<SQLStatement> InsertTransform(ParseResult *result, bool is_top_level, InsertStmt *root);

  // INSERT helpers
  static std::vector<std::string> ColumnNameTransform(List *root);
  static std::vector<std::vector<common::ManagedPointer<AbstractExpression>>> ValueListsTransform(ParseResult *result,
                                                                                                  List *root);

  // PREPARE statements
  static common::ManagedPointer<SQLStatement> PrepareTransform(ParseResult *result, bool is_top_level,
                                                               PrepareStmt *root);

  static common::ManagedPointer<SQLStatement> TruncateTransform(ParseResult *result, bool is_top_level,
                                                                TruncateStmt *truncate_stmt);

  /**
   * Converts a TRANSACTION statement from postgres parser form to internal form
   *
   * @param transaction_stmt from the postgres parser
   * @return converted to parser::TransactionStatement
   */
  static common::ManagedPointer<SQLStatement> TransactionTransform(ParseResult *result, bool is_top_level,
                                                                   TransactionStmt *transaction_stmt);

  // VACUUM statements as ANALYZE statements
  static common::ManagedPointer<SQLStatement> VacuumTransform(ParseResult *result, bool is_top_level, VacuumStmt *root);

  // VARIABLE SET statements
  static common::ManagedPointer<SQLStatement> VariableSetTransform(ParseResult *result, bool is_top_level,
                                                                   VariableSetStmt *root);

  /**
   * Converts the target of an update clause, i.e. one or more column = expression
   * statements, from postgres parser form to internal form
   * @param result parse result to hold the update clauses
   * @param root list of targets
   * @return vector of update clauses
   */
  static std::vector<common::ManagedPointer<UpdateClause>> UpdateTargetTransform(ParseResult *result, List *root);

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
  static common::ManagedPointer<SQLStatement> UpdateTransform(ParseResult *result, bool is_top_level,
                                                              UpdateStmt *update_stmt);
};

}  // namespace parser
}  // namespace terrier
