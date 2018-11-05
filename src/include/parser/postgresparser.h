#pragma once

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
   * @return static instance of the PostgresParser
   */
  static PostgresParser &GetInstance();

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
  static std::vector<std::unique_ptr<parser::SQLStatement>> ListTransform(List *root);

  /**
   * Transforms a single node in the parse list into a terrier SQLStatement object.
   * @param node parsed node
   * @return SQLStatement corresponding to the parsed node
   */
  static std::unique_ptr<parser::SQLStatement> NodeTransform(Node *node);

  // expressions
  static std::unique_ptr<AbstractExpression> ExprTransform(Node *node);
  static ExpressionType StringToExpressionType(const std::string &parser_str);

  static std::unique_ptr<AbstractExpression> AExprTransform(A_Expr *root);
  static std::unique_ptr<AbstractExpression> BoolExprTransform(BoolExpr *root);
  static std::unique_ptr<AbstractExpression> CaseExprTransform(CaseExpr *root);  // TODO(WAN)
  static std::unique_ptr<AbstractExpression> ColumnRefTransform(ColumnRef *root);
  static std::unique_ptr<AbstractExpression> ConstTransform(A_Const *root);
  static std::unique_ptr<AbstractExpression> FuncCallTransform(FuncCall *root);  // TODO(WAN)
  static std::unique_ptr<AbstractExpression> NullTestTransform(NullTest *root);
  static std::unique_ptr<AbstractExpression> ParamRefTransform(ParamRef *root);
  static std::unique_ptr<AbstractExpression> SubqueryExprTransform(SubLink *node);
  static std::unique_ptr<AbstractExpression> TypeCastTransform(TypeCast *root);
  static std::unique_ptr<AbstractExpression> ValueTransform(value val);

  // SELECT statements
  static std::unique_ptr<SelectStatement> SelectTransform(SelectStmt *root);
  // SELECT helpers
  static std::vector<std::unique_ptr<AbstractExpression>> TargetTransform(List *root);
  static std::unique_ptr<TableRef> FromTransform(SelectStmt *select_root);
  static std::unique_ptr<GroupByDescription> GroupByTransform(List *group, Node *having_node);
  static std::unique_ptr<OrderByDescription> OrderByTransform(List *order);
  static std::unique_ptr<AbstractExpression> WhereTransform(Node *root);

  // FromTransform helpers
  static std::unique_ptr<JoinDefinition> JoinTransform(JoinExpr *root);
  static std::string AliasTransform(Alias *root);
  static std::unique_ptr<TableRef> RangeVarTransform(RangeVar *root);
  static std::unique_ptr<TableRef> RangeSubselectTransform(RangeSubselect *root);

  // CREATE statements
  static std::unique_ptr<SQLStatement> CreateTransform(CreateStmt *root);
  static std::unique_ptr<SQLStatement> CreateDatabaseTransform(CreateDatabaseStmt *root);
  static std::unique_ptr<SQLStatement> CreateFunctionTransform(CreateFunctionStmt *root);

  // CREATE helpers
  using ColumnDefTransResult = struct {
    std::unique_ptr<ColumnDefinition> col;
    std::vector<std::unique_ptr<ColumnDefinition>> fks;
  };
  static ColumnDefTransResult ColumnDefTransform(ColumnDef *root);

  // INSERT statements
  static std::unique_ptr<InsertStatement> InsertTransform(InsertStmt *root);

  // INSERT helpers
  static std::unique_ptr<std::vector<std::string>> ColumnNameTransform(List *root);
  static std::unique_ptr<std::vector<std::vector<std::unique_ptr<AbstractExpression>>>> ValueListsTransform(List *root);

  /*
    //===--------------------------------------------------------------------===//
    // Transform Functions
    //===--------------------------------------------------------------------===//

    // transform helper for internal parse tree
    static parser::SQLStatementList *PgQueryInternalParsetreeTransform(
        PgQueryInternalParsetreeAndError stmt);


   //
   static AbstractExpression *WhenTransform(Node *root);



    // transform helper for function parameters
    static parser::FuncParameter *FunctionParameterTransform(
        FunctionParameter *root);

    // transforms helper for return type
    static parser::ReturnType *ReturnTypeTransform(TypeName *root);

    // transform helper for create index statements
    static parser::SQLStatement *CreateIndexTransform(IndexStmt *root);

    // transform helper for create trigger statements
    static parser::SQLStatement *CreateTriggerTransform(CreateTrigStmt *root);


    // transform helper for create schema statements
    static parser::SQLStatement *CreateSchemaTransform(CreateSchemaStmt *root);

    // transform helper for create view statements
    static parser::SQLStatement *CreateViewTransform(ViewStmt *root);



    // transform helper for explain statements
    static parser::SQLStatement *ExplainTransform(ExplainStmt *root);

    // transform helper for delete statements
    static parser::SQLStatement *DeleteTransform(DeleteStmt *root);



    // transform helper for update statement
    static parser::UpdateStatement *UpdateTransform(UpdateStmt *update_stmt);

    // transform helper for update statement
    static std::vector<std::unique_ptr<parser::UpdateClause>>
        *UpdateTargetTransform(List *root);

    // transform helper for drop statement
    static parser::DropStatement *DropTransform(DropStmt *root);


     * @brief transform helper for drop database statement
     *
     * @param Postgres DropDatabaseStmt parsenode
     * @return a peloton DropStatement node

    static parser::DropStatement *DropDatabaseTransform(DropDatabaseStmt *root);

    // transform helper for drop table statement
    static parser::DropStatement *DropTableTransform(DropStmt *root);

    // transform helper for drop trigger statement
    static parser::DropStatement *DropTriggerTransform(DropStmt *root);

    // transform helper for drop schema statement
    static parser::DropStatement *DropSchemaTransform(DropStmt *root);

    // tranform helper for drop index statement
    static parser::DropStatement *DropIndexTransform(DropStmt *root);

    // transform helper for truncate statement
    static parser::DeleteStatement *TruncateTransform(TruncateStmt *root);

    // transform helper for transaction statement
    static parser::TransactionStatement *TransactionTransform(
        TransactionStmt *root);

    // transform helper for execute statement
    static parser::ExecuteStatement *ExecuteTransform(ExecuteStmt *root);

    static std::vector<std::unique_ptr<expression::AbstractExpression>>
    ParamListTransform(List *root);

    // transform helper for execute statement
    static parser::PrepareStatement *PrepareTransform(PrepareStmt *root);

    // transform helper for execute statement
    static parser::CopyStatement *CopyTransform(CopyStmt *root);

    // transform helper for analyze statement
    static parser::AnalyzeStatement *VacuumTransform(VacuumStmt* root);

    static parser::VariableSetStatement *VariableSetTransform(VariableSetStmt* root);

  */
};

}  // namespace parser
}  // namespace terrier
