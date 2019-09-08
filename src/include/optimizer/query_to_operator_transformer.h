#pragma once

#include <memory>
#include <unordered_set>
#include "common/sql_node_visitor.h"

namespace terrier {

namespace parser {
class SQLStatement;
class AbstractExpression;
}  // namespace parser

namespace catalog{
class CatalogAccessor;
}

namespace optimizer {
class AnnotatedExpression;
class OperatorExpression;

/**
 * @brief Transform a query from parsed statement to operator expressions.
 */
class QueryToOperatorTransformer : public SqlNodeVisitor {
 public:
  explicit QueryToOperatorTransformer(std::unique_ptr<catalog::CatalogAccessor> catalog_accessor);

  OperatorExpression* ConvertToOpExpression(parser::SQLStatement *op);

  void Visit(parser::SelectStatement *op) override;

  void Visit(parser::TableRef *) override;
  void Visit(parser::JoinDefinition *) override;
  void Visit(parser::GroupByDescription *) override;
  void Visit(parser::OrderByDescription *) override;
  void Visit(parser::LimitDescription *) override;

  void Visit(parser::CreateStatement *op) override;
  void Visit(parser::CreateFunctionStatement *op) override;
  void Visit(parser::InsertStatement *op) override;
  void Visit(parser::DeleteStatement *op) override;
  void Visit(parser::DropStatement *op) override;
  void Visit(parser::PrepareStatement *op) override;
  void Visit(parser::ExecuteStatement *op) override;
  void Visit(parser::TransactionStatement *op) override;
  void Visit(parser::UpdateStatement *op) override;
  void Visit(parser::CopyStatement *op) override;
  void Visit(parser::AnalyzeStatement *op) override;
  void Visit(parser::ComparisonExpression *expr) override;
  void Visit(parser::OperatorExpression *expr) override;

 private:
  /**
   * @brief Walk through an expression, split it into a set of predicates that could be joined by conjunction.
   * We need the set to perform predicate push-down. For example, for the following query
   * "SELECT test.a, test1.b FROM test, test1 WHERE test.a = test1.b and test.a = 5"
   * we could will extract "test.a = test1.b" and "test.a = 5" from the original predicate
   * and we could let "test.a = 5" to be evaluated at the table scan level
   *
   * @param expr The original predicate
   */
  std::vector<AnnotatedExpression> CollectPredicates(common::ManagedPointer<parser::AbstractExpression> expr, std::vector<AnnotatedExpression> predicates = {});

  /**
   * @brief Transform a sub-query in an expression to use
   *
   * @param expr The potential sub-query expression
   * @param select_list The select list of the sub-query we generate
   * @param single_join A boolean to tell if
   *
   * @return If the expression could be transformed into sub-query, return true,
   *  return false otherwise
   */

  // TODO(Ling): the documentation has second parameter select_list, whereas the function definition has the second
  //  parameter oid_t child oid. What exactly is the second parameter supposed to be? If it is supposed to be an oid,
  //  what kind of oid should it be?
  bool GenerateSubqueryTree(parser::AbstractExpression *expr, int child_id, bool single_join = false);

  /**
   * @brief Decide if a conjunctive predicate is supported. We need to extract
   * conjunction predicate first then call this function to decide if the
   * predicate is supported by our system
   *
   * @param expr The conjunctive predicate provided
   *
   * @return True if supported, false otherwise
   */
  static bool IsSupportedConjunctivePredicate(common::ManagedPointer<parser::AbstractExpression> expr);

  /**
   * @brief Check if a sub-select statement is supported.
   *
   * @param op The select statement
   *
   * @return True if supported, false otherwise
   */
  static bool IsSupportedSubSelect(common::ManagedPointer<parser::SelectStatement> op);
  static bool RequireAggregation(common::ManagedPointer<parser::SelectStatement> op);

  /**
   * @brief Extract single table precates and multi-table predicates from the expr
   *
   * @param expr The original predicate
   * @param annotated_predicates The extracted conjunction predicates
   */
  static std::vector<AnnotatedExpression> ExtractPredicates(common::ManagedPointer<parser::AbstractExpression> expr,
      std::vector<AnnotatedExpression> annotated_predicates = {});

  /**
   * Generate a set of table alias included in an expression
   */
  static void GenerateTableAliasSet(const parser::AbstractExpression *expr, std::unordered_set<std::string> &table_alias_set);

  /**
 * @breif Split conjunction expression tree into a vector of expressions with AND
 */
  static void SplitPredicates(common::ManagedPointer<parser::AbstractExpression> expr, std::vector<common::ManagedPointer<parser::AbstractExpression>> &predicates);

  static std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>> ConstructSelectElementMap(const std::vector<common::ManagedPointer<parser::AbstractExpression>> &select_list);

  OperatorExpression* output_expr_;

  std::unique_ptr<catalog::CatalogAccessor> accessor_;


//  /**
//   * @brief The Depth of the current operator, we need this to tell if there's
//   *  dependent join in the query. Dependent join transformation logic is not
//   *  implemented yet
//   */
//  int depth_;

  /**
   * @brief A set of predicates the current operator generated, we use them to
   *  generate filter operator
   */
  std::vector<AnnotatedExpression> predicates_;
};

}  // namespace optimizer
}  // namespace terrier
