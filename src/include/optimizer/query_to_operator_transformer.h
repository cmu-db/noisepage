#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "parser/parser_defs.h"
#include "optimizer/abstract_optimizer_node.h"

namespace noisepage {

namespace parser {
class ParseResult;
class SQLStatement;
class AbstractExpression;
}  // namespace parser

namespace catalog {
class CatalogAccessor;
class Schema;
}  // namespace catalog

namespace optimizer {
class AnnotatedExpression;
class OperatorNode;

/**
 * Transform a query from parsed statement to operator expressions.
 */
class QueryToOperatorTransformer : public binder::SqlNodeVisitor {
 public:
  /**
   * Initialize the query to operator transformer object with a non-owning pointer to the catalog accessor
   * @param catalog_accessor Pointer to a catalog accessor
   * @param db_oid The database oid.
   */
  explicit QueryToOperatorTransformer(common::ManagedPointer<catalog::CatalogAccessor> catalog_accessor,
                                      catalog::db_oid_t db_oid);

  /**
   * Traverse the query AST to generate AST of logical operators.
   * @param op Parsed in AST tree of the SQL statement
   * @param parse_result Result generated by the parser. A collection of statements and expressions in the query.
   * @return Pointer to the logical operator AST
   */
  std::unique_ptr<AbstractOptimizerNode> ConvertToOpExpression(
      common::ManagedPointer<parser::SQLStatement> op, common::ManagedPointer<parser::ParseResult> parse_result);

  void Visit(common::ManagedPointer<parser::AnalyzeStatement> op) override;
  void Visit(common::ManagedPointer<parser::CopyStatement> op) override;
  void Visit(common::ManagedPointer<parser::CreateFunctionStatement> op) override;
  void Visit(common::ManagedPointer<parser::CreateStatement> op) override;
  void Visit(common::ManagedPointer<parser::DeleteStatement> op) override;
  void Visit(common::ManagedPointer<parser::DropStatement> op) override;
  void Visit(common::ManagedPointer<parser::ExecuteStatement> op) override;
  void Visit(common::ManagedPointer<parser::ExplainStatement> op) override;
  void Visit(common::ManagedPointer<parser::InsertStatement> op) override;
  void Visit(common::ManagedPointer<parser::PrepareStatement> op) override;
  void Visit(common::ManagedPointer<parser::SelectStatement> op) override;
  void Visit(common::ManagedPointer<parser::TransactionStatement> op) override;
  void Visit(common::ManagedPointer<parser::UpdateStatement> op) override;
  void Visit(common::ManagedPointer<parser::VariableSetStatement> op) override;

  void Visit(common::ManagedPointer<parser::ComparisonExpression> expr) override;
  void Visit(common::ManagedPointer<parser::OperatorExpression> expr) override;

  void Visit(common::ManagedPointer<parser::GroupByDescription> node) override;
  void Visit(common::ManagedPointer<parser::JoinDefinition> node) override;
  void Visit(common::ManagedPointer<parser::LimitDescription> node) override;
  void Visit(common::ManagedPointer<parser::OrderByDescription> node) override;
  void Visit(common::ManagedPointer<parser::TableRef> node) override;

 private:
  /**
   * Walk through an expression, split it into a set of predicates that could be joined by conjunction.
   * We need the set to perform predicate push-down. For example, for the following query
   * "SELECT test.a, test1.b FROM test, test1 WHERE test.a = test1.b and test.a = 5"
   * we could will extract "test.a = test1.b" and "test.a = 5" from the original predicate
   * and we could let "test.a = 5" to be evaluated at the table scan level
   *
   * @param expr The original predicate
   * @param parse_result Result generated by the parser. A collection of statements and expressions in the query.
   * @param predicates The extracted list of predicates
   */
  void CollectPredicates(common::ManagedPointer<parser::AbstractExpression> expr,
                         std::vector<AnnotatedExpression> *predicates);

  /**
   * Transform a sub-query in an expression to use
   * @param expr The potential sub-query expression
   * @param select_list The select list of the sub-query we generate
   * @param single_join A boolean to tell if it is a single join
   * @return If the expression could be transformed into sub-query, return true,
   *  return false otherwise
   */
  bool GenerateSubqueryTree(common::ManagedPointer<parser::AbstractExpression> expr, int child_id, bool single_join);

  /**
   * Decide if a conjunctive predicate is supported. We need to extract conjunction predicate first
   * then call this function to decide if the predicate is supported by our system
   * @param expr The conjunctive predicate provided
   * @return True if supported, false otherwise
   */
  static bool IsSupportedConjunctivePredicate(common::ManagedPointer<parser::AbstractExpression> expr);

  /**
   * Check if a sub-select statement is supported.
   * @param op The select statement
   * @return True if supported, false otherwise
   */
  static bool IsSupportedSubSelect(common::ManagedPointer<parser::SelectStatement> op);

  /**
   * Check if the select statement contains aggregation and/or groupby expressions
   * @param op The select statement
   * @return True if it has aggregate or groupby expression; false otherwise
   */
  static bool RequireAggregation(common::ManagedPointer<parser::SelectStatement> op);

  /**
   * Extract single table precates and multi-table predicates from the expr
   * @param expr The original predicate
   * @param annotated_predicates The extracted conjunction predicates
   */
  static void ExtractPredicates(common::ManagedPointer<parser::AbstractExpression> expr,
                                std::vector<AnnotatedExpression> *annotated_predicates);

  /**
   * Generate a set of table alias included in an expression
   * @param expr The original expression
   * @param table_alias_set The generated set of table alias
   */
  static void GenerateTableAliasSet(common::ManagedPointer<parser::AbstractExpression> expr,
                                    std::unordered_set<std::string> *table_alias_set);

  /**
   * Split conjunction expression tree into a vector of expressions with AND
   * @param expr The original expression
   * @param predicates The splited list of predicates
   */
  static void SplitPredicates(common::ManagedPointer<parser::AbstractExpression> expr,
                              std::vector<common::ManagedPointer<parser::AbstractExpression>> *predicates);

  /**
   * Generate a map of column alias to expression using select columns in the select statement
   * @param select_list Select columns in the select statement
   * @return table alias map
   */
  static std::unordered_map<parser::AliasType, common::ManagedPointer<parser::AbstractExpression>,
                            parser::AliasType::HashKey>
  ConstructSelectElementMap(const std::vector<common::ManagedPointer<parser::AbstractExpression>> &select_list);

  /**
   * Perform DFS over the expression tree to find leftmost LogicalCTEScanNode and
   * add the CTE query expression to the node.
   * @param child_expr The expression to search the CTE node
   * @return true if cte node was found
   */
  bool FindFirstCTEScanNode(common::ManagedPointer<AbstractOptimizerNode> child_expr,
                            const std::string &cte_table_name);

  /** The output logical operator AST */
  std::unique_ptr<OperatorNode> output_expr_;

  /** The parse result, because we end up creating new expressions that need to add to this. **/
  common::ManagedPointer<parser::ParseResult> parse_result_;

  /** The catalog accessor object */
  const common::ManagedPointer<catalog::CatalogAccessor> accessor_;
  const catalog::db_oid_t db_oid_;

  // TODO(tanujnay112) make all this a separate struct
  std::vector<std::string> cte_table_name_;
  std::vector<std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>> cte_expressions_;
  std::vector<parser::CTEType> cte_type_;
  std::vector<catalog::Schema> cte_schemas_;
  std::vector<catalog::table_oid_t> cte_oids_;

  /**
   * A set of predicates the current operator generated, we use them to generate filter operator
   */
  std::vector<AnnotatedExpression> predicates_;
};

}  // namespace optimizer
}  // namespace noisepage
