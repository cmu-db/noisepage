#pragma once

#include <vector>
#include "plan_node/abstract_plan_node.h"

namespace terrier {
namespace storage {
class SqlTable;
}
namespace parser {
class AnalyzeStatement;
}
namespace catalog {
class Schema;
}
namespace transaction {
class TransactionContext;
}

namespace plan_node {
/**
 * The plan node for ANALYZE
 */
class AnalyzePlanNode : public AbstractPlanNode {
 public:
  /**
   * Instantiate a new AnalyzePlanNode
   * @param target_table_ pointer to the target SQL table
   */
  explicit AnalyzePlanNode(std::shared_ptr<storage::SqlTable> target_table);

  /**
   * Instantiate a new AnalyzePlanNode
   * @param table_name name of the target table
   * @param schema_name name of the target table's schema
   * @param database_name name of the database
   * @param txn transaction context
   */
  explicit AnalyzePlanNode(std::string table_name, const std::string &schema_name, const std::string &database_name,
                           transaction::TransactionContext *txn);
  /**
   * @param table_name name of the target table
   * @param schema_name name of the target table's schema
   * @param database_name name of the database
   * @param column_names names of the columns of the target table
   * @param txn transaction context
   */
  explicit AnalyzePlanNode(std::string table_name, const std::string &schema_name, const std::string &database_name,
                           std::vector<std::string> &&column_names, transaction::TransactionContext *txn);
  /**
   * @param analyze_stmt the SQL ANALYZE statement
   * @param txn transaction context
   */
  explicit AnalyzePlanNode(parser::AnalyzeStatement *analyze_stmt, transaction::TransactionContext *txn);
  /**
   * @return the type of this plan node
   */
  inline PlanNodeType GetPlanNodeType() const override { return PlanNodeType::ANALYZE; }

  /**
   * @return the target table
   */
  inline std::shared_ptr<storage::SqlTable> GetTargetTable() const { return target_table_; }

  /**
   * @return the names of the columns to be analyzed
   */
  inline std::vector<std::string> GetColumnNames() const { return column_names_; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const override { return "AnalyzePlanNode"; }

  /**
   * @return pointer to a copy of abstract plan
   */
  inline std::unique_ptr<AbstractPlanNode> Copy() const override {
    return std::unique_ptr<AbstractPlanNode>(new AnalyzePlanNode(target_table_));
  }

 private:
  std::shared_ptr<storage::SqlTable> target_table_;  // pointer to the target table
  std::string table_name_;                           // name of the target table
  std::vector<std::string> column_names_;            // names of the columns to be analyzed
};

}  // namespace plan_node
}  // namespace terrier