#pragma once

#include <memory>
#include <string>
#include <vector>
#include "plan_node/abstract_plan_node.h"

namespace terrier {
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
   * @param target_table_oid the OID of the target SQL table
   */
  explicit AnalyzePlanNode(catalog::table_oid_t target_table_oid);

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
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::ANALYZE; }

  /**
   * @return the OID of the target table
   */
  catalog::table_oid_t GetTargetTable() const { return target_table_oid_; }

  /**
   * @return the names of the columns to be analyzed
   */
  std::vector<std::string> GetColumnNames() const { return column_names_; }

 private:
  catalog::table_oid_t target_table_oid_;  // OID of the target table
  std::string table_name_;                 // name of the target table
  std::vector<std::string> column_names_;  // names of the columns to be analyzed
};

}  // namespace plan_node
}  // namespace terrier
