#pragma once

#include <memory>
#include <string>
#include "plan_node/abstract_plan_node.h"
#include "transaction/transaction_context.h"

namespace terrier {
namespace storage {
class SqlTable;
}
namespace parser {
class DropStatement;
}
namespace catalog {
class Schema;
}

namespace plan_node {
/**
 *  The plan node for DROP
 */
class DropPlanNode : public AbstractPlanNode {
 public:
  DropPlanNode() = delete;

  /**
   * Instantiate a DropPlanNode
   * @param table_name name of the table to be dropped
   */
  explicit DropPlanNode(std::string table_name);

  /**
   * Instantiate a DropPlanNode
   * @param drop_stmt the drop statement from parser
   */
  explicit DropPlanNode(parser::DropStatement *drop_stmt);

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DROP; }

  /**
   * @return database name [DROP DATABASE]
   */
  std::string GetDatabaseName() const { return database_name_; }

  /**
   * @return table name for [DROP TABLE/TRIGGER]
   */
  std::string GetTableName() const { return table_name_; }

  /**
   * @return schema name for [DROP SCHEMA]
   */
  std::string GetSchemaName() const { return schema_name_; }

  /**
   * @return trigger name for [DROP TRIGGER]
   */
  std::string GetTriggerName() const { return trigger_name_; }

  /**
   * @return index name for [DROP INDEX]
   */
  std::string GetIndexName() const { return index_name_; }

  /**
   * @return drop type
   */
  DropType GetDropType() const { return drop_type_; }

  /**
   * @return true if "IF EXISTS" was used for [DROP DATABASE, DROP SCHEMA]
   */
  bool IsIfExists() const { return if_exists_; }

  DISALLOW_COPY_AND_MOVE(DropPlanNode);

 private:
  DropType drop_type_ = DropType::TABLE;

  // Target Table
  std::string table_name_;

  // Database Name
  std::string database_name_;

  // Schema Name
  std::string schema_name_;

  // Trigger Name
  std::string trigger_name_;

  // Index Name
  std::string index_name_;

  bool if_exists_;
};

}  // namespace plan_node
}  // namespace terrier
