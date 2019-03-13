#include "plan_node/drop_plan_node.h"
#include "parser/drop_statement.h"
#include "storage/data_table.h"

namespace terrier::plan_node {

DropPlanNode::DropPlanNode(std::string table_name) {
  table_name_ = std::move(table_name);
  if_exists_ = false;
}

DropPlanNode::DropPlanNode(parser::DropStatement *drop_stmt) {
  switch (drop_stmt->GetDropType()) {
    case parser::DropStatement::DropType::kDatabase:
      database_name_ = drop_stmt->GetDatabaseName();
      if_exists_ = drop_stmt->IsIfExists();
      drop_type_ = DropType::DB;
      break;
    case parser::DropStatement::DropType::kSchema:
      database_name_ = drop_stmt->GetDatabaseName();
      schema_name_ = drop_stmt->GetSchemaName();
      if_exists_ = drop_stmt->IsIfExists();
      drop_type_ = DropType::SCHEMA;
      break;
    case parser::DropStatement::DropType::kTable:
      database_name_ = drop_stmt->GetDatabaseName();
      schema_name_ = drop_stmt->GetSchemaName();
      table_name_ = drop_stmt->GetTableName();
      if_exists_ = drop_stmt->IsIfExists();
      drop_type_ = DropType::TABLE;
      break;
    case parser::DropStatement::DropType::kTrigger:
      // TODO(Gus,Wen) it used to be in Peloton that drop statement can have two
      // different table names, but now drop statement only has one, need to
      // check correctness
      database_name_ = drop_stmt->GetDatabaseName();
      schema_name_ = drop_stmt->GetSchemaName();
      table_name_ = drop_stmt->GetTableName();
      trigger_name_ = drop_stmt->GetTriggerName();
      drop_type_ = DropType::TRIGGER;
      break;
    case parser::DropStatement::DropType::kIndex:
      database_name_ = drop_stmt->GetDatabaseName();
      schema_name_ = drop_stmt->GetSchemaName();
      index_name_ = drop_stmt->GetIndexName();
      drop_type_ = DropType::INDEX;
      break;
    default:
      break;
  }
}

}  // namespace terrier::plan_node