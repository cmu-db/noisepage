#include "plan_node/analyze_plan_node.h"
#include "catalog/catalog_defs.h"
#include "parser/analyze_statement.h"
#include "storage/data_table.h"

namespace terrier::plan_node {

// TODO(Gus,Wen): Do catalog lookups once catalog is available

AnalyzePlanNode::AnalyzePlanNode(std::shared_ptr<storage::SqlTable> target_table)
    : AbstractPlanNode(nullptr), target_table_(std::move(target_table)) {}

AnalyzePlanNode::AnalyzePlanNode(std::string table_name, const std::string &schema_name,
                                 const std::string &database_name, transaction::TransactionContext *txn)
    : AbstractPlanNode(nullptr), table_name_(std::move(table_name)) {
  /*target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(txn,
                                                                    database_name,
                                                                    schema_name,
                                                                    table_name);
  */
}

AnalyzePlanNode::AnalyzePlanNode(std::string table_name, const std::string &schema_name,
                                 const std::string &database_name, std::vector<std::string> &&column_names,
                                 transaction::TransactionContext *txn)
    : AbstractPlanNode(nullptr), table_name_(std::move(table_name)), column_names_(std::move(column_names)) {
  /*target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(txn,
                                                                    database_name,
                                                                    schema_name,
                                                                    table_name);
  */
}

AnalyzePlanNode::AnalyzePlanNode(parser::AnalyzeStatement *analyze_stmt, transaction::TransactionContext *txn)
    : AbstractPlanNode(nullptr) {
  table_name_ = analyze_stmt->GetAnalyzeTable()->GetTableName();
  column_names_ = *analyze_stmt->GetAnalyzeColumns();

  /*if (!table_name_.empty()) {
    target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(txn,
                                                                      analyze_stmt->GetDatabaseName(),
                                                                      analyze_stmt->GetSchemaName(),
                                                                      table_name_);
  */
}
}  // namespace terrier::plan_node