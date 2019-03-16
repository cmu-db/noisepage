#include "plan_node/insert_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/constant_value_expression.h"
#include "storage/sql_table.h"
#include "type/transient_value_factory.h"

namespace terrier::plan_node {

InsertPlanNode::InsertPlanNode(std::shared_ptr<storage::SqlTable> target_table, const std::vector<std::string> &columns,
                               std::vector<std::vector<std::unique_ptr<parser::AbstractExpression>>> &&insert_values)
    : target_table_(std::move(target_table)), bulk_insert_count_(static_cast<uint32_t>(insert_values.size())) {
  // TODO(Gus,Wen) Table Schema have been reworked, need to rewrite this part
}

bool InsertPlanNode::FindSchemaColIndex(const std::string &col_name,
                                        const std::vector<catalog::Schema::Column> &tbl_columns, uint32_t *index) {
  for (auto tcol = tbl_columns.begin(); tcol != tbl_columns.end(); tcol++) {
    if (tcol->GetName() == col_name) {
      *index = static_cast<uint32_t>(std::distance(tbl_columns.begin(), tcol));
      return true;
    }
  }
  return false;
}  // namespace terrier::plan_node

void InsertPlanNode::ProcessColumnSpec(const std::vector<std::string> &columns) {
  // TODO(Gus,Wen) Table Schema have been reworked, need to rewrite this part
}

bool InsertPlanNode::ProcessValueExpr(parser::AbstractExpression *expr, uint32_t schema_idx) {
  // TODO(Gus,Wen) Table Schema have been reworked, need to rewrite this part
  return false;
}

void InsertPlanNode::SetDefaultValue(uint32_t idx) {
  // TODO(Gus,Wen) Table Schema and default values have been reworked, need to rewrite this part
}

common::hash_t InsertPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // TODO(Gus,Wen) Add hash for target table

  if (GetChildren().empty()) {
    auto bulk_insert_count = GetBulkInsertCount();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&bulk_insert_count));
  }

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool InsertPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = static_cast<const plan_node::InsertPlanNode &>(rhs);

  // TODO(Gus, Wen) Compare target tables

  if (GetChildren().empty()) {
    if (!other.GetChildren().empty()) return false;

    if (GetBulkInsertCount() != other.GetBulkInsertCount()) return false;
  }

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
