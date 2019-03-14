#include "plan_node/populate_index_plan_node.h"

namespace terrier::plan_node {
PopulateIndexPlanNode::PopulateIndexPlanNode(std::shared_ptr<storage::SqlTable> target_table,
                                             std::vector<catalog::col_oid_t> column_ids)
    : AbstractPlanNode(nullptr), target_table_(std::move(target_table)), column_ids_(std::move(column_ids)) {}
}  // namespace terrier::plan_node
