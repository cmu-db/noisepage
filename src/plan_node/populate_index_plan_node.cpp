#include "plan_node/populate_index_plan_node.h"
#include <memory>
#include <utility>
#include <vector>

namespace terrier::plan_node {
PopulateIndexPlanNode::PopulateIndexPlanNode(catalog::table_oid_t target_table_oid,
                                             std::vector<catalog::col_oid_t> column_ids)
    : AbstractPlanNode(nullptr), target_table_oid_(target_table_oid), column_ids_(std::move(column_ids)) {}
}  // namespace terrier::plan_node
