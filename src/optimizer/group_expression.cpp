#include "common/hash_util.h"
#include "optimizer/group_expression.h"
#include "optimizer/group.h"
#include "optimizer/rule.h"

namespace terrier {
namespace optimizer {

void GroupExpression::SetLocalHashTable(PropertySet* output_properties,
                                        std::vector<PropertySet*> input_properties_list,
                                        double cost) {
  auto it = lowest_cost_table_.find(output_properties);
  if (it == lowest_cost_table_.end()) {
    // No other cost to compare against
    lowest_cost_table_.insert(std::make_pair(output_properties, std::make_tuple(cost, input_properties_list)));
  } else {
    // Only insert if the cost is lower than the existing cost
    if (std::get<0>(it->second) > cost) {
      // Delete the existing vector there
      for (auto prop : std::get<1>(it->second)) { delete prop; }

      // Insert
      lowest_cost_table_[output_properties] = std::make_tuple(cost, input_properties_list);
      delete output_properties;
    } else {
      delete output_properties;
      for (auto prop : input_properties_list) { delete prop; }
    }
  }
}

common::hash_t GroupExpression::Hash() const {
  size_t hash = op.Hash();

  for (size_t i = 0; i < child_groups.size(); ++i) {
    size_t child_hash = common::HashUtil::Hash<GroupID>(child_groups[i]);
    hash = common::HashUtil::CombineHashes(hash, child_hash);
  }

  return hash;
}

}  // namespace optimizer
}  // namespace terrier
