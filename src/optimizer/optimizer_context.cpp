#include "optimizer/optimizer_context.h"

#include "optimizer/logical_operators.h"

namespace noisepage::optimizer {

GroupExpression *OptimizerContext::MakeGroupExpression(common::ManagedPointer<AbstractOptimizerNode> node) {
  std::vector<group_id_t> child_groups;
  for (auto &child : node->GetChildren()) {
    if (child->Contents()->GetOpType() == OpType::LEAF) {
      // Special case for LEAF
      const auto leaf = child->Contents()->GetContentsAs<LeafOperator>();
      auto child_group = leaf->GetOriginGroup();
      child_groups.push_back(child_group);
    } else {
      // Create a GroupExpression for the child
      auto gexpr = MakeGroupExpression(child);

      // Insert into the memo (this allows for duplicate detection)
      auto mexpr = memo_.InsertExpression(gexpr, false);
      if (mexpr == nullptr) {
        // Delete if need to (see InsertExpression spec)
        child_groups.push_back(gexpr->GetGroupID());
        delete gexpr;
      } else {
        child_groups.push_back(mexpr->GetGroupID());
      }
    }
  }
  return new GroupExpression(node->Contents(), std::move(child_groups));
}

}  // namespace noisepage::optimizer
