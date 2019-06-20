#pragma once

#include "common/settings.h"
#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/memo.h"
#include "optimizer/group_expression.h"
#include "optimizer/rule.h"

namespace terrier {

namespace transaction {
class TransactionContext;
}

namespace catalog {
class CatalogAccessor;
}

namespace optimizer {

class OptimizerTaskPool;
class RuleSet;

class OptimizerMetadata {
 public:

  /**
   * Constructor for OptimizerMetadata
   * @param cost_model Cost Model to be stored by OptimizerMetadata
   */
  explicit OptimizerMetadata(AbstractCostModel *cost_model)
      : cost_model(cost_model),
        task_pool(nullptr) {}

  /**
   * Destructor
   *
   * Destroys cost_model and task_pool
   */
  ~OptimizerMetadata() {
    delete cost_model;
    delete task_pool;

    for (auto *ctx : track_list) { delete ctx; }
  }

  /**
   * Memo for memoization and group tracking
   */
  Memo memo;

  /**
   * RuleSet used for particular optimization pass
   */
  RuleSet rule_set;

  /**
   * Cost Model pointer
   */
  AbstractCostModel* cost_model;

  /**
   * Pool of Optimizer tasks to execute
   */
  OptimizerTaskPool *task_pool;

  /**
   * CatalogAccessor
   */
  catalog::CatalogAccessor *accessor;

  /**
   * TransactionContxt used for execution
   */
  transaction::TransactionContext* txn;

  /**
   * List to track OptimizeContext created
   * TODO(wz2): narrow object lifecycle to parent task
   */
  std::vector<OptimizeContext*> track_list;

  /**
   * Set the task pool tracked by the OptimizerMetadata.
   * Function passes ownership over task_pool
   * @param task_pool Pointer to OptimizerTaskPool
   */
  void SetTaskPool(OptimizerTaskPool *task_pool) {
    if (this->task_pool) delete this->task_pool;
    this->task_pool = task_pool;
  }

  /**
   * Converts an OperatorExpression into a GroupExpression.
   * The GroupExpression is internal tracking that is focused on the concept
   * of groups rather than operators like OperatorExpression.
   *
   * Subtrees of the OperatorExpression are individually converted to
   * GroupExpressions and inserted into Memo, which allows for duplicate
   * detection. The root GroupExpression, however, is not automatically
   * inserted into Memo.
   *
   * @param expr OperatorExpression to convert
   * @returns GroupExpression representing OperatorExpression
   */
  GroupExpression* MakeGroupExpression(OperatorExpression* expr) {
    std::vector<GroupID> child_groups;
    for (auto &child : expr->GetChildren()) {
      // Create a GroupExpression for the child
      auto gexpr = MakeGroupExpression(child);

      // Insert into the memo (this allows for duplicate detection)
      auto expr = memo.InsertExpression(gexpr, false);
      if (expr == nullptr) {
        // Delete if need to (see InsertExpression spec)
        child_groups.push_back(gexpr->GetGroupID());
        delete gexpr;
      } else {
        child_groups.push_back(expr->GetGroupID());
      }
    }

    return new GroupExpression(expr->GetOp(), std::move(child_groups));
  }

  /**
   * Records a transformed OperatorExpression by creating a new group.
   * A new group will be created if expr does not already exist.
   *
   * expr is not freed
   *
   * @param expr OperatorExpression to record
   * @param gexpr[out] Places the newly created GroupExpression
   * @returns Whether the OperatorExpression already exists
   */
  bool RecordTransformedExpression(OperatorExpression* expr, GroupExpression* &gexpr) {
    return RecordTransformedExpression(expr, gexpr, UNDEFINED_GROUP);
  }

  /**
   * Records a transformed OperatorExpression into a given group.
   * A group contains all logical/physically equivalent OperatorExpressions.
   *
   * expr is not freed
   *
   * @param expr OperatorExpression to record into the group
   * @param gexpr[out] Places the newly created GroupExpression
   * @param target_group ID of the Group that the OperatorExpression belongs to
   * @returns Whether the OperatorExpression already exists
   */
  bool RecordTransformedExpression(OperatorExpression* expr, GroupExpression* &gexpr, GroupID target_group) {
    auto new_gexpr = MakeGroupExpression(expr);
    auto ptr = memo.InsertExpression(new_gexpr, target_group, false);
    TERRIER_ASSERT(ptr, "Root of expr should not fail insertion");

    gexpr = ptr; // ptr is actually usable
    return (ptr == new_gexpr);
  }

  /**
   * Replaces the OperatorExpression in a given group.
   * This is used primarily for the rewrite stage of the Optimizer
   * (i.e predicate push-down, query unnesting)
   *
   * expr is not freed
   *
   * @param expr OperatorExpression to store into the group
   * @param target_group ID of the Group to replace
   */
  void ReplaceRewritedExpression(OperatorExpression* expr, GroupID target_group) {
    memo.EraseExpression(target_group);
    auto ret = memo.InsertExpression(MakeGroupExpression(expr), target_group, false);
    TERRIER_ASSERT(ret, "Root expr should always be inserted");
  }
};

}  // namespace optimizer
}  // namespace terrier
