#pragma once

#include <utility>
#include <vector>

#include "common/settings.h"
#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/group_expression.h"
#include "optimizer/memo.h"
#include "optimizer/rule.h"
#include "optimizer/statistics/stats_storage.h"

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

/**
 * OptimizerContext is a class containing pointers to various objects
 * that are required during the entire query optimization process.
 */
class OptimizerContext {
 public:
  /**
   * Constructor for OptimizerContext
   * @param cost_model Cost Model to be stored by OptimizerContext
   */
  explicit OptimizerContext(AbstractCostModel *cost_model) : cost_model_(cost_model), task_pool_(nullptr) {}

  /**
   * Destructor
   *
   * Destroys cost_model and task_pool
   */
  ~OptimizerContext() {
    delete task_pool_;

    for (auto *ctx : track_list_) {
      delete ctx;
    }
  }

  /**
   * Gets the Memo
   * @returns Memo
   */
  Memo &GetMemo() { return memo_; }

  /**
   * Gets the RuleSet
   * @returns RuleSet
   */
  RuleSet &GetRuleSet() { return rule_set_; }

  /**
   * Gets the CatalogAccessor
   * @returns CatalogAccessor
   */
  catalog::CatalogAccessor *GetCatalogAccessor() { return accessor_; }

  /**
   * Gets the StatsStorage
   * @returns StatsStorage
   */
  StatsStorage *GetStatsStorage() { return stats_storage_; }

  /**
   * Adds a OptimizationContext to the tracking list
   * @param ctx OptimizationContext to add to tracking
   */
  void AddOptimizationContext(OptimizationContext *ctx) { track_list_.push_back(ctx); }

  /**
   * Pushes a task to the task pool managed
   * @param task Task to push
   */
  void PushTask(OptimizerTask *task) { task_pool_->Push(task); }

  /**
   * Gets the cost model
   * @returns Cost Model
   */
  AbstractCostModel *GetCostModel() { return cost_model_; }

  /**
   * Gets the transaction
   * @returns transaction
   */
  transaction::TransactionContext *GetTxn() { return txn_; }

  /**
   * Sets the transaction
   * @param txn TransactionContext
   */
  void SetTxn(transaction::TransactionContext *txn) { txn_ = txn; }

  /**
   * Sets the CatalogAccessor
   * @param accessor CatalogAccessor
   */
  void SetCatalogAccessor(catalog::CatalogAccessor *accessor) { accessor_ = accessor; }

  /**
   * Sets the StatsStorage
   * @param storage StatsStorage
   */
  void SetStatsStorage(StatsStorage *storage) { stats_storage_ = storage; }

  /**
   * Set the task pool tracked by the OptimizerContext.
   * Function passes ownership over task_pool
   * @param task_pool Pointer to OptimizerTaskPool
   */
  void SetTaskPool(OptimizerTaskPool *task_pool) {
    delete this->task_pool_;
    this->task_pool_ = task_pool;
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
  GroupExpression *MakeGroupExpression(common::ManagedPointer<OperatorExpression> expr) {
    std::vector<group_id_t> child_groups;
    for (auto &child : expr->GetChildren()) {
      // Create a GroupExpression for the child
      auto gexpr = MakeGroupExpression(child);

      // Insert into the memo (this allows for duplicate detection)
      auto expr = memo_.InsertExpression(gexpr, false);
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
   * @param expr OperatorExpression to record
   * @param gexpr Places the newly created GroupExpression
   * @returns Whether the OperatorExpression already exists
   */
  bool RecordTransformedExpression(common::ManagedPointer<OperatorExpression> expr, GroupExpression **gexpr) {
    return RecordTransformedExpression(expr, gexpr, UNDEFINED_GROUP);
  }

  /**
   * Adds a transformed OperatorExpression into a given group.
   * A group contains all logical/physically equivalent OperatorExpressions.
   * This function is invoked by tasks which explore the plan search space
   * through rules (recording all "equivalent" expressions for cost/selection later).
   *
   * @param expr OperatorExpression to record into the group
   * @param gexpr Places the newly created GroupExpression
   * @param target_group ID of the Group that the OperatorExpression belongs to
   * @returns Whether the OperatorExpression already exists
   */
  bool RecordTransformedExpression(common::ManagedPointer<OperatorExpression> expr, GroupExpression **gexpr,
                                   group_id_t target_group) {
    auto new_gexpr = MakeGroupExpression(expr);
    auto ptr = memo_.InsertExpression(new_gexpr, target_group, false);
    TERRIER_ASSERT(ptr, "Root of expr should not fail insertion");

    (*gexpr) = ptr;
    return (ptr == new_gexpr);
  }

  /**
   * Replaces the OperatorExpression in a given group.
   * This is used primarily for the rewrite stage of the Optimizer
   * (i.e predicate push-down, query unnesting)
   *
   * @param expr OperatorExpression to store into the group
   * @param target_group ID of the Group to replace
   */
  void ReplaceRewriteExpression(common::ManagedPointer<OperatorExpression> expr, group_id_t target_group) {
    memo_.EraseExpression(target_group);
    UNUSED_ATTRIBUTE auto ret = memo_.InsertExpression(MakeGroupExpression(expr), target_group, false);
    TERRIER_ASSERT(ret, "Root expr should always be inserted");
  }

 private:
  /**
   * Memo for memoization and group tracking
   */
  Memo memo_;

  /**
   * RuleSet used for particular optimization pass
   */
  RuleSet rule_set_;

  /**
   * Cost Model pointer
   */
  AbstractCostModel *cost_model_;

  /**
   * Pool of Optimizer tasks to execute
   */
  OptimizerTaskPool *task_pool_;

  /**
   * CatalogAccessor
   */
  catalog::CatalogAccessor *accessor_;

  /**
   * StatsStorage
   */
  StatsStorage *stats_storage_;

  /**
   * TransactionContxt used for execution
   */
  transaction::TransactionContext *txn_;

  /**
   * List to track OptimizationContext created
   */
  std::vector<OptimizationContext *> track_list_;
};

}  // namespace optimizer
}  // namespace terrier
