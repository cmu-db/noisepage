#pragma once

#include <memory>
#include <vector>

#include "optimizer/abstract_optimizer.h"
#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/optimizer_metadata.h"
#include "optimizer/property_set.h"

namespace terrier {

namespace planner {
class AbstractPlanNode;
}  // namespace planner

namespace catalog {
class CatalogAccessor;
}

namespace transaction {
class TransactionContext;
}  // namespace transaction

namespace optimizer {

class OperatorExpression;

/**
 * Optimizer class that implements the AbstractOptimizer abstract class
 */
class Optimizer : public AbstractOptimizer {
 public:
  /**
   * Disallow copy and move
   */
  DISALLOW_COPY_AND_MOVE(Optimizer);

  /**
   * Constructor for Optimizer with a cost_model
   * @param model Cost Model to use for the optimizer
   */
  explicit Optimizer(AbstractCostModel *model) {
    metadata_ = new OptimizerMetadata(model);
    cost_model_ = model;
  }

  /**
   * Destructor
   */
  ~Optimizer() override {
    delete cost_model_;
    delete metadata_;
  }

  /**
   * Build the plan tree for query execution
   * @param op_tree Logical operator tree for execution
   * @param query_info Information about the query
   * @param txn TransactionContext
   * @param settings SettingsManager to read settings from
   * @param accessor CatalogAccessor for catalog
   * @param storage StatsStorage
   * @returns execution plan
   */
  std::unique_ptr<planner::AbstractPlanNode> BuildPlanTree(OperatorExpression *op_tree, QueryInfo query_info,
                                                           transaction::TransactionContext *txn,
                                                           settings::SettingsManager *settings,
                                                           catalog::CatalogAccessor *accessor,
                                                           StatsStorage *storage) override;

  /**
   * Invoke a single optimization pass through the entire query.
   * The optimization pass includes rewriting and optimization logic.
   * @param root_group_id Group to begin optimization at
   * @param required_props Physical properties to enforce
   * @param settings SettingsManager to read settings from
   */
  void OptimizeLoop(int root_group_id, PropertySet *required_props, settings::SettingsManager *settings);

  /**
   * Reset the optimizer state
   */
  void Reset() override;

  /**
   * Gets the OptimizerMetadata used and set by the optimizer
   * @returns metadata_
   */
  OptimizerMetadata *GetMetadata() { return metadata_; }

 private:
  /**
   * Retrieve the lowest cost execution plan with the given properties
   *
   * @param id ID of the group to produce the best physical operator
   * @param requirements Set of properties produced operator tree must satisfy
   * @param required_cols AbstractExpression tree output columns group must generate
   * @param settings SettingsManager
   * @param accessor CatalogAccessor
   * @param txn TransactionContext
   * @returns Lowest cost plan
   */
  std::unique_ptr<planner::AbstractPlanNode> ChooseBestPlan(
      GroupID id, PropertySet *required_props,
      const std::vector<common::ManagedPointer<parser::AbstractExpression>> &required_cols,
      settings::SettingsManager *settings, catalog::CatalogAccessor *accessor, transaction::TransactionContext *txn);

  /**
   * Execute elements of given optimization task stack and ensure that we
   * do not go beyond the time limit (unless if one plan has not been
   * generated yet)
   *
   * @param task_stack Optimizer's Task Stack to execute through
   * @param root_group_id Root Group ID to check whether there is a plan or not
   * @param root_context OptimizerContext to use that maintains required properties
   * @param settings SettingsManager to read settings from
   */
  void ExecuteTaskStack(OptimizerTaskStack *task_stack, int root_group_id, OptimizeContext *root_context,
                        settings::SettingsManager *settings);

  /**
   * Metadata
   */
  OptimizerMetadata *metadata_;

  /**
   * Cost Model
   */
  AbstractCostModel *cost_model_;
};

}  // namespace optimizer
}  // namespace terrier
