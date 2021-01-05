#pragma once

#include <limits>

#include "optimizer/optimizer_task.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/property_set.h"

namespace noisepage::optimizer {

class OptimizerContext;

/**
 * OptimizationContext containing information for each optimization.
 * A new OptimizationContext is created when optimizing sub-groups.
 */
class OptimizationContext {
 public:
  /**
   * Constructor
   * @param context OptimizerContext containing optimization context
   * @param required_props Properties required to satisfy. acquires ownership
   * @param optional_props Set of properties expressions in group can optionally attempt satisfying
   * @param cost_upper_bound Upper cost bound
   */
  OptimizationContext(OptimizerContext *context, PropertySet *required_props,
                      double cost_upper_bound = std::numeric_limits<double>::max())
      : context_(context),
        required_props_(required_props),
        cost_upper_bound_(cost_upper_bound) {}

  /**
   * Destructor
   */
  ~OptimizationContext() { delete required_props_; }

  /**
   * @returns OptimizerContext
   */
  OptimizerContext *GetOptimizerContext() const { return context_; }

  /**
   * @returns Properties to satisfy, owned by this OptimizationContext
   */
  PropertySet *GetRequiredProperties() const { return required_props_; }

  /**
   * @returns Current context's upper bound cost
   */
  double GetCostUpperBound() const { return cost_upper_bound_; }

  /**
   * Sets the context's upper bound cost
   * @param cost New cost upper bound
   */
  void SetCostUpperBound(double cost) { cost_upper_bound_ = cost; }

 private:
  /**
   * OptimizerContext
   */
  OptimizerContext *context_;

  /**
   * Required properties
   */
  PropertySet *required_props_;

  /**
   * Cost Upper Bound (for pruning)
   */
  double cost_upper_bound_;
};

}  // namespace noisepage::optimizer
