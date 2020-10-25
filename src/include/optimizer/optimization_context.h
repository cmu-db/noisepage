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
   * @param required_prop Properties required to satisfy. acquires ownership
   * @param cost_upper_bound Upper cost bound
   * @param optional_props Set of properties expressions in group can optionally attempt satisfying
   */
  OptimizationContext(OptimizerContext *context, PropertySet *required_prop,
                      double cost_upper_bound = std::numeric_limits<double>::max(),
                      PropertySet *optional_props = new PropertySet())
      : context_(context),
        required_prop_(required_prop),
        cost_upper_bound_(cost_upper_bound),
        optional_props_(optional_props) {}

  /**
   * Destructor
   */
  ~OptimizationContext() {
    delete required_prop_;
    delete optional_props_;
  }

  /**
   * @returns OptimizerContext
   */
  OptimizerContext *GetOptimizerContext() const { return context_; }

  /**
   * @returns Properties to satisfy, owned by this OptimizationContext
   */
  PropertySet *GetRequiredProperties() const { return required_prop_; }

  /**
   * @returns Properties to attempt, owned by this OptimizationContext
   */
  PropertySet *GetOptionalProperties() const { return optional_props_; }

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
  PropertySet *required_prop_;

  /**
   * Cost Upper Bound (for pruning)
   */
  double cost_upper_bound_;

  /**
   * Required properties
   */
  PropertySet *optional_props_;
};

}  // namespace noisepage::optimizer
