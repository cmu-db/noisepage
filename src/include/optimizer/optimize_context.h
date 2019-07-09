#pragma once

#include <limits>

#include "optimizer/property_set.h"
#include "optimizer/optimizer_task.h"
#include "optimizer/optimizer_task_pool.h"

namespace terrier::optimizer {

class OptimizerMetadata;

/**
 * OptimizeContext containing information for each optimization.
 * A new OptimizeContext is created when optimizing sub-groups.
 */
class OptimizeContext {
 public:
  /**
   * Constructor
   * @param metadata OptimizerMetadata containing optimization metadata
   * @param required_prop Properties required to satisfy. acquires ownership
   * @param cost_upper_bound Upper cost bound
   */
  OptimizeContext(OptimizerMetadata *metadata,
                  PropertySet* required_prop,
                  double cost_upper_bound = std::numeric_limits<double>::max())
      : metadata_(metadata),
        required_prop_(required_prop),
        cost_upper_bound_(cost_upper_bound) {}

  /**
   * Destructor
   */
  ~OptimizeContext() {
    delete required_prop_;
  }

  /**
   * @returns OptimizerMetadata
   */
  OptimizerMetadata *GetMetadata() const { return metadata_; }

  /**
   * @returns Properties to satisfy, owned by this OptimizeContext
   */
  PropertySet *GetRequiredProperties() const { return required_prop_; }

  /**
   * @returns Current context's upper bound cost
   */
  double GetCostUpperBound() const { return cost_upper_bound_; }

  /**
   * Sets the context's upper bound cost
   * @param cost New cost upper bound
   */
  void SetCostUpperBound(double cost) {
    cost_upper_bound_ = cost;
  }

 private:
  OptimizerMetadata *metadata_;
  PropertySet* required_prop_;
  double cost_upper_bound_;
};

}  // namespace terrier::optimizer
