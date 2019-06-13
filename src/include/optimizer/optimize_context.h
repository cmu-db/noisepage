#pragma once

#include "optimizer/property_set.h"
#include "optimizer/optimizer_task.h"
#include "optimizer/optimizer_task_pool.h"

namespace terrier {
namespace optimizer {

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
   * @param required_prop Properties required to satisfy
   * @param cost_upper_bound Upper cost bound
   */
  OptimizeContext(OptimizerMetadata *metadata,
                  PropertySet* required_prop,
                  double cost_upper_bound = std::numeric_limits<double>::max())
      : metadata(metadata),
        required_prop(required_prop),
        cost_upper_bound(cost_upper_bound) {}

  OptimizerMetadata *metadata;
  PropertySet* required_prop;
  double cost_upper_bound;
};

}  // namespace optimizer
}  // namespace terrier
