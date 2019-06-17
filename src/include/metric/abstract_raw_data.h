#pragma once

#include "common/macros.h"
#include "metric/metric_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::metric {
/**
 * @brief An always-consistent storage unit for intermediate stats results.
 *
 * These objects hold raw data points processed by a metric on the thread-local
 * level. Entries into this object must be always consistent.
 * (i.e. future entries should not rely on some early entries being in this
 * object)
 * This is because an aggregator can come at any time and swap out
 * the object for aggregation.
 *
 * @see Metric for detailed description of how this would work.
 */
class AbstractRawData {
 public:
  virtual ~AbstractRawData() = default;

  /**
   * Given another AbstractRawData classes, combine the other's content with the
   * content of this one. It is guaranteed that nobody will have access to the
   * other object at this point or after.
   * @param other The other AbstractRawData to be merged
   */
  virtual void Aggregate(AbstractRawData *other) = 0;

  /**
   * @return the type of the metric this object is holding the data for
   */
  virtual MetricsComponent GetMetricType() const = 0;
};
}  // namespace terrier::metric
