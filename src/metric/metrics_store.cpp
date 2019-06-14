#include "metric/metrics_store.h"
#include <memory>
#include <vector>
#include "metric/metric_defs.h"
#include "metric/transaction_metric.h"

namespace terrier::metric {

MetricsStore::MetricsStore() {
  RegisterMetric<TransactionMetric>({MetricsEventType::TXN_BEGIN, MetricsEventType::TXN_COMMIT,
                                     MetricsEventType::TXN_ABORT, MetricsEventType::TUPLE_READ,
                                     MetricsEventType::TUPLE_UPDATE, MetricsEventType::TUPLE_INSERT,
                                     MetricsEventType::TUPLE_DELETE});
}

std::vector<std::unique_ptr<AbstractRawData>> MetricsStore::GetDataToAggregate() {
  std::vector<std::unique_ptr<AbstractRawData>> result;
  for (auto &metric : metrics_) result.emplace_back(metric->Swap());
  return result;
}
}  // namespace terrier::metric
