#include "metrics/metrics_manager.h"

#include <sys/stat.h>

#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace noisepage::metrics {

bool FileExists(const std::string &path) {
  struct stat buffer;
  return (stat(path.c_str(), &buffer) == 0);
}

template <typename abstract_raw_data>
void OpenFiles(std::vector<std::ofstream> *outfiles) {
  const auto num_files = abstract_raw_data::FILES.size();
  outfiles->reserve(num_files);
  for (size_t file = 0; file < num_files; file++) {
    const auto file_name = std::string(abstract_raw_data::FILES[file]);
    const auto file_existed = FileExists(file_name);
    outfiles->emplace_back(file_name, std::ios_base::out | std::ios_base::app);
    if (!file_existed) {
      // write the column titles on the first line since we're creating a new csv file
      if (!abstract_raw_data::FEATURE_COLUMNS[file].empty())
        outfiles->back() << abstract_raw_data::FEATURE_COLUMNS[file] << ", ";
      outfiles->back() << common::ResourceTracker::Metrics::COLUMNS << std::endl;
    }
  }
}

void MetricsManager::Aggregate() {
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
  for (const auto &metrics_store : stores_map_) {
    auto raw_data = metrics_store.second->GetDataToAggregate();

    for (uint8_t component = 0; component < NUM_COMPONENTS; component++) {
      if (enabled_metrics_.test(component)) {
        if (aggregated_metrics_[component] == nullptr)
          aggregated_metrics_[component] = std::move(raw_data[component]);
        else
          aggregated_metrics_[component]->Aggregate(raw_data[component].get());
      }
    }
  }
}

void MetricsManager::ResetMetric(const MetricsComponent component) const {
  for (const auto &metrics_store : stores_map_) {
    switch (static_cast<MetricsComponent>(component)) {
      case MetricsComponent::LOGGING: {
        const auto &metric = metrics_store.second->logging_metric_;
        metric->Swap();
        break;
      }
      case MetricsComponent::TRANSACTION: {
        const auto &metric = metrics_store.second->txn_metric_;
        metric->Swap();
        break;
      }
      case MetricsComponent::GARBAGECOLLECTION: {
        const auto &metric = metrics_store.second->gc_metric_;
        metric->Swap();
        break;
      }
      case MetricsComponent::EXECUTION: {
        const auto &metric = metrics_store.second->execution_metric_;
        metric->Swap();
        break;
      }
      case MetricsComponent::EXECUTION_PIPELINE: {
        const auto &metric = metrics_store.second->pipeline_metric_;
        metric->Swap();
        break;
      }
      case MetricsComponent::BIND_COMMAND: {
        const auto &metric = metrics_store.second->bind_command_metric_;
        metric->Swap();
        break;
      }
      case MetricsComponent::EXECUTE_COMMAND: {
        const auto &metric = metrics_store.second->execute_command_metric_;
        metric->Swap();
        break;
      }
      case MetricsComponent::QUERY_TRACE: {
        const auto &metric = metrics_store.second->query_trace_metric_;
        metric->Swap();
        break;
      }
    }
  }
}

void MetricsManager::RegisterThread() {
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
  const auto thread_id = std::this_thread::get_id();
  NOISEPAGE_ASSERT(stores_map_.count(thread_id) == 0, "This thread was already registered.");
  auto result = stores_map_.emplace(thread_id,
                                    new MetricsStore(common::ManagedPointer(this), enabled_metrics_, sample_interval_));
  NOISEPAGE_ASSERT(result.second, "Insertion to concurrent map failed.");
  common::thread_context.metrics_store_ = result.first->second;
}

/**
 * Should be called by the thread when it is guaranteed to no longer be collecting any more metrics, otherwise,
 * segfault could happen when the unique_ptr releases the MetricsStore
 */
void MetricsManager::UnregisterThread() {
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
  const auto thread_id = std::this_thread::get_id();
  stores_map_.erase(thread_id);
  NOISEPAGE_ASSERT(stores_map_.count(thread_id) == 0, "Deletion from concurrent map failed.");
  common::thread_context.metrics_store_ = nullptr;
}

void MetricsManager::ToCSV() const {
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
  for (uint8_t component = 0; component < NUM_COMPONENTS; component++) {
    if (enabled_metrics_.test(component) && aggregated_metrics_[component] != nullptr) {
      std::vector<std::ofstream> outfiles;
      switch (static_cast<MetricsComponent>(component)) {
        case MetricsComponent::LOGGING: {
          OpenFiles<LoggingMetricRawData>(&outfiles);
          break;
        }
        case MetricsComponent::TRANSACTION: {
          OpenFiles<TransactionMetricRawData>(&outfiles);
          break;
        }
        case MetricsComponent::GARBAGECOLLECTION: {
          OpenFiles<GarbageCollectionMetricRawData>(&outfiles);
          break;
        }
        case MetricsComponent::EXECUTION: {
          OpenFiles<ExecutionMetricRawData>(&outfiles);
          break;
        }
        case MetricsComponent::EXECUTION_PIPELINE: {
          OpenFiles<PipelineMetricRawData>(&outfiles);
          break;
        }
        case MetricsComponent::BIND_COMMAND: {
          OpenFiles<BindCommandMetricRawData>(&outfiles);
          break;
        }
        case MetricsComponent::EXECUTE_COMMAND: {
          OpenFiles<ExecuteCommandMetricRawData>(&outfiles);
          break;
        }
        case MetricsComponent::QUERY_TRACE: {
          OpenFiles<QueryTraceMetricRawData>(&outfiles);
          break;
        }
      }
      aggregated_metrics_[component]->ToCSV(&outfiles);
      for (auto &file : outfiles) {
        file.close();
      }
    }
  }
}

}  // namespace noisepage::metrics
