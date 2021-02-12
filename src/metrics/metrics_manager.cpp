#include "metrics/metrics_manager.h"

#include <sys/stat.h>

#include <algorithm>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "common/macros.h"

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

MetricsManager::MetricsManager() {
  // construct a bitset of all true (sampling rate 100) by default
  std::vector<bool> samples_mask(100, true);
  for (uint8_t i = 0; i < NUM_COMPONENTS; i++) {
    samples_mask_[i] = samples_mask;
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

void MetricsManager::SetMetricSampleRate(const MetricsComponent component, const uint8_t sample_rate) {
  NOISEPAGE_ASSERT(sample_rate >= 0 && sample_rate <= 100, "Invalid sampling rate.");
  // create a bitset with the correct number of trues based on sample rate
  std::vector<bool> samples_mask(100, false);
  for (uint8_t i = 0; i < sample_rate; i++) {
    samples_mask[i] = true;
  }
  // shuffle the bitset to distribute the samples
  auto rd = std::random_device{};
  auto rng = std::default_random_engine{rd()};
  std::shuffle(samples_mask.begin(), samples_mask.end(), rng);
  // replace the existing samples mask for this component in the global data structure
  NOISEPAGE_ASSERT(std::count(samples_mask.begin(), samples_mask.end(), true) == sample_rate,
                   "Vector should count number of trues equal to sample rate.");
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
  samples_mask_[static_cast<uint8_t>(component)] = samples_mask;
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
  auto result =
      stores_map_.emplace(thread_id, new MetricsStore(common::ManagedPointer(this), enabled_metrics_, samples_mask_));
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
