#include <string>
#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/data_table_benchmark_util.h"
#include "main/db_main.h"

namespace terrier {

class LargeTransactionMetricsBenchmark : public benchmark::Fixture {
 public:
  const std::vector<uint16_t> attr_sizes_ = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8};
  const uint32_t initial_table_size_ = 1e6;
  const uint32_t num_txns_ = 1e5;
  std::default_random_engine generator_;
  const uint32_t num_concurrent_txns_ = 4;
};

/**
 * Run a TPCC-like workload (5 statements per txn, 10% insert, 40% update, 50% select).
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LargeTransactionMetricsBenchmark, TPCCish)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 5;
  const std::vector<double> insert_update_select_ratio = {0.1, 0.4, 0.5};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (const auto &file : metrics::TransactionMetricRawData::FILES) unlink(std::string(file).c_str());

    auto db_main = DBMain::Builder()
                       .SetUseGC(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .SetUseMetrics(true)
                       .Build();
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::TRANSACTION);

    auto *const tested = new LargeDataTableBenchmarkObject(
        attr_sizes_, initial_table_size_, txn_length, insert_update_select_ratio,
        db_main->GetStorageLayer()->GetBlockStore().Get(), db_main->GetBufferSegmentPool().Get(), &generator_, true);
    const auto result = tested->SimulateOltp(num_txns_, num_concurrent_txns_, db_main->GetMetricsManager().Get());
    abort_count += result.first;
    state.SetIterationTime(static_cast<double>(result.second) / 1000.0);
    db_main->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete tested; });
  }
  state.SetItemsProcessed(state.iterations() * num_txns_ - abort_count);
}

/**
 * Run a high number of statements with lots of updates to try to trigger aborts.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LargeTransactionMetricsBenchmark, HighAbortRate)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 40;
  const std::vector<double> insert_update_select_ratio = {0.0, 0.8, 0.2};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (const auto &file : metrics::TransactionMetricRawData::FILES) unlink(std::string(file).c_str());

    auto db_main = DBMain::Builder()
                       .SetUseGC(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .SetUseMetrics(true)
                       .Build();
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::TRANSACTION);

    // use a smaller table to make aborts more likely
    auto *const tested = new LargeDataTableBenchmarkObject(attr_sizes_, 1000, txn_length, insert_update_select_ratio,
                                                           db_main->GetStorageLayer()->GetBlockStore().Get(),
                                                           db_main->GetBufferSegmentPool().Get(), &generator_, true);
    const auto result = tested->SimulateOltp(num_txns_, num_concurrent_txns_, db_main->GetMetricsManager().Get());
    abort_count += result.first;
    state.SetIterationTime(static_cast<double>(result.second) / 1000.0);
    db_main->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete tested; });
  }
  state.SetItemsProcessed(state.iterations() * num_txns_ - abort_count);
}

/**
 * Single statement insert throughput. Should have no aborts.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LargeTransactionMetricsBenchmark, SingleStatementInsert)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 1;
  const std::vector<double> insert_update_select_ratio = {1, 0, 0};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (const auto &file : metrics::TransactionMetricRawData::FILES) unlink(std::string(file).c_str());

    auto db_main = DBMain::Builder()
                       .SetUseGC(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .SetUseMetrics(true)
                       .Build();
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::TRANSACTION);

    // don't need any initial tuples
    auto *const tested = new LargeDataTableBenchmarkObject(attr_sizes_, 0, txn_length, insert_update_select_ratio,
                                                           db_main->GetStorageLayer()->GetBlockStore().Get(),
                                                           db_main->GetBufferSegmentPool().Get(), &generator_, true);
    const auto result = tested->SimulateOltp(num_txns_, num_concurrent_txns_, db_main->GetMetricsManager().Get());
    abort_count += result.first;
    state.SetIterationTime(static_cast<double>(result.second) / 1000.0);
    db_main->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete tested; });
  }
  state.SetItemsProcessed(state.iterations() * num_txns_ - abort_count);
}

/**
 * Single statement update throughput. Should have low abort rates.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LargeTransactionMetricsBenchmark, SingleStatementUpdate)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 1;
  const std::vector<double> insert_update_select_ratio = {0, 1, 0};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (const auto &file : metrics::TransactionMetricRawData::FILES) unlink(std::string(file).c_str());

    auto db_main = DBMain::Builder()
                       .SetUseGC(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .SetUseMetrics(true)
                       .Build();
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::TRANSACTION);

    auto *const tested = new LargeDataTableBenchmarkObject(
        attr_sizes_, initial_table_size_, txn_length, insert_update_select_ratio,
        db_main->GetStorageLayer()->GetBlockStore().Get(), db_main->GetBufferSegmentPool().Get(), &generator_, true);
    const auto result = tested->SimulateOltp(num_txns_, num_concurrent_txns_, db_main->GetMetricsManager().Get());
    abort_count += result.first;
    state.SetIterationTime(static_cast<double>(result.second) / 1000.0);
    db_main->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete tested; });
  }
  state.SetItemsProcessed(state.iterations() * num_txns_ - abort_count);
}

/**
 * Single statement select throughput. Should have no aborts.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LargeTransactionMetricsBenchmark, SingleStatementSelect)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 1;
  const std::vector<double> insert_update_select_ratio = {0, 0, 1};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (const auto &file : metrics::TransactionMetricRawData::FILES) unlink(std::string(file).c_str());

    auto db_main = DBMain::Builder()
                       .SetUseGC(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .SetUseMetrics(true)
                       .Build();
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::TRANSACTION);

    auto *const tested = new LargeDataTableBenchmarkObject(
        attr_sizes_, initial_table_size_, txn_length, insert_update_select_ratio,
        db_main->GetStorageLayer()->GetBlockStore().Get(), db_main->GetBufferSegmentPool().Get(), &generator_, true);
    const auto result = tested->SimulateOltp(num_txns_, num_concurrent_txns_, db_main->GetMetricsManager().Get());
    abort_count += result.first;
    state.SetIterationTime(static_cast<double>(result.second) / 1000.0);
    db_main->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete tested; });
  }
  state.SetItemsProcessed(state.iterations() * num_txns_ - abort_count);
}

BENCHMARK_REGISTER_F(LargeTransactionMetricsBenchmark, TPCCish)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);

BENCHMARK_REGISTER_F(LargeTransactionMetricsBenchmark, HighAbortRate)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(10);

BENCHMARK_REGISTER_F(LargeTransactionMetricsBenchmark, SingleStatementInsert)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2);

BENCHMARK_REGISTER_F(LargeTransactionMetricsBenchmark, SingleStatementUpdate)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(1);

BENCHMARK_REGISTER_F(LargeTransactionMetricsBenchmark, SingleStatementSelect)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(1);
}  // namespace terrier
