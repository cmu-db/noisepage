#include "storage/metric/database_metric.h"
#include <random>
#include "storage/metric/stats_aggregator.h"
#include "storage/metric/thread_level_stats_collector.h"
#include "util/test_harness.h"
#include "util/testing_stats_util.h"
#include "util/transaction_test_util.h"

namespace terrier {

/**
 * @brief Test the correctness of database metric
 */
class DatabaseMetricTests : public TerrierTest {
 public:
  std::default_random_engine generator_;
  const uint8_t num_iterations_ = 100;
  const uint8_t num_databases_ = 100;
};

/**
 * Basic test for testing metric registration and stats collection, single thread
 */
// NOLINTNEXTLINE
TEST_F(DatabaseMetricTests, BasicTest) {
  for (uint8_t i = 0; i < num_iterations_; i++) {
    auto stats_collector = storage::metric::ThreadLevelStatsCollector();

    // transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    transaction::TransactionManager txn_manager(nullptr, false, LOGGING_DISABLED);
    std::unordered_map<uint8_t, int32_t> commit_map;
    std::unordered_map<uint8_t, int32_t> abort_map;
    for (uint8_t j = 0; j < num_databases_; j++) {
      commit_map[j] = 0;
      abort_map[j] = 0;
      auto num_txns_ = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(0, UINT8_MAX)(generator_));
      for (uint8_t k = 0; k < num_txns_; k++) {
        auto *txn = txn_manager.BeginTransaction();
        storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionBegin(txn);
        auto txn_res = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
        if (txn_res) {
          txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionCommit(
              txn, static_cast<catalog::db_oid_t>(j));
          commit_map[j]++;
        } else {
          txn_manager.Abort(txn);
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionAbort(
              txn, static_cast<catalog::db_oid_t>(j));
          abort_map[j]++;
        }
      }
    }

    storage::metric::StatsAggregator aggregator(&txn_manager, nullptr);
    auto result = aggregator.AggregateRawData();
    EXPECT_FALSE(result.empty());

    for (auto &raw_data : result) {
      if (raw_data->GetMetricType() == storage::metric::MetricType::DATABASE) {
        for (uint8_t j = 0; j < num_databases_; j++) {
          auto commit_cnt = dynamic_cast<storage::metric::DatabaseMetricRawData *>(raw_data.get())
                                ->GetCommitCount(static_cast<catalog::db_oid_t>(j));
          auto abort_cnt = dynamic_cast<storage::metric::DatabaseMetricRawData *>(raw_data.get())
                               ->GetAbortCount(static_cast<catalog::db_oid_t>(j));
          EXPECT_EQ(commit_cnt, commit_map[j]);
          EXPECT_EQ(abort_cnt, abort_map[j]);
        }
      }
    }
  }
}
}  // namespace terrier
