#include <random>
#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "storage/checkpoint_manager.h"
#include "util/storage_test_util.h"

#define CHECKPOINT_FILE_PREFIX "checkpoint_file_"

namespace terrier {

class CheckpointBenchmark : public benchmark::Fixture {
 public:
  storage::CheckpointManager checkpoint_manager_{CHECKPOINT_FILE_PREFIX};
};

/**
 * Checkpoint one single table
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CheckpointBenchmark, SingleTable)(benchmark::State &state) {
  const uint32_t num_rows = 1000000;
  const uint32_t num_columns = 3;
  int magic_seed = 13523777;
  // initialize test
  auto tested = RandomSqlTableTestObject();
  std::default_random_engine random_generator(magic_seed);
  tested.GenerateRandomColumns(num_columns, true, &random_generator);
  tested.Create(catalog::table_oid_t(1));
  tested.InsertRandomRows(num_rows, 0.2, &random_generator);

  storage::SqlTable *table = tested.GetTable();
  catalog::Schema *schema = tested.GetSchema();
  transaction::TransactionManager *txn_manager = tested.GetTxnManager();

  // NOLINTNEXTLINE
  for (auto _ : state) {
    // checkpoint
    transaction::TransactionContext *txn = txn_manager->BeginTransaction();
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      checkpoint_manager_.Process(txn, *table, *schema);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    txn_manager->Commit(txn, StorageTestUtil::EmptyCallback, nullptr);
    checkpoint_manager_.UnlinkCheckpointFiles();
    delete txn;
  }
  state.SetItemsProcessed(state.iterations() * num_rows);
}

BENCHMARK_REGISTER_F(CheckpointBenchmark, SingleTable)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(10);
}  // namespace terrier
