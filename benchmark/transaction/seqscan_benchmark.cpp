#include <test_util/catalog_test_util.h>
#include <vector>
#include "benchmark/benchmark.h"

#include "execution/sql/table_vector_iterator.h"
#include "execution/sql_test.h"

#include "benchmark_util/data_table_benchmark_util.h"
#include "storage/garbage_collector_thread.h"

namespace terrier {

// This benchmark simulates a sequential scan over a filled table to determine the total amount of time
// required for a complete search through the table. We simulate a table with random initialization to mirror
// realistic transaction behavior.

class SeqscanBenchmark : public benchmark::Fixture {
  //TODO: Do I need a transaction/action/buffers manager, catalog, ?
 public:
  // Attribute and table sizes
  const std::vector<uint8_t> attr_sizes_ = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8};
  const uint32_t initial_table_size_ = 1000000;
  const uint32_t num_txns_ = 100000;

  // Test infrastructure
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  std::default_random_engine generator_;
  const uint32_t num_concurrent_txns_ = 4;

  // Garbage collector
  storage::GarbageCollector *gc_;
  storage::GarbageCollectorThread *gc_thread_ = nullptr;
  const std::chrono::milliseconds gc_period_{10};

 protected:
  // Execution context for test
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;

  std::unique_ptr<exec::ExecutionContext> MakeExecCtx(exec::OutputCallback &&callback = nullptr,
                                                      const planner::OutputSchema *schema = nullptr) {
    auto accessor = catalog_->GetAccessor(test_txn_, test_db_oid_);
    return std::make_unique<exec::ExecutionContext>(test_db_oid_, test_txn_, callback, schema, std::move(accessor));
  }

  void SetUp(const benchmark::State &state) override {
    // single statement insert
    const uint32_t txn_length = 1;
    const std::vector<double> insert_update_select_ratio = {1, 0, 0};


    // TODO: IS test_table accessible outside SetUp?
    // create table object with parameters
    LargeDataTableBenchmarkObject test_table(attr_sizes_, initial_table_size_, txn_length, insert_update_select_ratio,
                                       &block_store_, &buffer_pool_, &generator_, true);

    // execution context initialization
    exec_ctx_ = MakeExecCtx();

    // gc initialization
    gc_ = new storage::GarbageCollector(test_table.GetTimestampManager(), DISABLED, test_table.GetTxnManager(), DISABLED);
    gc_thread_ = new storage::GarbageCollectorThread(gc_, gc_period_);

    // ignore output of oltp
    static_cast<void>(test_table.SimulateOltp(num_txns_, num_concurrent_txns_));
  }



  void TearDown(const benchmark::State &state) override {
    // delete allocated garbage collectors
    delete gc_thread_;
    delete gc_;
  }
};

/**
 * Run a sequential scan through the datatable
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SeqscanBenchmark, SequentialScan)(benchmark::State &state) {
  // TODO: get execution context, table oid, iterate over columns,

  auto table_oid = CatalogTestUtil::TEST_TABLE_OID;
  // Incorrect^?

  std::array<uint32_t, 1> col_oids{1};
  TableVectorIterator iter(exec_ctx_.get(), !table_oid, col_oids.data(), static_cast<uint32_t>(col_oids.size()));
  iter.Init();

  ProjectedColumnsIterator *pci = iter.GetProjectedColumnsIterator();


  uint32_t num_tuples = 0;
  int32_t prev_val{0};
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      auto *val = pci->Get<int32_t, false>(0, nullptr);
      if (num_tuples > 0) {
        ASSERT_EQ(*val, prev_val + 1);
      }
      prev_val = *val;
      num_tuples++;
    }
    pci->Reset();
  }

  // TODO: GET SIZE OF TABLE
  EXPECT_EQ(, num_tuples);
}

BENCHMARK_REGISTER_F(SeqscanBenchmark, SequentialScan)->UseManualTime()->Unit(benchmark::kMillisecond);
}  // namespace terrier::execution