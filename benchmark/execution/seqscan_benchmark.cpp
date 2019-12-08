#include <common/scoped_timer.h>

#include <array>
#include <memory>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/data_table_benchmark_util.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql_test.h"
#include "storage/garbage_collector_thread.h"

namespace terrier {

// This benchmark simulates a sequential scan over a filled table to determine the total amount of time
// required for a complete search through the table. We simulate a table with random initialization to mirror
// realistic transaction behavior.

class SeqscanBenchmark : public benchmark::Fixture {
 public:
  // Function to create execution context for database
  std::unique_ptr<execution::exec::ExecutionContext> MakeExecCtx(execution::exec::OutputCallback &&callback = nullptr,
                                                                 const planner::OutputSchema *schema = nullptr) {
    auto accessor = catalog_->GetAccessor(test_txn_, test_db_oid_);
    return std::make_unique<execution::exec::ExecutionContext>(test_db_oid_, test_txn_, callback, schema,
                                                               std::move(accessor));
  }

  // Generate set of test tables
  void GenerateTestTables(execution::exec::ExecutionContext *exec_ctx) {
    execution::sql::TableGenerator table_generator{exec_ctx, block_store_.get(), test_ns_oid_};
    table_generator.GenerateTestTables();
  }

  // Get namespace oids
  catalog::namespace_oid_t NSOid() { return test_ns_oid_; }

  void SetUp(const benchmark::State &state) override {
    // Initialize terrier objects
    block_store_ = std::make_unique<storage::BlockStore>(1000, 1000);
    buffer_pool_ = std::make_unique<storage::RecordBufferSegmentPool>(100000, 100000);
    tm_manager_ = std::make_unique<transaction::TimestampManager>();
    da_manager_ = std::make_unique<transaction::DeferredActionManager>(tm_manager_.get());
    txn_manager_ = std::make_unique<transaction::TransactionManager>(tm_manager_.get(), da_manager_.get(),
                                                                     buffer_pool_.get(), true, nullptr);

    // Garbage collector
    gc_ =
        std::make_unique<storage::GarbageCollector>(tm_manager_.get(), da_manager_.get(), txn_manager_.get(), nullptr);

    // Transaction context
    test_txn_ = txn_manager_->BeginTransaction();

    // Create catalog and test namespace
    catalog_ = std::make_unique<catalog::Catalog>(txn_manager_.get(), block_store_.get());
    test_db_oid_ = catalog_->CreateDatabase(test_txn_, "test_db", true);
    ASSERT_NE(test_db_oid_, catalog::INVALID_DATABASE_OID) << "Default database does not exist";
    auto accessor = catalog_->GetAccessor(test_txn_, test_db_oid_);
    test_ns_oid_ = accessor->GetDefaultNamespace();

    // Set up execution context
    exec_ctx_ = MakeExecCtx();

    // Generate test tables
    GenerateTestTables(exec_ctx_.get());
  }

  void TearDown(const benchmark::State &state) override {
    txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    // Note: execution context must be released to end lifecycle before tear down completes
    (void) exec_ctx_.release();
    catalog_->TearDown();
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
  }

 protected:
  std::unique_ptr<execution::exec::ExecutionContext> exec_ctx_;

 private:
  std::unique_ptr<storage::BlockStore> block_store_;
  std::unique_ptr<storage::RecordBufferSegmentPool> buffer_pool_;
  std::unique_ptr<transaction::TimestampManager> tm_manager_;
  std::unique_ptr<transaction::DeferredActionManager> da_manager_;
  std::unique_ptr<transaction::TransactionManager> txn_manager_;
  std::unique_ptr<catalog::Catalog> catalog_;
  std::unique_ptr<storage::GarbageCollector> gc_;
  catalog::db_oid_t test_db_oid_{0};
  catalog::namespace_oid_t test_ns_oid_;
  transaction::TransactionContext *test_txn_;
};

/**
 * Run a sequential scan through the sql table
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SeqscanBenchmark, SequentialScan)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // Get execution context for namespace and test tables
    auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");

    // Get array of column object ids
    std::array<uint32_t, 1> col_oids{1};

    // Declare iterator through table data
    execution::sql::TableVectorIterator iter(exec_ctx_.get(), !table_oid, col_oids.data(),
                                             static_cast<uint32_t>(col_oids.size()));
    iter.Init();

    // Iterator over projection
    execution::sql::ProjectedColumnsIterator *pci = iter.GetProjectedColumnsIterator();

    // Initialize timer for iteration
    uint64_t elapsed_ms = 0;

    {
      // Create timer for iteration time
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);

      // count number of tuples read
      uint32_t num_tuples = 0;
      int32_t prev_val{0};

      // Iterate through tuples in iterator
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

      // Expect number of tuples to match size
      EXPECT_EQ(execution::sql::TEST1_SIZE, num_tuples);
    }

    // Set iteration time
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  // Set number of iterations run
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(SeqscanBenchmark, SequentialScan)->UseManualTime()->Unit(benchmark::kMillisecond);
}  // namespace terrier
