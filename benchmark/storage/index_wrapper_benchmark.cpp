#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "parser/expression/column_value_expression.h"
#include "storage/garbage_collector_thread.h"
#include "storage/index/index.h"
#include "storage/index/index_builder.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "test_util/catalog_test_util.h"
#include "test_util/multithread_test_util.h"
#include "transaction/deferred_action_manager.h"
#include "type/type_id.h"

namespace noisepage {

// This benchmark simulates key lookup on a filled table and associated indices to determine the total amount of time
// required for an expected number (equivalent to the size of the table) of searches. We simulate a basic table as we
// expect the search on an index to be roughly equivalent regardless of the the size of the actual table.

class IndexBenchmark : public benchmark::Fixture {
 private:
  // Garbage Collector
  const std::chrono::microseconds gc_period_{1000};
  storage::GarbageCollector *gc_;
  storage::GarbageCollectorThread *gc_thread_;

  // Test infrastructure
  storage::BlockStore block_store_{10000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};

  // Table
  catalog::Schema table_schema_;
  catalog::IndexSchema index_schema_;

 public:
  std::default_random_engine generator_;

  // Table size chosen to exceed L3 cache size on benchmark machine
  const uint32_t table_size_ = 100000000;

  // SqlTable
  storage::SqlTable *sql_table_;
  storage::ProjectedRowInitializer tuple_initializer_ =
      storage::ProjectedRowInitializer::Create(std::vector<uint16_t>{1}, std::vector<uint16_t>{1});  // This is a dummy

  // HashIndex or BwTreeIndex
  common::ManagedPointer<storage::index::Index> index_;
  transaction::TimestampManager *timestamp_manager_;
  transaction::DeferredActionManager *deferred_action_manager_;
  transaction::TransactionManager *txn_manager_;

  byte *key_buffer_;

 protected:
  // Set up table and associated managers and garbage collector for structure
  void SetUp(const benchmark::State &state) override {
    // Define standard column template type to have name, integer type, non-nullable state, and associated parser
    auto col = catalog::Schema::Column("attribute", type::TypeId::INTEGER, false,
                                       parser::ConstantValueExpression(type::TypeId::INTEGER));

    StorageTestUtil::ForceOid(&(col), catalog::col_oid_t(1));

    // Setup table with schema based on column structures
    table_schema_ = catalog::Schema({col});
    sql_table_ = new storage::SqlTable(common::ManagedPointer(&block_store_), table_schema_);
    tuple_initializer_ = sql_table_->InitializerForProjectedRow({catalog::col_oid_t(1)});

    // Create time, action, and transaction managers
    timestamp_manager_ = new transaction::TimestampManager;
    deferred_action_manager_ = new transaction::DeferredActionManager(common::ManagedPointer(timestamp_manager_));
    txn_manager_ = new transaction::TransactionManager(common::ManagedPointer(timestamp_manager_),
                                                       common::ManagedPointer(deferred_action_manager_),
                                                       common::ManagedPointer(&buffer_pool_), true, false, DISABLED);
    gc_ = new storage::GarbageCollector(common::ManagedPointer(timestamp_manager_),
                                        common::ManagedPointer(deferred_action_manager_),
                                        common::ManagedPointer(txn_manager_), DISABLED);
    gc_thread_ = new storage::GarbageCollectorThread(common::ManagedPointer(gc_), gc_period_, nullptr);
  }

  // Script to free all allocated elements of table structure
  void TearDown(const benchmark::State &state) override {
    gc_thread_->GetGarbageCollector()->UnregisterIndexForGC(index_);

    delete gc_thread_;
    delete gc_;
    delete sql_table_;
    delete[] key_buffer_;
    delete txn_manager_;
    delete deferred_action_manager_;
    delete timestamp_manager_;
  }

  // Schema will consist of single column as we are more concerned about overhead of storage layer than content
  void CreateIndex(const storage::index::IndexType type) {
    // Create attribute for schema
    std::vector<catalog::IndexSchema::Column> keycols;
    keycols.emplace_back("", type::TypeId::INTEGER, false,
                         parser::ColumnValueExpression(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID,
                                                       catalog::col_oid_t(1)));

    // Enforce OID is 1 for column utilized
    StorageTestUtil::ForceOid(&(keycols[0]), catalog::indexkeycol_oid_t(1));

    // Define fields of index schema and declare index
    index_schema_ = catalog::IndexSchema(keycols, type, false, false, false, true);
    index_ = (storage::index::IndexBuilder().SetKeySchema(index_schema_)).Build();

    // Register index to garbage collector
    gc_thread_->GetGarbageCollector()->RegisterIndexForGC(index_);

    // Allocate buffer for data
    key_buffer_ = common::AllocationUtil::AllocateAligned(index_->GetProjectedRowInitializer().ProjectedRowSize());
  }

  // Table is populated with sequentially increasing keys, which will not make performance difference on hash index, but
  // may on the BwTree structure
  void PopulateTableAndIndex() {
    // Set up keystore and transaction manager for insert actions
    auto *const insert_key = index_->GetProjectedRowInitializer().InitializeRow(key_buffer_);
    auto *const insert_txn = txn_manager_->BeginTransaction();

    // While table is not full, continue to insert elements
    for (uint32_t i = 0; i < table_size_; i++) {
      auto *const insert_redo =
          insert_txn->StageWrite(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, tuple_initializer_);
      auto *const insert_tuple = insert_redo->Delta();
      *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
      const auto tuple_slot = sql_table_->Insert(common::ManagedPointer(insert_txn), insert_redo);
      *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;

      // Ensure that insert action appropriately listed
      EXPECT_TRUE(index_->Insert(common::ManagedPointer(insert_txn), *insert_key, tuple_slot));
    }
    // Store transactions completed
    txn_manager_->Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  // Do a random lookup of a subset of keys in the domain; scoped timer only times ScanKey operation
  // and will accumulate into a value for the total amount of time required
  uint64_t RunWorkload() {
    // Set-up transaction manager, buffer, and times for scans
    auto *scan_txn = txn_manager_->BeginTransaction();
    auto *const scan_key_pr = index_->GetProjectedRowInitializer().InitializeRow(key_buffer_);
    uint64_t total_ns = 0;
    uint64_t elapsed_ns = 0;

    std::vector<storage::TupleSlot> results;
    // Get random key table_size times and measure time elapsed for search operation
    for (uint32_t i = 0; i < table_size_; i++) {
      const uint32_t random_key =
          std::uniform_int_distribution(static_cast<uint32_t>(0), static_cast<uint32_t>(table_size_ - 1))(generator_);
      *reinterpret_cast<uint32_t *>(scan_key_pr->AccessForceNotNull(0)) = random_key;
      {
        common::ScopedTimer<std::chrono::nanoseconds> timer(&elapsed_ns);
        index_->ScanKey(*scan_txn, *scan_key_pr, &results);
      }
      EXPECT_EQ(results.size(), 1);
      results.clear();
      total_ns += elapsed_ns;
    }

    // Store transactions completed
    txn_manager_->Commit(scan_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    return total_ns;
  }
};

// Determine required time to run key lookup with BwTree structure for index
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(IndexBenchmark, BwTreeIndexRandomScanKey)(benchmark::State &state) {
  // Create index using BwTree and populate associated table
  CreateIndex(storage::index::IndexType::BWTREE);
  PopulateTableAndIndex();
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // Run key lookup and record amount of time required in seconds
    const auto total_ns = RunWorkload();
    state.SetIterationTime(static_cast<double>(total_ns) / 1000000000.0);
  }
  // Determine total number of items processed
  state.SetItemsProcessed(state.iterations() * table_size_);
}

// Determine required time to run key lookup with HashMap structure for index
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(IndexBenchmark, HashIndexRandomScanKey)(benchmark::State &state) {
  CreateIndex(storage::index::IndexType::HASHMAP);
  PopulateTableAndIndex();
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // Run key lookup and record amount of time required in seconds
    const auto total_ns = RunWorkload();
    state.SetIterationTime(static_cast<double>(total_ns) / 1000000000.0);
  }
  // Determine total number of items processed
  state.SetItemsProcessed(state.iterations() * table_size_);
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(IndexBenchmark, BwTreeIndexRandomScanKey)
    ->UseManualTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(IndexBenchmark, HashIndexRandomScanKey)
    ->UseManualTime()
    ->Unit(benchmark::kMillisecond);
// clang-format on

}  // namespace noisepage
