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
#include "type/type_id.h"
#include "util/catalog_test_util.h"
#include "util/multithread_test_util.h"

namespace terrier {

class IndexBenchmark : public benchmark::Fixture {
 private:
  const std::chrono::milliseconds gc_period_{10};
  storage::GarbageCollector *gc_;
  storage::GarbageCollectorThread *gc_thread_;

  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  catalog::Schema table_schema_;
  catalog::IndexSchema index_schema_;

 public:
  std::default_random_engine generator_;

  const uint32_t table_size_ = 10000000;

  // SqlTable
  storage::SqlTable *sql_table_;
  storage::ProjectedRowInitializer tuple_initializer_ =
      storage::ProjectedRowInitializer::Create(std::vector<uint8_t>{1}, std::vector<uint16_t>{1});  // This is a dummy

  // HashIndex
  storage::index::Index *index_;
  transaction::TimestampManager *timestamp_manager_;
  transaction::DeferredActionManager *deferred_action_manager_;
  transaction::TransactionManager *txn_manager_;

  byte *key_buffer_;

 protected:
  void SetUp(const benchmark::State &state) override {
    auto col = catalog::Schema::Column(
        "attribute", type::TypeId::INTEGER, false,
        parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    StorageTestUtil::ForceOid(&(col), catalog::col_oid_t(1));
    table_schema_ = catalog::Schema({col});
    sql_table_ = new storage::SqlTable(&block_store_, table_schema_);
    tuple_initializer_ = sql_table_->InitializerForProjectedRow({catalog::col_oid_t(1)});

    timestamp_manager_ = new transaction::TimestampManager;
    deferred_action_manager_ = new transaction::DeferredActionManager(timestamp_manager_);
    txn_manager_ = new transaction::TransactionManager(timestamp_manager_, deferred_action_manager_, &buffer_pool_,
                                                       true, DISABLED);
    gc_ = new storage::GarbageCollector(timestamp_manager_, deferred_action_manager_, txn_manager_, DISABLED);
    gc_thread_ = new storage::GarbageCollectorThread(gc_, gc_period_);
  }
  void TearDown(const benchmark::State &state) override {
    gc_thread_->GetGarbageCollector().UnregisterIndexForGC(index_);

    delete gc_thread_;
    delete gc_;
    delete sql_table_;
    delete index_;
    delete[] key_buffer_;
    delete txn_manager_;
    delete deferred_action_manager_;
    delete timestamp_manager_;
  }

  void CreateIndex(const storage::index::IndexType type) {
    std::vector<catalog::IndexSchema::Column> keycols;
    keycols.emplace_back("", type::TypeId::INTEGER, false,
                         parser::ColumnValueExpression(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID,
                                                       catalog::col_oid_t(1)));
    StorageTestUtil::ForceOid(&(keycols[0]), catalog::indexkeycol_oid_t(1));
    index_schema_ = catalog::IndexSchema(keycols, type, false, false, false, true);

    index_ = (storage::index::IndexBuilder().SetKeySchema(index_schema_)).Build();
    gc_thread_->GetGarbageCollector().RegisterIndexForGC(index_);
    key_buffer_ = common::AllocationUtil::AllocateAligned(index_->GetProjectedRowInitializer().ProjectedRowSize());
  }

  void PopulateTableAndIndex() {
    auto *const insert_key = index_->GetProjectedRowInitializer().InitializeRow(key_buffer_);
    auto *const insert_txn = txn_manager_->BeginTransaction();
    for (uint32_t i = 0; i < table_size_; i++) {
      auto *const insert_redo =
          insert_txn->StageWrite(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, tuple_initializer_);
      auto *const insert_tuple = insert_redo->Delta();
      *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
      const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);
      *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
      EXPECT_TRUE(index_->Insert(insert_txn, *insert_key, tuple_slot));
    }
    txn_manager_->Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  uint64_t RunWorkload() {
    auto *scan_txn = txn_manager_->BeginTransaction();
    auto *const scan_key_pr = index_->GetProjectedRowInitializer().InitializeRow(key_buffer_);
    uint64_t total_ns = 0;
    uint64_t elapsed_ns = 0;
    std::vector<storage::TupleSlot> results;
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
    txn_manager_->Commit(scan_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    return total_ns;
  }
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(IndexBenchmark, BwTreeIndexRandomScanKey)(benchmark::State &state) {
  CreateIndex(storage::index::IndexType::BWTREE);
  PopulateTableAndIndex();
  // NOLINTNEXTLINE
  for (auto _ : state) {
    const auto total_ns = RunWorkload();
    state.SetIterationTime(static_cast<double>(total_ns) / 1000000000.0);
  }
  state.SetItemsProcessed(state.iterations() * table_size_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(IndexBenchmark, HashIndexRandomScanKey)(benchmark::State &state) {
  CreateIndex(storage::index::IndexType::HASHMAP);
  PopulateTableAndIndex();
  // NOLINTNEXTLINE
  for (auto _ : state) {
    const auto total_ns = RunWorkload();
    state.SetIterationTime(static_cast<double>(total_ns) / 1000000000.0);
  }
  state.SetItemsProcessed(state.iterations() * table_size_);
}

BENCHMARK_REGISTER_F(IndexBenchmark, BwTreeIndexRandomScanKey)->UseManualTime()->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(IndexBenchmark, HashIndexRandomScanKey)->UseManualTime()->Unit(benchmark::kMillisecond);

}  // namespace terrier