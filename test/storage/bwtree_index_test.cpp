#include <util/catalog_test_util.h>
#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <random>
#include <vector>
#include "parser/expression/column_value_expression.h"
#include "portable_endian/portable_endian.h"
#include "storage/garbage_collector_thread.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/index_builder.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "type/type_id.h"
#include "type/type_util.h"
#include "util/random_test_util.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

namespace terrier::storage::index {

class BwTreeIndexTests : public TerrierTest {
 private:
  const std::chrono::milliseconds gc_period_{10};
  storage::GarbageCollectorThread *gc_thread_;

  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  catalog::Schema table_schema_;
  catalog::IndexSchema unique_schema_;
  catalog::IndexSchema default_schema_;

 public:
  BwTreeIndexTests() {
    auto col = catalog::Schema::Column(
        "attribute", type::TypeId::INTEGER, false,
        parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    StorageTestUtil::ForceOid(&(col), catalog::col_oid_t(1));
    table_schema_ = catalog::Schema({col});
    sql_table_ = new storage::SqlTable(&block_store_, table_schema_);
    tuple_initializer_ = sql_table_->InitializerForProjectedRow({catalog::col_oid_t(1)}).first;

    std::vector<catalog::IndexSchema::Column> keycols;
    keycols.emplace_back(
        "", type::TypeId::INTEGER, false,
        parser::ColumnValueExpression(catalog::db_oid_t(0), catalog::table_oid_t(0), catalog::col_oid_t(1)));
    StorageTestUtil::ForceOid(&(keycols[0]), catalog::indexkeycol_oid_t(1));
    unique_schema_ = catalog::IndexSchema(keycols, true, true, false, true);
    default_schema_ = catalog::IndexSchema(keycols, false, false, false, true);
  }

  std::default_random_engine generator_;
  const uint32_t num_threads_ = 4;

  // SqlTable
  storage::SqlTable *sql_table_;
  storage::ProjectedRowInitializer tuple_initializer_ =
      storage::ProjectedRowInitializer::Create(std::vector<uint8_t>{1}, {1});

  // BwTreeIndex
  Index *default_index_, *unique_index_;
  transaction::TransactionManager txn_manager_{&buffer_pool_, true, LOGGING_DISABLED};

  byte *key_buffer_1_, *key_buffer_2_;

  common::WorkerPool thread_pool_{num_threads_, {}};

 protected:
  void SetUp() override {
    TerrierTest::SetUp();
    gc_thread_ = new storage::GarbageCollectorThread(&txn_manager_, gc_period_);

    unique_index_ = (IndexBuilder()
                         .SetConstraintType(ConstraintType::UNIQUE)
                         .SetKeySchema(unique_schema_)
                         .SetOid(catalog::index_oid_t(2)))
                        .Build();
    default_index_ = (IndexBuilder()
                          .SetConstraintType(ConstraintType::DEFAULT)
                          .SetKeySchema(default_schema_)
                          .SetOid(catalog::index_oid_t(2)))
                         .Build();

    gc_thread_->GetGarbageCollector().RegisterIndexForGC(unique_index_);
    gc_thread_->GetGarbageCollector().RegisterIndexForGC(default_index_);

    key_buffer_1_ =
        common::AllocationUtil::AllocateAligned(default_index_->GetProjectedRowInitializer().ProjectedRowSize());
    key_buffer_2_ =
        common::AllocationUtil::AllocateAligned(default_index_->GetProjectedRowInitializer().ProjectedRowSize());
  }
  void TearDown() override {
    gc_thread_->GetGarbageCollector().UnregisterIndexForGC(unique_index_);
    gc_thread_->GetGarbageCollector().UnregisterIndexForGC(default_index_);

    delete gc_thread_;
    delete sql_table_;
    delete default_index_;
    delete unique_index_;
    delete[] key_buffer_1_;
    delete[] key_buffer_2_;
    TerrierTest::TearDown();
  }
};

/**
 * This test creates multiple worker threads that all try to insert [0,num_inserts) as tuples in the table and into the
 * primary key index. At completion of the workload, only num_inserts_ txns should have committed with visible versions
 * in the index and table.
 */
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, UniqueInsert) {
  const uint32_t num_inserts_ = 100000;  // number of tuples/primary keys for each worker to attempt to insert
  auto workload = [&](uint32_t worker_id) {
    //    auto *const insert_buffer =
    //        common::AllocationUtil::AllocateAligned(unique_index_->GetProjectedRowInitializer().ProjectedRowSize());
    //    auto *const insert_tuple = tuple_initializer_.InitializeRow(insert_buffer);
    auto *const key_buffer =
        common::AllocationUtil::AllocateAligned(unique_index_->GetProjectedRowInitializer().ProjectedRowSize());
    auto *const insert_key = unique_index_->GetProjectedRowInitializer().InitializeRow(key_buffer);

    // some threads count up, others count down. This is to mix whether threads abort for write-write conflict or
    // previously committed versions
    if (worker_id % 2 == 0) {
      for (uint32_t i = 0; i < num_inserts_; i++) {
        auto *const insert_txn = txn_manager_.BeginTransaction();
        auto *const insert_redo =
            insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
        auto *const insert_tuple = insert_redo->Delta();
        *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
        const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

        *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
        if (unique_index_->InsertUnique(insert_txn, *insert_key, tuple_slot)) {
          txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
        } else {
          txn_manager_.Abort(insert_txn);
        }
      }

    } else {
      for (uint32_t i = num_inserts_ - 1; i < num_inserts_; i--) {
        auto *const insert_txn = txn_manager_.BeginTransaction();
        auto *const insert_redo =
            insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
        auto *const insert_tuple = insert_redo->Delta();
        *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
        const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

        *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
        if (unique_index_->InsertUnique(insert_txn, *insert_key, tuple_slot)) {
          txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
        } else {
          txn_manager_.Abort(insert_txn);
        }
      }
    }
    delete[] key_buffer;
  };

  // run the workload
  for (uint32_t i = 0; i < num_threads_; i++) {
    thread_pool_.SubmitTask([i, &workload] { workload(i); });
  }
  thread_pool_.WaitUntilAllFinished();

  // scan the results
  auto *const scan_txn = txn_manager_.BeginTransaction();

  std::vector<storage::TupleSlot> results;

  auto *const low_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  auto *const high_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_2_);

  // scan[0,num_inserts_) should hit num_inserts_ keys (no duplicates)
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 0;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = num_inserts_ - 1;
  unique_index_->ScanAscending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), num_inserts_);

  txn_manager_.Commit(scan_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/**
 * This test creates multiple worker threads that all try to insert [0,num_inserts) as tuples in the table and into the
 * primary key index. At completion of the workload, all num_inserts_ txns * num_threads_ should have committed with
 * visible versions in the index and table.
 */
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, DefaultInsert) {
  const uint32_t num_inserts_ = 100000;  // number of tuples/primary keys for each worker to attempt to insert
  auto workload = [&](uint32_t worker_id) {
    auto *const key_buffer =
        common::AllocationUtil::AllocateAligned(default_index_->GetProjectedRowInitializer().ProjectedRowSize());
    auto *const insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer);

    // some threads count up, others count down. Threads shouldn't abort each other
    if (worker_id % 2 == 0) {
      for (uint32_t i = 0; i < num_inserts_; i++) {
        auto *const insert_txn = txn_manager_.BeginTransaction();
        auto *const insert_redo =
            insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
        auto *const insert_tuple = insert_redo->Delta();
        *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
        const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

        *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
        EXPECT_TRUE(default_index_->Insert(insert_txn, *insert_key, tuple_slot));
        txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      }
    } else {
      for (uint32_t i = num_inserts_ - 1; i < num_inserts_; i--) {
        auto *const insert_txn = txn_manager_.BeginTransaction();
        auto *const insert_redo =
            insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
        auto *const insert_tuple = insert_redo->Delta();
        *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
        const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

        *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
        EXPECT_TRUE(default_index_->Insert(insert_txn, *insert_key, tuple_slot));
        txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      }
    }

    delete[] key_buffer;
  };

  // run the workload
  for (uint32_t i = 0; i < num_threads_; i++) {
    thread_pool_.SubmitTask([i, &workload] { workload(i); });
  }
  thread_pool_.WaitUntilAllFinished();

  // scan the results
  auto *const scan_txn = txn_manager_.BeginTransaction();

  std::vector<storage::TupleSlot> results;

  auto *const low_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  auto *const high_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_2_);

  // scan[0,num_inserts_) should hit num_inserts_ * num_threads_ keys
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 0;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = num_inserts_ - 1;
  default_index_->ScanAscending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), num_inserts_ * num_threads_);

  txn_manager_.Commit(scan_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/**
 * Tests basic scan behavior using various windows to scan over (some out of of bounds of keyspace, some matching
 * exactly, etc.)
 */
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, ScanAscending) {
  // populate index with [0..20] even keys
  std::map<int32_t, storage::TupleSlot> reference;
  auto *const insert_txn = txn_manager_.BeginTransaction();
  for (int32_t i = 0; i <= 20; i += 2) {
    auto *const insert_redo =
        insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
    auto *const insert_tuple = insert_redo->Delta();
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
    const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

    auto *const insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
    *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;

    EXPECT_TRUE(default_index_->Insert(insert_txn, *insert_key, tuple_slot));
    reference[i] = tuple_slot;
  }
  txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *const scan_txn = txn_manager_.BeginTransaction();

  std::vector<storage::TupleSlot> results;

  auto *const low_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  auto *const high_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_2_);

  // scan[8,12] should hit keys 8, 10, 12
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 8;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 12;
  default_index_->ScanAscending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(8), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  EXPECT_EQ(reference.at(12), results[2]);
  results.clear();

  // scan[7,13] should hit keys 8, 10, 12
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 7;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 13;
  default_index_->ScanAscending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(8), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  EXPECT_EQ(reference.at(12), results[2]);
  results.clear();

  // scan[-1,5] should hit keys 0, 2, 4
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = -1;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 5;
  default_index_->ScanAscending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(0), results[0]);
  EXPECT_EQ(reference.at(2), results[1]);
  EXPECT_EQ(reference.at(4), results[2]);
  results.clear();

  // scan[15,21] should hit keys 16, 18, 20
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 21;
  default_index_->ScanAscending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(16), results[0]);
  EXPECT_EQ(reference.at(18), results[1]);
  EXPECT_EQ(reference.at(20), results[2]);
  results.clear();

  txn_manager_.Commit(scan_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/**
 * Tests basic scan behavior using various windows to scan over (some out of of bounds of keyspace, some matching
 * exactly, etc.)
 */
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, ScanDescending) {
  // populate index with [0..20] even keys
  std::map<int32_t, storage::TupleSlot> reference;
  auto *const insert_txn = txn_manager_.BeginTransaction();
  for (int32_t i = 0; i <= 20; i += 2) {
    auto *const insert_redo =
        insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
    auto *const insert_tuple = insert_redo->Delta();
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
    const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

    auto *const insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
    *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
    EXPECT_TRUE(default_index_->Insert(insert_txn, *insert_key, tuple_slot));
    reference[i] = tuple_slot;
  }
  txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *const scan_txn = txn_manager_.BeginTransaction();

  std::vector<storage::TupleSlot> results;

  auto *const low_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  auto *const high_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_2_);

  // scan[8,12] should hit keys 12, 10, 8
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 8;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 12;
  default_index_->ScanDescending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(12), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  EXPECT_EQ(reference.at(8), results[2]);
  results.clear();

  // scan[7,13] should hit keys 12, 10, 8
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 7;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 13;
  default_index_->ScanDescending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(12), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  EXPECT_EQ(reference.at(8), results[2]);
  results.clear();

  // scan[-1,5] should hit keys 4, 2, 0
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = -1;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 5;
  default_index_->ScanDescending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(4), results[0]);
  EXPECT_EQ(reference.at(2), results[1]);
  EXPECT_EQ(reference.at(0), results[2]);
  results.clear();

  // scan[15,21] should hit keys 20, 18, 16
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 21;
  default_index_->ScanDescending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(20), results[0]);
  EXPECT_EQ(reference.at(18), results[1]);
  EXPECT_EQ(reference.at(16), results[2]);
  results.clear();

  txn_manager_.Commit(scan_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/**
 * Tests basic scan behavior using various windows to scan over (some out of of bounds of keyspace, some matching
 * exactly, etc.)
 */
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, ScanLimitAscending) {
  // populate index with [0..20] even keys
  std::map<int32_t, storage::TupleSlot> reference;
  auto *const insert_txn = txn_manager_.BeginTransaction();
  for (int32_t i = 0; i <= 20; i += 2) {
    auto *const insert_redo =
        insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
    auto *const insert_tuple = insert_redo->Delta();
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
    const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

    auto *const insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
    *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
    EXPECT_TRUE(default_index_->Insert(insert_txn, *insert_key, tuple_slot));
    reference[i] = tuple_slot;
  }
  txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *const scan_txn = txn_manager_.BeginTransaction();

  std::vector<storage::TupleSlot> results;

  auto *const low_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  auto *const high_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_2_);

  // scan_limit[8,12] should hit keys 8, 10
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 8;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 12;
  default_index_->ScanLimitAscending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(8), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  results.clear();

  // scan_limit[7,13] should hit keys 8, 10
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 7;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 13;
  default_index_->ScanLimitAscending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(8), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  results.clear();

  // scan_limit[-1,5] should hit keys 0, 2
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = -1;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 5;
  default_index_->ScanLimitAscending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(0), results[0]);
  EXPECT_EQ(reference.at(2), results[1]);
  results.clear();

  // scan_limit[15,21] should hit keys 16, 18
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 21;
  default_index_->ScanLimitAscending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(16), results[0]);
  EXPECT_EQ(reference.at(18), results[1]);
  results.clear();

  txn_manager_.Commit(scan_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

/**
 * Tests basic scan behavior using various windows to scan over (some out of of bounds of keyspace, some matching
 * exactly, etc.)
 */
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, ScanLimitDescending) {
  // populate index with [0..20] even keys
  std::map<int32_t, storage::TupleSlot> reference;
  auto *const insert_txn = txn_manager_.BeginTransaction();
  for (int32_t i = 0; i <= 20; i += 2) {
    auto *const insert_redo =
        insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
    auto *const insert_tuple = insert_redo->Delta();
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
    const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

    auto *const insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
    *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
    EXPECT_TRUE(default_index_->Insert(insert_txn, *insert_key, tuple_slot));
    reference[i] = tuple_slot;
  }
  txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *const scan_txn = txn_manager_.BeginTransaction();

  std::vector<storage::TupleSlot> results;

  auto *const low_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  auto *const high_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_2_);

  // scan_limit[8,12] should hit keys 12, 10
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 8;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 12;
  default_index_->ScanLimitDescending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(12), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  results.clear();

  // scan_limit[7,13] should hit keys 12, 10
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 7;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 13;
  default_index_->ScanLimitDescending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(12), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  results.clear();

  // scan_limit[-1,5] should hit keys 4, 2
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = -1;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 5;
  default_index_->ScanLimitDescending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(4), results[0]);
  EXPECT_EQ(reference.at(2), results[1]);
  results.clear();

  // scan_limit[15,21] should hit keys 20, 18
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 21;
  default_index_->ScanLimitDescending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(20), results[0]);
  EXPECT_EQ(reference.at(18), results[1]);
  results.clear();

  txn_manager_.Commit(scan_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Verifies that primary key insert fails on write-write conflict
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, UniqueKey1) {
  auto *txn0 = txn_manager_.BeginTransaction();

  // txn 0 inserts into table
  auto *insert_redo =
      txn0->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(txn0, insert_redo);

  // txn 0 inserts into index
  auto *insert_key = unique_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(unique_index_->InsertUnique(txn0, *insert_key, tuple_slot));

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = unique_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  // txn 0 scans index and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  unique_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 scans index and gets no visible result
  unique_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  // txn 1 inserts into table
  insert_redo = txn1->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto new_tuple_slot = sql_table_->Insert(txn1, insert_redo);

  // txn 1 inserts into index and fails due to write-write conflict with txn 0
  insert_key = unique_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_FALSE(unique_index_->InsertUnique(txn1, *insert_key, new_tuple_slot));

  txn_manager_.Abort(txn1);

  txn_manager_.Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index and gets a visible, correct result
  unique_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Verifies that primary key insert fails on visible key conflict
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, UniqueKey2) {
  auto *txn0 = txn_manager_.BeginTransaction();

  // txn 0 inserts into table
  auto *insert_redo =
      txn0->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(txn0, insert_redo);

  // txn 0 inserts into index
  auto *insert_key = unique_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(unique_index_->InsertUnique(txn0, *insert_key, tuple_slot));

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = unique_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  // txn 0 scans index and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  unique_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 inserts into table
  insert_redo = txn1->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto new_tuple_slot = sql_table_->Insert(txn1, insert_redo);

  // txn 1 inserts into index and fails due to visible key conflict with txn 0
  insert_key = unique_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_FALSE(unique_index_->InsertUnique(txn1, *insert_key, new_tuple_slot));

  txn_manager_.Abort(txn1);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index and gets a visible, correct result
  unique_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Verifies that primary key insert fails on same txn trying to insert key twice
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, UniqueKey3) {
  auto *txn0 = txn_manager_.BeginTransaction();

  // txn 0 inserts into table
  auto *insert_redo =
      txn0->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(txn0, insert_redo);

  // txn 0 inserts into index
  auto *insert_key = unique_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(unique_index_->InsertUnique(txn0, *insert_key, tuple_slot));

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = unique_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  // txn 0 scans index and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  unique_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 0 inserts into table
  insert_redo = txn0->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto new_tuple_slot = sql_table_->Insert(txn0, insert_redo);

  // txn 0 inserts into index and fails due to visible key conflict with txn 0
  insert_key = unique_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_FALSE(unique_index_->InsertUnique(txn0, *insert_key, new_tuple_slot));

  txn_manager_.Abort(txn0);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index and gets no visible result
  unique_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// Verifies that primary key insert fails even if conflicting transaction is an uncommitted delete
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, UniqueKey4) {
  auto *txn0 = txn_manager_.BeginTransaction();

  // txn 0 inserts into table
  auto *insert_redo =
      txn0->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(txn0, insert_redo);

  // txn 0 inserts into index
  auto *insert_key = unique_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(unique_index_->InsertUnique(txn0, *insert_key, tuple_slot));

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = unique_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  // txn 0 scans index and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  unique_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 0 deletes from table
  txn0->StageDelete(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_slot);
  EXPECT_TRUE(sql_table_->Delete(txn0, tuple_slot));

  // txn 0 deletes from index
  insert_key = unique_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  unique_index_->Delete(txn0, *insert_key, tuple_slot);

  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 inserts into table
  insert_redo = txn1->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto new_tuple_slot = sql_table_->Insert(txn1, insert_redo);

  // txn 1 inserts into index and fails due to write-write conflict with txn 0
  insert_key = unique_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_FALSE(unique_index_->InsertUnique(txn1, *insert_key, new_tuple_slot));

  txn_manager_.Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  txn_manager_.Abort(txn1);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index and gets no visible result
  unique_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//    W(X)   |        |        |
//    R(X)   |        |        |
//           | BEGIN  |        |
//           | R(X)   |        |
//    COMMIT |        |        |
//           | R(X)   |        |
//           | COMMIT |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read Txn #0's version of X
// Txn #1 should only read the previous version of X because its start time is before #0's commit
// Txn #2 should only read Txn #0's version of X
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, CommitInsert1) {
  auto *txn0 = txn_manager_.BeginTransaction();

  // txn 0 inserts into table
  auto *insert_redo =
      txn0->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(txn0, insert_redo);

  // txn 0 inserts into index
  auto *const insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(default_index_->Insert(txn0, *insert_key, tuple_slot));

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  // txn 0 scans index and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 scans index and gets no visible result
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  // txn 1 scans index and gets no visible result
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index and gets a visible, correct result
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//           | BEGIN  |        |
//           | W(X)   |        |
//    R(X)   |        |        |
//           | R(X)   |        |
//           | COMMIT |        |
//    R(X)   |        |        |
//    COMMIT |        |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read the previous version of X because its start time is before #1's commit
// Txn #1 should only read Txn #1's version of X
// Txn #2 should only read Txn #1's version of X
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, CommitInsert2) {
  auto *txn0 = txn_manager_.BeginTransaction();
  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 inserts into table
  auto *insert_redo =
      txn1->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(txn1, insert_redo);

  // txn 1 inserts into index
  auto *const insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(default_index_->Insert(txn1, *insert_key, tuple_slot));

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  // txn 0 scans index and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  // txn 1 scans index and gets a visible, correct result
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

  // txn 0 scans index and gets no visible result
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index and gets a visible, correct result
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//    W(X)   |        |        |
//    R(X)   |        |        |
//           | BEGIN  |        |
//           | R(X)   |        |
//    ABORT  |        |        |
//           | R(X)   |        |
//           | COMMIT |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read Txn #0's version of X
// Txn #1 should only read the previous version of X because Txn #0's is uncommitted
// Txn #2 should only read the previous version of X because Txn #0 aborted
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, AbortInsert1) {
  auto *txn0 = txn_manager_.BeginTransaction();

  // txn 0 inserts into table
  auto *insert_redo =
      txn0->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(txn0, insert_redo);

  // txn 0 inserts into index
  auto *const insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(default_index_->Insert(txn0, *insert_key, tuple_slot));

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  // txn 0 scans index and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 scans index and gets no visible result
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Abort(txn0);

  // txn 1 scans index and gets no visible result
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index and gets no visible result
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//           | BEGIN  |        |
//           | W(X)   |        |
//    R(X)   |        |        |
//           | R(X)   |        |
//           | ABORT  |        |
//    R(X)   |        |        |
//    COMMIT |        |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read the previous version of X because Txn #1's is uncommitted
// Txn #1 should only read Txn #1's version of X
// Txn #2 should only read the previous version of X because Txn #1 aborted
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, AbortInsert2) {
  auto *txn0 = txn_manager_.BeginTransaction();
  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 inserts into table
  auto *insert_redo =
      txn1->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(txn1, insert_redo);

  // txn 1 inserts into index
  auto *const insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(default_index_->Insert(txn1, *insert_key, tuple_slot));

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  // txn 0 scans index and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  // txn 1 scans index and gets a visible, correct result
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Abort(txn1);

  // txn 0 scans index and gets no visible result
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index and gets no visible result
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//    W(X)   |        |        |
//    R(X)   |        |        |
//           | BEGIN  |        |
//           | R(X)   |        |
//    COMMIT |        |        |
//           | R(X)   |        |
//           | COMMIT |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read Txn #0's version of X
// Txn #1 should only read the previous version of X because its start time is before #0's commit
// Txn #2 should only read Txn #0's version of X
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, CommitUpdate1) {
  auto *insert_txn = txn_manager_.BeginTransaction();

  // insert_txn inserts into table
  auto *insert_redo =
      insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

  // insert_txn inserts into index
  auto *insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(default_index_->Insert(insert_txn, *insert_key, tuple_slot));

  txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  auto *txn0 = txn_manager_.BeginTransaction();

  // txn 0 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 0 updates in the table, which is really a delete and insert since it's an indexed attribute
  txn0->StageDelete(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, results[0]);
  EXPECT_TRUE(sql_table_->Delete(txn0, results[0]));
  default_index_->Delete(txn0, *insert_key, results[0]);

  insert_redo = txn0->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15445;
  const auto new_tuple_slot = sql_table_->Insert(txn0, insert_redo);

  insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15445;
  EXPECT_TRUE(default_index_->Insert(txn0, *insert_key, new_tuple_slot));

  // txn 1 scans index for 15721 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  // txn 1 scans index for 15445 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(new_tuple_slot, results[0]);
  results.clear();

  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 1 scans index for 15445 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  // txn 1 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 1 scans index for 15445 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index for 15721 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  // txn 2 scans index for 15445 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(new_tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//           | BEGIN  |        |
//           | W(X)   |        |
//    R(X)   |        |        |
//           | R(X)   |        |
//           | COMMIT |        |
//    R(X)   |        |        |
//    COMMIT |        |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read the previous version of X because its start time is before #1's commit
// Txn #1 should only read Txn #1's version of X
// Txn #2 should only read Txn #1's version of X
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, CommitUpdate2) {
  auto *insert_txn = txn_manager_.BeginTransaction();

  // insert_txn inserts into table
  auto *insert_redo =
      insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

  // insert_txn inserts into index
  auto *insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(default_index_->Insert(insert_txn, *insert_key, tuple_slot));

  txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  auto *txn0 = txn_manager_.BeginTransaction();
  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);

  // txn 1 updates in the table, which is really a delete and insert since it's an indexed attribute
  txn1->StageDelete(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, results[0]);
  EXPECT_TRUE(sql_table_->Delete(txn1, results[0]));
  default_index_->Delete(txn1, *insert_key, results[0]);

  insert_redo = txn1->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15445;
  const auto new_tuple_slot = sql_table_->Insert(txn1, insert_redo);

  insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15445;
  EXPECT_TRUE(default_index_->Insert(txn1, *insert_key, new_tuple_slot));

  results.clear();

  // txn 0 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 0 scans index for 15445 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  // txn 1 scans index for 15721 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  // txn 1 scans index for 15445 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(new_tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

  // txn 0 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 0 scans index for 15445 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index for 15721 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  // txn 2 scans index for 15445 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(new_tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//    W(X)   |        |        |
//    R(X)   |        |        |
//           | BEGIN  |        |
//           | R(X)   |        |
//    ABORT  |        |        |
//           | R(X)   |        |
//           | COMMIT |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read Txn #0's version of X
// Txn #1 should only read the previous version of X because Txn #0's is uncommitted
// Txn #2 should only read the previous version of X because Txn #0 aborted
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, AbortUpdate1) {
  auto *insert_txn = txn_manager_.BeginTransaction();

  // insert_txn inserts into table
  auto *insert_redo =
      insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

  // insert_txn inserts into index
  auto *insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(default_index_->Insert(insert_txn, *insert_key, tuple_slot));

  txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  auto *txn0 = txn_manager_.BeginTransaction();

  // txn 0 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 0 updates in the table, which is really a delete and insert since it's an indexed attribute
  txn0->StageDelete(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, results[0]);
  EXPECT_TRUE(sql_table_->Delete(txn0, results[0]));
  default_index_->Delete(txn0, *insert_key, results[0]);

  insert_redo = txn0->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15445;
  const auto new_tuple_slot = sql_table_->Insert(txn0, insert_redo);

  insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15445;
  EXPECT_TRUE(default_index_->Insert(txn0, *insert_key, new_tuple_slot));

  // txn 1 scans index for 15721 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  // txn 1 scans index for 15445 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(new_tuple_slot, results[0]);
  results.clear();

  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 1 scans index for 15445 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Abort(txn0);

  // txn 1 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 1 scans index for 15445 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 2 scans index for 15445 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//           | BEGIN  |        |
//           | W(X)   |        |
//    R(X)   |        |        |
//           | R(X)   |        |
//           | ABORT  |        |
//    R(X)   |        |        |
//    COMMIT |        |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read the previous version of X because Txn #1's is uncommitted
// Txn #1 should only read Txn #1's version of X
// Txn #2 should only read the previous version of X because Txn #1 aborted
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, AbortUpdate2) {
  auto *insert_txn = txn_manager_.BeginTransaction();

  // insert_txn inserts into table
  auto *insert_redo =
      insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

  // insert_txn inserts into index
  auto *insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(default_index_->Insert(insert_txn, *insert_key, tuple_slot));

  txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  auto *txn0 = txn_manager_.BeginTransaction();
  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);

  // txn 1 updates in the table, which is really a delete and insert since it's an indexed attribute
  txn1->StageDelete(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, results[0]);
  EXPECT_TRUE(sql_table_->Delete(txn1, results[0]));
  default_index_->Delete(txn1, *insert_key, results[0]);

  insert_redo = txn1->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15445;
  const auto new_tuple_slot = sql_table_->Insert(txn1, insert_redo);

  insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15445;
  EXPECT_TRUE(default_index_->Insert(txn1, *insert_key, new_tuple_slot));

  results.clear();

  // txn 0 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 0 scans index for 15445 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  // txn 1 scans index for 15721 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  // txn 1 scans index for 15445 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(new_tuple_slot, results[0]);
  results.clear();

  txn_manager_.Abort(txn1);

  // txn 0 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 0 scans index for 15445 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 2 scans index for 15445 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15445;
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//    W(X)   |        |        |
//    R(X)   |        |        |
//           | BEGIN  |        |
//           | R(X)   |        |
//    COMMIT |        |        |
//           | R(X)   |        |
//           | COMMIT |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read Txn #0's version of X
// Txn #1 should only read the previous version of X because its start time is before #0's commit
// Txn #2 should only read Txn #0's version of X
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, CommitDelete1) {
  auto *insert_txn = txn_manager_.BeginTransaction();

  // insert_txn inserts into table
  auto *insert_redo =
      insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

  // insert_txn inserts into index
  auto *insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(default_index_->Insert(insert_txn, *insert_key, tuple_slot));

  txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  auto *txn0 = txn_manager_.BeginTransaction();

  // txn 0 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 0 deletes in the table and index
  txn0->StageDelete(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, results[0]);
  EXPECT_TRUE(sql_table_->Delete(txn0, results[0]));
  default_index_->Delete(txn0, *insert_key, results[0]);

  // txn 0 scans index for 15721 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  // txn 1 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index for 15721 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//           | BEGIN  |        |
//           | W(X)   |        |
//    R(X)   |        |        |
//           | R(X)   |        |
//           | COMMIT |        |
//    R(X)   |        |        |
//    COMMIT |        |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read the previous version of X because its start time is before #1's commit
// Txn #1 should only read Txn #1's version of X
// Txn #2 should only read Txn #1's version of X
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, CommitDelete2) {
  auto *insert_txn = txn_manager_.BeginTransaction();

  // insert_txn inserts into table
  auto *insert_redo =
      insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

  // insert_txn inserts into index
  auto *insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(default_index_->Insert(insert_txn, *insert_key, tuple_slot));

  txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  auto *txn0 = txn_manager_.BeginTransaction();
  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);

  // txn 1 deletes in the table and index
  txn1->StageDelete(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, results[0]);
  EXPECT_TRUE(sql_table_->Delete(txn1, results[0]));
  default_index_->Delete(txn1, *insert_key, results[0]);

  results.clear();

  // txn 0 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 1 scans index for 15721 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

  // txn 0 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index for 15721 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//    W(X)   |        |        |
//    R(X)   |        |        |
//           | BEGIN  |        |
//           | R(X)   |        |
//    ABORT  |        |        |
//           | R(X)   |        |
//           | COMMIT |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read Txn #0's version of X
// Txn #1 should only read the previous version of X because Txn #0's is uncommitted
// Txn #2 should only read the previous version of X because Txn #0 aborted
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, AbortDelete1) {
  auto *insert_txn = txn_manager_.BeginTransaction();

  // insert_txn inserts into table
  auto *insert_redo =
      insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

  // insert_txn inserts into index
  auto *insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(default_index_->Insert(insert_txn, *insert_key, tuple_slot));

  txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  auto *txn0 = txn_manager_.BeginTransaction();

  // txn 0 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 0 deletes in the table and index
  txn0->StageDelete(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, results[0]);
  EXPECT_TRUE(sql_table_->Delete(txn0, results[0]));
  default_index_->Delete(txn0, *insert_key, results[0]);

  // txn 0 scans index for 15721 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Abort(txn0);

  // txn 1 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//           | BEGIN  |        |
//           | W(X)   |        |
//    R(X)   |        |        |
//           | R(X)   |        |
//           | ABORT  |        |
//    R(X)   |        |        |
//    COMMIT |        |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read the previous version of X because Txn #1's is uncommitted
// Txn #1 should only read Txn #1's version of X
// Txn #2 should only read the previous version of X because Txn #1 aborted
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, AbortDelete2) {
  auto *insert_txn = txn_manager_.BeginTransaction();

  // insert_txn inserts into table
  auto *insert_redo =
      insert_txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer_);
  auto *insert_tuple = insert_redo->Delta();
  *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;
  const auto tuple_slot = sql_table_->Insert(insert_txn, insert_redo);

  // insert_txn inserts into index
  auto *insert_key = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = 15721;
  EXPECT_TRUE(default_index_->Insert(insert_txn, *insert_key, tuple_slot));

  txn_manager_.Commit(insert_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  std::vector<storage::TupleSlot> results;

  auto *const scan_key_pr = default_index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);

  auto *txn0 = txn_manager_.BeginTransaction();
  auto *txn1 = txn_manager_.BeginTransaction();

  // txn 1 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);

  // txn 1 deletes in the table and index
  txn1->StageDelete(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, results[0]);
  EXPECT_TRUE(sql_table_->Delete(txn1, results[0]));
  default_index_->Delete(txn1, *insert_key, results[0]);

  results.clear();

  // txn 0 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  // txn 1 scans index for 15721 and gets no visible result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn1, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 0);
  results.clear();

  txn_manager_.Abort(txn1);

  // txn 0 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn0, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto *txn2 = txn_manager_.BeginTransaction();

  // txn 2 scans index for 15721 and gets a visible, correct result
  *reinterpret_cast<int32_t *>(scan_key_pr->AccessForceNotNull(0)) = 15721;
  default_index_->ScanKey(*txn2, *scan_key_pr, &results);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(tuple_slot, results[0]);
  results.clear();

  txn_manager_.Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
}

}  // namespace terrier::storage::index
