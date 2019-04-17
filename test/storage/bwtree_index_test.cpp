#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <random>
#include <vector>
#include "portable_endian/portable_endian.h"
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
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{10000, 10000};
  const catalog::Schema table_schema_{
      catalog::Schema({{"attribute", type::TypeId::INTEGER, false, catalog::col_oid_t(0)}})};
  const IndexKeySchema key_schema_{{catalog::indexkeycol_oid_t(0), type::TypeId::INTEGER, false}};

 public:
  std::default_random_engine generator_;

  // SqlTable
  storage::SqlTable *const sql_table_{new storage::SqlTable(&block_store_, table_schema_, catalog::table_oid_t(1))};
  const storage::ProjectedRowInitializer tuple_initializer_{
      sql_table_->InitializerForProjectedRow({catalog::col_oid_t(0)}).first};

  // BwTreeIndex
  Index *index_;
  transaction::TransactionManager txn_manager_{&buffer_pool_, false, LOGGING_DISABLED};

  byte *insert_buffer_, *key_buffer_1_, *key_buffer_2_;

 protected:
  void SetUp() override {
    TerrierTest::SetUp();
    index_ = (IndexBuilder()
                  .SetConstraintType(ConstraintType::DEFAULT)
                  .SetKeySchema(key_schema_)
                  .SetOid(catalog::index_oid_t(2)))
                 .Build();
    insert_buffer_ = common::AllocationUtil::AllocateAligned(index_->GetProjectedRowInitializer().ProjectedRowSize());
    key_buffer_1_ = common::AllocationUtil::AllocateAligned(index_->GetProjectedRowInitializer().ProjectedRowSize());
    key_buffer_2_ = common::AllocationUtil::AllocateAligned(index_->GetProjectedRowInitializer().ProjectedRowSize());
  }
  void TearDown() override {
    delete sql_table_;
    TerrierTest::TearDown();
  }
};

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
    auto *const insert_tuple = tuple_initializer_.InitializeRow(insert_buffer_);
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
    const auto tuple_slot = sql_table_->Insert(insert_txn, *insert_tuple);

    auto *const insert_key = index_->GetProjectedRowInitializer().InitializeRow(insert_buffer_);
    *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
    EXPECT_TRUE(index_->Insert(*insert_key, tuple_slot));
    reference[i] = tuple_slot;
  }
  txn_manager_.Commit(insert_txn, TestCallbacks::EmptyCallback, nullptr);

  auto *const scan_txn = txn_manager_.BeginTransaction();

  std::vector<storage::TupleSlot> results;

  auto *const low_key_pr = index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  auto *const high_key_pr = index_->GetProjectedRowInitializer().InitializeRow(key_buffer_2_);

  // scan[8,12] should hit keys 8, 10, 12
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 8;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 12;
  index_->ScanAscending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(8), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  EXPECT_EQ(reference.at(12), results[2]);
  results.clear();

  // scan[7,13] should hit keys 8, 10, 12
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 7;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 13;
  index_->ScanAscending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(8), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  EXPECT_EQ(reference.at(12), results[2]);
  results.clear();

  // scan[-1,5] should hit keys 0, 2, 4
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = -1;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 5;
  index_->ScanAscending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(0), results[0]);
  EXPECT_EQ(reference.at(2), results[1]);
  EXPECT_EQ(reference.at(4), results[2]);
  results.clear();

  // scan[15,21] should hit keys 16, 18, 20
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 21;
  index_->ScanAscending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(16), results[0]);
  EXPECT_EQ(reference.at(18), results[1]);
  EXPECT_EQ(reference.at(20), results[2]);
  results.clear();

  txn_manager_.Commit(scan_txn, TestCallbacks::EmptyCallback, nullptr);

  // Clean up
  delete insert_txn;
  delete scan_txn;
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
    auto *const insert_tuple = tuple_initializer_.InitializeRow(insert_buffer_);
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
    const auto tuple_slot = sql_table_->Insert(insert_txn, *insert_tuple);

    auto *const insert_key = index_->GetProjectedRowInitializer().InitializeRow(insert_buffer_);
    *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
    EXPECT_TRUE(index_->Insert(*insert_key, tuple_slot));
    reference[i] = tuple_slot;
  }
  txn_manager_.Commit(insert_txn, TestCallbacks::EmptyCallback, nullptr);

  auto *const scan_txn = txn_manager_.BeginTransaction();

  std::vector<storage::TupleSlot> results;

  auto *const low_key_pr = index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  auto *const high_key_pr = index_->GetProjectedRowInitializer().InitializeRow(key_buffer_2_);

  // scan[8,12] should hit keys 12, 10, 8
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 8;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 12;
  index_->ScanDescending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(12), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  EXPECT_EQ(reference.at(8), results[2]);
  results.clear();

  // scan[7,13] should hit keys 12, 10, 8
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 7;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 13;
  index_->ScanDescending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(12), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  EXPECT_EQ(reference.at(8), results[2]);
  results.clear();

  // scan[-1,5] should hit keys 4, 2, 0
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = -1;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 5;
  index_->ScanDescending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(4), results[0]);
  EXPECT_EQ(reference.at(2), results[1]);
  EXPECT_EQ(reference.at(0), results[2]);
  results.clear();

  // scan[15,21] should hit keys 20, 18, 16
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 21;
  index_->ScanDescending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(20), results[0]);
  EXPECT_EQ(reference.at(18), results[1]);
  EXPECT_EQ(reference.at(16), results[2]);
  results.clear();

  txn_manager_.Commit(scan_txn, TestCallbacks::EmptyCallback, nullptr);

  // Clean up
  delete insert_txn;
  delete scan_txn;
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
    auto *const insert_tuple = tuple_initializer_.InitializeRow(insert_buffer_);
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
    const auto tuple_slot = sql_table_->Insert(insert_txn, *insert_tuple);

    auto *const insert_key = index_->GetProjectedRowInitializer().InitializeRow(insert_buffer_);
    *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
    EXPECT_TRUE(index_->Insert(*insert_key, tuple_slot));
    reference[i] = tuple_slot;
  }
  txn_manager_.Commit(insert_txn, TestCallbacks::EmptyCallback, nullptr);

  auto *const scan_txn = txn_manager_.BeginTransaction();

  std::vector<storage::TupleSlot> results;

  auto *const low_key_pr = index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  auto *const high_key_pr = index_->GetProjectedRowInitializer().InitializeRow(key_buffer_2_);

  // scan_limit[8,12] should hit keys 8, 10
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 8;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 12;
  index_->ScanLimitAscending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(8), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  results.clear();

  // scan_limit[7,13] should hit keys 8, 10
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 7;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 13;
  index_->ScanLimitAscending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(8), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  results.clear();

  // scan_limit[-1,5] should hit keys 0, 2
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = -1;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 5;
  index_->ScanLimitAscending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(0), results[0]);
  EXPECT_EQ(reference.at(2), results[1]);
  results.clear();

  // scan_limit[15,21] should hit keys 16, 18
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 21;
  index_->ScanLimitAscending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(16), results[0]);
  EXPECT_EQ(reference.at(18), results[1]);
  results.clear();

  txn_manager_.Commit(scan_txn, TestCallbacks::EmptyCallback, nullptr);

  // Clean up
  delete insert_txn;
  delete scan_txn;
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
    auto *const insert_tuple = tuple_initializer_.InitializeRow(insert_buffer_);
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
    const auto tuple_slot = sql_table_->Insert(insert_txn, *insert_tuple);

    auto *const insert_key = index_->GetProjectedRowInitializer().InitializeRow(insert_buffer_);
    *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
    EXPECT_TRUE(index_->Insert(*insert_key, tuple_slot));
    reference[i] = tuple_slot;
  }
  txn_manager_.Commit(insert_txn, TestCallbacks::EmptyCallback, nullptr);

  auto *const scan_txn = txn_manager_.BeginTransaction();

  std::vector<storage::TupleSlot> results;

  auto *const low_key_pr = index_->GetProjectedRowInitializer().InitializeRow(key_buffer_1_);
  auto *const high_key_pr = index_->GetProjectedRowInitializer().InitializeRow(key_buffer_2_);

  // scan_limit[8,12] should hit keys 12, 10
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 8;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 12;
  index_->ScanLimitDescending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(12), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  results.clear();

  // scan_limit[7,13] should hit keys 12, 10
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 7;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 13;
  index_->ScanLimitDescending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(12), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  results.clear();

  // scan_limit[-1,5] should hit keys 4, 2
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = -1;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 5;
  index_->ScanLimitDescending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(4), results[0]);
  EXPECT_EQ(reference.at(2), results[1]);
  results.clear();

  // scan_limit[15,21] should hit keys 20, 18
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 21;
  index_->ScanLimitDescending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(20), results[0]);
  EXPECT_EQ(reference.at(18), results[1]);
  results.clear();

  txn_manager_.Commit(scan_txn, TestCallbacks::EmptyCallback, nullptr);

  // Clean up
  delete insert_txn;
  delete scan_txn;
}

}  // namespace terrier::storage::index
