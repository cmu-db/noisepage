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
 public:
  std::default_random_engine generator_;
  std::vector<byte *> loose_pointers_;

 protected:
  void TearDown() override {
    for (byte *ptr : loose_pointers_) {
      delete[] ptr;
    }
    TerrierTest::TearDown();
  }
};

/**
 * Tests basic scan behavior using various windows to scan over (some out of of bounds of keyspace, some matching
 * exactly, etc.)
 */
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, ScanAscending) {
  // Create a table
  storage::BlockStore block_store{100, 100};
  storage::RecordBufferSegmentPool buffer_pool{10000, 10000};
  std::vector<catalog::Schema::Column> columns;
  columns.emplace_back("attribute", type::TypeId ::INTEGER, false, catalog::col_oid_t(0));
  catalog::Schema schema{columns};
  storage::SqlTable sql_table(&block_store, schema, catalog::table_oid_t(1));
  const auto &tuple_initializer = sql_table.InitializerForProjectedRow({catalog::col_oid_t(0)}).first;

  // Create an index
  IndexKeySchema key_schema;
  key_schema.emplace_back(catalog::indexkeycol_oid_t(0), type::TypeId::INTEGER, false);
  IndexBuilder builder;
  builder.SetConstraintType(ConstraintType::DEFAULT).SetKeySchema(key_schema).SetOid(catalog::index_oid_t(2));
  auto *index = builder.Build();
  const auto &key_initializer = index->GetProjectedRowInitializer();

  transaction::TransactionManager txn_manager(&buffer_pool, false, LOGGING_DISABLED);

  // populate index with [0..20] even keys
  std::map<int32_t, storage::TupleSlot> reference;
  auto *const insert_txn = txn_manager.BeginTransaction();
  auto *const insert_buffer = common::AllocationUtil::AllocateAligned(key_initializer.ProjectedRowSize());
  for (int32_t i = 0; i <= 20; i += 2) {
    auto *const insert_tuple = tuple_initializer.InitializeRow(insert_buffer);
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
    const auto tuple_slot = sql_table.Insert(insert_txn, *insert_tuple);

    auto *const insert_key = key_initializer.InitializeRow(insert_buffer);
    *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
    EXPECT_TRUE(index->Insert(*insert_key, tuple_slot));
    reference[i] = tuple_slot;
  }
  txn_manager.Commit(insert_txn, TestCallbacks::EmptyCallback, nullptr);

  auto *const scan_txn = txn_manager.BeginTransaction();

  std::vector<storage::TupleSlot> results;

  auto *const low_key_buffer = common::AllocationUtil::AllocateAligned(key_initializer.ProjectedRowSize());
  auto *const low_key_pr = key_initializer.InitializeRow(low_key_buffer);
  auto *const high_key_buffer = common::AllocationUtil::AllocateAligned(key_initializer.ProjectedRowSize());
  auto *const high_key_pr = key_initializer.InitializeRow(high_key_buffer);

  // scan[8,12] should hit keys 8, 10, 12
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 8;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 12;
  index->ScanAscending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(8), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  EXPECT_EQ(reference.at(12), results[2]);
  results.clear();

  // scan[7,13] should hit keys 8, 10, 12
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 7;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 13;
  index->ScanAscending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(8), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  EXPECT_EQ(reference.at(12), results[2]);
  results.clear();

  // scan[-1,5] should hit keys 0, 2, 4
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = -1;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 5;
  index->ScanAscending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(0), results[0]);
  EXPECT_EQ(reference.at(2), results[1]);
  EXPECT_EQ(reference.at(4), results[2]);
  results.clear();

  // scan[15,21] should hit keys 16, 18, 20
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 21;
  index->ScanAscending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(16), results[0]);
  EXPECT_EQ(reference.at(18), results[1]);
  EXPECT_EQ(reference.at(20), results[2]);
  results.clear();

  txn_manager.Commit(scan_txn, TestCallbacks::EmptyCallback, nullptr);

  // Clean up
  delete[] insert_buffer;
  delete[] low_key_buffer;
  delete[] high_key_buffer;
  delete index;
  delete insert_txn;
  delete scan_txn;
}

/**
 * Tests basic scan behavior using various windows to scan over (some out of of bounds of keyspace, some matching
 * exactly, etc.)
 */
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, ScanDescending) {
  // Create a table
  storage::BlockStore block_store{100, 100};
  storage::RecordBufferSegmentPool buffer_pool{10000, 10000};
  std::vector<catalog::Schema::Column> columns;
  columns.emplace_back("attribute", type::TypeId ::INTEGER, false, catalog::col_oid_t(0));
  catalog::Schema schema{columns};
  storage::SqlTable sql_table(&block_store, schema, catalog::table_oid_t(1));
  const auto &tuple_initializer = sql_table.InitializerForProjectedRow({catalog::col_oid_t(0)}).first;

  // Create an index
  IndexKeySchema key_schema;
  key_schema.emplace_back(catalog::indexkeycol_oid_t(0), type::TypeId::INTEGER, false);
  IndexBuilder builder;
  builder.SetConstraintType(ConstraintType::DEFAULT).SetKeySchema(key_schema).SetOid(catalog::index_oid_t(2));
  auto *index = builder.Build();
  const auto &key_initializer = index->GetProjectedRowInitializer();

  transaction::TransactionManager txn_manager(&buffer_pool, false, LOGGING_DISABLED);

  // populate index with [0..20] even keys
  std::map<int32_t, storage::TupleSlot> reference;
  auto *const insert_txn = txn_manager.BeginTransaction();
  auto *const insert_buffer = common::AllocationUtil::AllocateAligned(key_initializer.ProjectedRowSize());
  for (int32_t i = 0; i <= 20; i += 2) {
    auto *const insert_tuple = tuple_initializer.InitializeRow(insert_buffer);
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
    const auto tuple_slot = sql_table.Insert(insert_txn, *insert_tuple);

    auto *const insert_key = key_initializer.InitializeRow(insert_buffer);
    *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
    EXPECT_TRUE(index->Insert(*insert_key, tuple_slot));
    reference[i] = tuple_slot;
  }
  txn_manager.Commit(insert_txn, TestCallbacks::EmptyCallback, nullptr);

  auto *const scan_txn = txn_manager.BeginTransaction();

  std::vector<storage::TupleSlot> results;

  auto *const low_key_buffer = common::AllocationUtil::AllocateAligned(key_initializer.ProjectedRowSize());
  auto *const low_key_pr = key_initializer.InitializeRow(low_key_buffer);
  auto *const high_key_buffer = common::AllocationUtil::AllocateAligned(key_initializer.ProjectedRowSize());
  auto *const high_key_pr = key_initializer.InitializeRow(high_key_buffer);

  // scan[8,12] should hit keys 12, 10, 8
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 8;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 12;
  index->ScanDescending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(12), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  EXPECT_EQ(reference.at(8), results[2]);
  results.clear();

  // scan[7,13] should hit keys 12, 10, 8
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 7;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 13;
  index->ScanDescending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(12), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  EXPECT_EQ(reference.at(8), results[2]);
  results.clear();

  // scan[-1,5] should hit keys 4, 2, 0
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = -1;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 5;
  index->ScanDescending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(4), results[0]);
  EXPECT_EQ(reference.at(2), results[1]);
  EXPECT_EQ(reference.at(0), results[2]);
  results.clear();

  // scan[15,21] should hit keys 20, 18, 16
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 21;
  index->ScanDescending(*scan_txn, *low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(reference.at(20), results[0]);
  EXPECT_EQ(reference.at(18), results[1]);
  EXPECT_EQ(reference.at(16), results[2]);
  results.clear();

  txn_manager.Commit(scan_txn, TestCallbacks::EmptyCallback, nullptr);

  // Clean up
  delete[] insert_buffer;
  delete[] low_key_buffer;
  delete[] high_key_buffer;
  delete index;
  delete insert_txn;
  delete scan_txn;
}

/**
 * Tests basic scan behavior using various windows to scan over (some out of of bounds of keyspace, some matching
 * exactly, etc.)
 */
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, ScanLimitAscending) {
  // Create a table
  storage::BlockStore block_store{100, 100};
  storage::RecordBufferSegmentPool buffer_pool{10000, 10000};
  std::vector<catalog::Schema::Column> columns;
  columns.emplace_back("attribute", type::TypeId ::INTEGER, false, catalog::col_oid_t(0));
  catalog::Schema schema{columns};
  storage::SqlTable sql_table(&block_store, schema, catalog::table_oid_t(1));
  const auto &tuple_initializer = sql_table.InitializerForProjectedRow({catalog::col_oid_t(0)}).first;

  // Create an index
  IndexKeySchema key_schema;
  key_schema.emplace_back(catalog::indexkeycol_oid_t(0), type::TypeId::INTEGER, false);
  IndexBuilder builder;
  builder.SetConstraintType(ConstraintType::DEFAULT).SetKeySchema(key_schema).SetOid(catalog::index_oid_t(2));
  auto *index = builder.Build();
  const auto &key_initializer = index->GetProjectedRowInitializer();

  transaction::TransactionManager txn_manager(&buffer_pool, false, LOGGING_DISABLED);

  // populate index with [0..20] even keys
  std::map<int32_t, storage::TupleSlot> reference;
  auto *const insert_txn = txn_manager.BeginTransaction();
  auto *const insert_buffer = common::AllocationUtil::AllocateAligned(key_initializer.ProjectedRowSize());
  for (int32_t i = 0; i <= 20; i += 2) {
    auto *const insert_tuple = tuple_initializer.InitializeRow(insert_buffer);
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
    const auto tuple_slot = sql_table.Insert(insert_txn, *insert_tuple);

    auto *const insert_key = key_initializer.InitializeRow(insert_buffer);
    *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
    EXPECT_TRUE(index->Insert(*insert_key, tuple_slot));
    reference[i] = tuple_slot;
  }
  txn_manager.Commit(insert_txn, TestCallbacks::EmptyCallback, nullptr);

  auto *const scan_txn = txn_manager.BeginTransaction();

  std::vector<storage::TupleSlot> results;

  auto *const low_key_buffer = common::AllocationUtil::AllocateAligned(key_initializer.ProjectedRowSize());
  auto *const low_key_pr = key_initializer.InitializeRow(low_key_buffer);
  auto *const high_key_buffer = common::AllocationUtil::AllocateAligned(key_initializer.ProjectedRowSize());
  auto *const high_key_pr = key_initializer.InitializeRow(high_key_buffer);

  // scan_limit[8,12] should hit keys 8, 10
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 8;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 12;
  index->ScanLimitAscending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(8), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  results.clear();

  // scan_limit[7,13] should hit keys 8, 10
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 7;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 13;
  index->ScanLimitAscending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(8), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  results.clear();

  // scan_limit[-1,5] should hit keys 0, 2
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = -1;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 5;
  index->ScanLimitAscending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(0), results[0]);
  EXPECT_EQ(reference.at(2), results[1]);
  results.clear();

  // scan_limit[15,21] should hit keys 16, 18
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 21;
  index->ScanLimitAscending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(16), results[0]);
  EXPECT_EQ(reference.at(18), results[1]);
  results.clear();

  txn_manager.Commit(scan_txn, TestCallbacks::EmptyCallback, nullptr);

  // Clean up
  delete[] insert_buffer;
  delete[] low_key_buffer;
  delete[] high_key_buffer;
  delete index;
  delete insert_txn;
  delete scan_txn;
}

/**
 * Tests basic scan behavior using various windows to scan over (some out of of bounds of keyspace, some matching
 * exactly, etc.)
 */
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, ScanLimitDescending) {
  // Create a table
  storage::BlockStore block_store{100, 100};
  storage::RecordBufferSegmentPool buffer_pool{10000, 10000};
  std::vector<catalog::Schema::Column> columns;
  columns.emplace_back("attribute", type::TypeId ::INTEGER, false, catalog::col_oid_t(0));
  catalog::Schema schema{columns};
  storage::SqlTable sql_table(&block_store, schema, catalog::table_oid_t(1));
  const auto &tuple_initializer = sql_table.InitializerForProjectedRow({catalog::col_oid_t(0)}).first;

  // Create an index
  IndexKeySchema key_schema;
  key_schema.emplace_back(catalog::indexkeycol_oid_t(0), type::TypeId::INTEGER, false);
  IndexBuilder builder;
  builder.SetConstraintType(ConstraintType::DEFAULT).SetKeySchema(key_schema).SetOid(catalog::index_oid_t(2));
  auto *index = builder.Build();
  const auto &key_initializer = index->GetProjectedRowInitializer();

  transaction::TransactionManager txn_manager(&buffer_pool, false, LOGGING_DISABLED);

  // populate index with [0..20] even keys
  std::map<int32_t, storage::TupleSlot> reference;
  auto *const insert_txn = txn_manager.BeginTransaction();
  auto *const insert_buffer = common::AllocationUtil::AllocateAligned(key_initializer.ProjectedRowSize());
  for (int32_t i = 0; i <= 20; i += 2) {
    auto *const insert_tuple = tuple_initializer.InitializeRow(insert_buffer);
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
    const auto tuple_slot = sql_table.Insert(insert_txn, *insert_tuple);

    auto *const insert_key = key_initializer.InitializeRow(insert_buffer);
    *reinterpret_cast<int32_t *>(insert_key->AccessForceNotNull(0)) = i;
    EXPECT_TRUE(index->Insert(*insert_key, tuple_slot));
    reference[i] = tuple_slot;
  }
  txn_manager.Commit(insert_txn, TestCallbacks::EmptyCallback, nullptr);

  auto *const scan_txn = txn_manager.BeginTransaction();

  std::vector<storage::TupleSlot> results;

  auto *const low_key_buffer = common::AllocationUtil::AllocateAligned(key_initializer.ProjectedRowSize());
  auto *const low_key_pr = key_initializer.InitializeRow(low_key_buffer);
  auto *const high_key_buffer = common::AllocationUtil::AllocateAligned(key_initializer.ProjectedRowSize());
  auto *const high_key_pr = key_initializer.InitializeRow(high_key_buffer);

  // scan_limit[8,12] should hit keys 12, 10
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 8;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 12;
  index->ScanLimitDescending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(12), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  results.clear();

  // scan_limit[7,13] should hit keys 12, 10
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 7;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 13;
  index->ScanLimitDescending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(12), results[0]);
  EXPECT_EQ(reference.at(10), results[1]);
  results.clear();

  // scan_limit[-1,5] should hit keys 4, 2
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = -1;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 5;
  index->ScanLimitDescending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(4), results[0]);
  EXPECT_EQ(reference.at(2), results[1]);
  results.clear();

  // scan_limit[15,21] should hit keys 20, 18
  *reinterpret_cast<int32_t *>(low_key_pr->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<int32_t *>(high_key_pr->AccessForceNotNull(0)) = 21;
  index->ScanLimitDescending(*scan_txn, *low_key_pr, *high_key_pr, &results, 2);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(reference.at(20), results[0]);
  EXPECT_EQ(reference.at(18), results[1]);
  results.clear();

  txn_manager.Commit(scan_txn, TestCallbacks::EmptyCallback, nullptr);

  // Clean up
  delete[] insert_buffer;
  delete[] low_key_buffer;
  delete[] high_key_buffer;
  delete index;
  delete insert_txn;
  delete scan_txn;
}

}  // namespace terrier::storage::index
