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

  /**
   * Generates random data for the given type and writes it to both attr and reference.
   */
  template <typename Random>
  void WriteRandomAttribute(const IndexKeyColumn &col, void *attr, void *reference, Random *generator) {
    std::uniform_int_distribution<int64_t> rng(std::numeric_limits<int64_t>::min(),
                                               std::numeric_limits<int64_t>::max());
    const auto type = col.GetType();
    const auto type_size = type::TypeUtil::GetTypeSize(type);

    // note that for memcmp to work, signed integers must have their sign flipped and converted to big endian

    switch (type) {
      case type::TypeId::BOOLEAN: {
        auto boolean = static_cast<uint8_t>(rng(*generator)) % 2;
        std::memcpy(attr, &boolean, type_size);
        std::memcpy(reference, &boolean, type_size);
        break;
      }
      case type::TypeId::TINYINT: {
        auto tinyint = static_cast<int8_t>(rng(*generator));
        std::memcpy(attr, &tinyint, type_size);
        tinyint ^= static_cast<int8_t>(static_cast<int8_t>(0x1) << (sizeof(int8_t) * 8UL - 1));
        std::memcpy(reference, &tinyint, type_size);
        break;
      }
      case type::TypeId::SMALLINT: {
        auto smallint = static_cast<int16_t>(rng(*generator));
        std::memcpy(attr, &smallint, type_size);
        smallint ^= static_cast<int16_t>(static_cast<int16_t>(0x1) << (sizeof(int16_t) * 8UL - 1));
        smallint = htobe16(smallint);
        std::memcpy(reference, &smallint, type_size);
        break;
      }
      case type::TypeId::INTEGER: {
        auto integer = static_cast<int32_t>(rng(*generator));
        std::memcpy(attr, &integer, type_size);
        integer ^= static_cast<int32_t>(static_cast<int32_t>(0x1) << (sizeof(int32_t) * 8UL - 1));
        integer = htobe32(integer);
        std::memcpy(reference, &integer, type_size);
        break;
      }
      case type::TypeId::DATE: {
        auto date = static_cast<uint32_t>(rng(*generator));
        std::memcpy(attr, &date, type_size);
        date = htobe32(date);
        std::memcpy(reference, &date, type_size);
        break;
      }
      case type::TypeId::BIGINT: {
        auto bigint = static_cast<int64_t>(rng(*generator));
        std::memcpy(attr, &bigint, type_size);
        bigint ^= static_cast<int64_t>(static_cast<int64_t>(0x1) << (sizeof(int64_t) * 8UL - 1));
        bigint = htobe64(bigint);
        std::memcpy(reference, &bigint, type_size);
        break;
      }
      case type::TypeId::DECIMAL: {
        auto decimal = static_cast<int64_t>(rng(*generator));
        std::memcpy(attr, &decimal, type_size);
        decimal ^= static_cast<int64_t>(static_cast<int64_t>(0x1) << (sizeof(int64_t) * 8UL - 1));
        decimal = htobe64(decimal);
        std::memcpy(reference, &decimal, type_size);
        break;
      }
      case type::TypeId::TIMESTAMP: {
        auto timestamp = static_cast<uint64_t>(rng(*generator));
        std::memcpy(attr, &timestamp, type_size);
        timestamp = htobe64(timestamp);
        std::memcpy(reference, &timestamp, type_size);
        break;
      }
      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY: {
        // pick a random varlen size, meant to hit the {inline (prefix), inline (prefix+content), content} cases
        auto varlen_size = col.GetMaxVarlenSize();

        // generate random varlen content
        auto *varlen_content = new byte[varlen_size];
        uint8_t bytes_copied = 0;
        while (bytes_copied < varlen_size) {
          auto random_content = static_cast<int64_t>(rng(*generator));
          uint8_t copy_amount =
              std::min(static_cast<uint8_t>(sizeof(int64_t)), static_cast<uint8_t>(varlen_size - bytes_copied));
          std::memcpy(varlen_content + bytes_copied, &random_content, copy_amount);
          bytes_copied = static_cast<uint8_t>(bytes_copied + copy_amount);
        }

        // write the varlen content into a varlen entry, inlining if appropriate
        VarlenEntry varlen_entry{};
        if (varlen_size <= VarlenEntry::InlineThreshold()) {
          varlen_entry = VarlenEntry::CreateInline(varlen_content, varlen_size);
        } else {
          varlen_entry = VarlenEntry::Create(varlen_content, varlen_size, false);
        }
        loose_pointers_.emplace_back(varlen_content);

        // copy the varlen entry into our attribute and reference
        std::memcpy(attr, &varlen_entry, sizeof(VarlenEntry));
        std::memcpy(reference, &varlen_entry, sizeof(VarlenEntry));
        break;
      }
      default:
        throw std::runtime_error("Unsupported type");
    }
  }

  /**
   * This function randomly generates data per the schema to fill the projected row.
   * It returns essentially a CompactIntsKey of all the data in one big byte array.
   */
  template <typename Random>
  byte *FillProjectedRow(const IndexMetadata &metadata, storage::ProjectedRow *pr, Random *generator) {
    const auto &key_schema = metadata.GetKeySchema();
    const auto &oid_offset_map = metadata.GetKeyOidToOffsetMap();
    const auto key_size = std::accumulate(metadata.GetAttributeSizes().begin(), metadata.GetAttributeSizes().end(), 0);

    auto *reference = new byte[key_size];
    uint32_t offset = 0;
    for (const auto &key : key_schema) {
      auto key_oid = key.GetOid();
      auto key_type = key.GetType();
      auto pr_offset = static_cast<uint16_t>(oid_offset_map.at(key_oid));
      auto attr = pr->AccessForceNotNull(pr_offset);
      WriteRandomAttribute(key, attr, reference + offset, generator);
      offset += type::TypeUtil::GetTypeSize(key_type);
    }
    return reference;
  }

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
