#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <random>
#include <vector>

#include "catalog/index_schema.h"
#include "main/db_main.h"
#include "portable_endian/portable_endian.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_key.h"
#include "storage/index/index.h"
#include "storage/index/index_builder.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "test_util/catalog_test_util.h"
#include "test_util/data_table_test_util.h"
#include "test_util/random_test_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace noisepage::storage::index {

class IndexKeyTests : public TerrierTest {
 public:
  std::default_random_engine generator_;
  std::vector<byte *> loose_pointers_;

  /**
   * Generates random data for the given type and writes it to both attr and reference.
   */
  template <typename Random>
  void WriteRandomAttribute(const catalog::IndexSchema::Column &col, void *attr, void *reference, Random *generator) {
    std::uniform_int_distribution<int64_t> rng(std::numeric_limits<int64_t>::min(),
                                               std::numeric_limits<int64_t>::max());
    const auto type = col.Type();
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
        auto varlen_size = col.MaxVarlenSize();

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
    const auto &key_schema = metadata.GetSchema();
    const auto &oid_offset_map = metadata.GetKeyOidToOffsetMap();
    const auto key_size = metadata.KeySize();

    auto *reference = new byte[key_size];
    uint32_t offset = 0;
    for (const auto &key : key_schema.GetColumns()) {
      auto key_oid = key.Oid();
      auto key_type = key.Type();
      auto pr_offset = static_cast<uint16_t>(oid_offset_map.at(key_oid));
      auto attr = pr->AccessForceNotNull(pr_offset);
      WriteRandomAttribute(key, attr, reference + offset, generator);
      offset += AttrSizeBytes(type::TypeUtil::GetTypeSize(key_type));
    }
    return reference;
  }

  /**
   * Strictly speaking, this function is a less general version of FillProjectedRow above.
   * But it is easier to reason about and useful in a debugger, so we keep it around.
   */
  template <typename Random>
  std::vector<int64_t> FillProjectedRowWithRandomCompactInts(const IndexMetadata &metadata, storage::ProjectedRow *pr,
                                                             Random *generator) {
    std::uniform_int_distribution<int64_t> rng(std::numeric_limits<int64_t>::min(),
                                               std::numeric_limits<int64_t>::max());
    const auto &key_schema = metadata.GetSchema();
    const auto &oid_offset_map = metadata.GetKeyOidToOffsetMap();

    std::vector<int64_t> data;
    data.reserve(key_schema.GetColumns().size());
    for (const auto &key : key_schema.GetColumns()) {
      auto key_type = key.Type();
      const auto type_size = type::TypeUtil::GetTypeSize(key_type);
      int64_t rand_int;

      switch (key_type) {
        case type::TypeId::TINYINT:
          rand_int = static_cast<int64_t>(static_cast<int8_t>(rng(*generator)));
          break;
        case type::TypeId::SMALLINT:
          rand_int = static_cast<int64_t>(static_cast<int16_t>(rng(*generator)));
          break;
        case type::TypeId::INTEGER:
          rand_int = static_cast<int64_t>(static_cast<int32_t>(rng(*generator)));
          break;
        case type::TypeId::BIGINT:
          rand_int = static_cast<int64_t>(static_cast<int64_t>(rng(*generator)));
          break;
        default:
          throw std::runtime_error("Invalid compact ints key schema.");
      }

      auto pr_offset = static_cast<uint16_t>(oid_offset_map.at(key.Oid()));
      auto attr = pr->AccessForceNotNull(pr_offset);
      std::memcpy(attr, &rand_int, type_size);
      data.emplace_back(rand_int);
    }
    return data;
  }

  /**
   * Tests:
   * 1. Scan -> Insert -> Scan -> Delete -> Scan
   * 2. If primary key or unique index, 2 x Conditional Insert -> Scan -> Delete -> Scan
   */
  void BasicOps(Index *const index) {
    // Create a table. Note that this schema does not match the index's randomly generated schema, and what we're
    // inserting doesn't match either. We just need valid, visible TupleSlots to reference as values for all of the
    // index keys.

    auto db_main = DBMain::Builder().SetUseGC(true).Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

    std::vector<catalog::Schema::Column> columns;
    columns.emplace_back("attribute", type::TypeId ::INTEGER, false,
                         parser::ConstantValueExpression(type::TypeId::INTEGER));
    catalog::Schema schema{columns};
    auto *sql_table = new storage::SqlTable(db_main->GetStorageLayer()->GetBlockStore(), schema);
    const auto &tuple_initializer = sql_table->InitializerForProjectedRow({catalog::col_oid_t(0)});

    auto *const txn = txn_manager->BeginTransaction();

    // dummy tuple to insert for each key. We just need the visible TupleSlot
    auto *const insert_redo =
        txn->StageWrite(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, tuple_initializer);
    auto *const insert_tuple = insert_redo->Delta();
    *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = 15721;

    // instantiate projected row and key
    const auto &metadata = index->metadata_;
    const auto &initializer = metadata.GetProjectedRowInitializer();
    auto *key_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *key = initializer.InitializeRow(key_buffer);

    auto *ref = FillProjectedRow(metadata, key, &generator_);
    delete[] ref;

    // 1. Scan -> Insert -> Scan -> Delete -> Scan
    std::vector<storage::TupleSlot> results;
    index->ScanKey(*txn, *key, &results);
    EXPECT_TRUE(results.empty());

    const auto tuple_slot = sql_table->Insert(common::ManagedPointer(txn), insert_redo);

    EXPECT_TRUE(index->Insert(common::ManagedPointer(txn), *key, tuple_slot));

    index->ScanKey(*txn, *key, &results);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], tuple_slot);

    txn->StageDelete(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID, tuple_slot);
    sql_table->Delete(common::ManagedPointer(txn), tuple_slot);
    index->Delete(common::ManagedPointer(txn), *key, tuple_slot);

    results.clear();
    index->ScanKey(*txn, *key, &results);
    EXPECT_TRUE(results.empty());

    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Clean up
    db_main->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete sql_table; });
    delete[] key_buffer;
  }

  /**
   * Modifies a random column of the projected row with the given probability.
   * Returns true and updates reference if modified, false otherwise.
   */
  template <typename Random>
  bool ModifyRandomColumn(const IndexMetadata &metadata, storage::ProjectedRow *pr, byte *reference, float probability,
                          Random *generator) {
    std::bernoulli_distribution coin(probability);

    if (coin(*generator)) {
      const auto &oid_offset_map = metadata.GetKeyOidToOffsetMap();
      const auto &key_schema = metadata.GetSchema();
      std::uniform_int_distribution<uint16_t> rng(0, static_cast<uint16_t>(key_schema.GetColumns().size() - 1));
      const auto column = rng(*generator);

      uint16_t offset = 0;
      for (uint32_t i = 0; i < column; i++) {
        offset = static_cast<uint16_t>(offset + type::TypeUtil::GetTypeSize(key_schema.GetColumns()[i].Type()));
      }

      const auto type = key_schema.GetColumns()[column].Type();
      const auto type_size = type::TypeUtil::GetTypeSize(type);

      auto pr_offset = static_cast<uint16_t>(oid_offset_map.at(key_schema.GetColumns()[column].Oid()));
      auto attr = pr->AccessForceNotNull(pr_offset);
      auto *old_value = new byte[type_size];
      std::memcpy(old_value, attr, type_size);
      // force the value to change
      while (std::memcmp(old_value, attr, type_size) == 0) {
        WriteRandomAttribute(key_schema.GetColumns()[column], attr, reference + offset, generator);
      }
      delete[] old_value;
      return true;
    }

    return false;
  }

  /**
   * Sets the generic key to contain the given string. If c_str is nullptr, the key is zeroed out.
   */
  template <uint8_t KeySize>
  void SetGenericKeyFromString(const IndexMetadata &metadata, GenericKey<KeySize> *key, ProjectedRow *pr,
                               const char *c_str) {
    if (c_str != nullptr) {
      auto len = static_cast<uint32_t>(std::strlen(c_str));

      VarlenEntry data{};
      if (len <= VarlenEntry::InlineThreshold()) {
        data = VarlenEntry::CreateInline(reinterpret_cast<const byte *>(c_str), len);
      } else {
        auto *c_str_dup = new byte[len];
        std::memcpy(c_str_dup, c_str, len);
        data = VarlenEntry::Create(c_str_dup, len, false);
        loose_pointers_.emplace_back(c_str_dup);
      }

      *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
      (*key).SetFromProjectedRow(*pr, metadata, 1);
    } else {
      pr->SetNull(0);
      (*key).SetFromProjectedRow(*pr, metadata, 1);
    }
  }

  /**
   * Tests GenericKey's equality and comparison for the two null-terminated c_str's.
   */
  template <uint8_t KeySize>
  void TestGenericKeyStrings(const IndexMetadata &metadata, ProjectedRow *pr, char *c_str1, char *c_str2) {
    const auto generic_eq64 =
        std::equal_to<GenericKey<KeySize>>();                    // NOLINT transparent functors can't deduce template
    const auto generic_lt64 = std::less<GenericKey<KeySize>>();  // NOLINT transparent functors can't deduce template

    GenericKey<KeySize> key1, key2;
    SetGenericKeyFromString<KeySize>(metadata, &key1, pr, c_str1);
    SetGenericKeyFromString<KeySize>(metadata, &key2, pr, c_str2);

    bool ref_eq, ref_lt;
    if (c_str1 == nullptr && c_str2 == nullptr) {
      // NULL and NULL
      ref_eq = true;
      ref_lt = false;
    } else if (c_str1 == nullptr) {
      // NULL and NOT NULL
      ref_eq = false;
      ref_lt = true;
    } else if (c_str2 == nullptr) {
      // NOT NULL and NULL
      ref_eq = false;
      ref_lt = false;
    } else {
      // NOT NULL and NOT NULL
      ref_eq = strcmp(c_str1, c_str2) == 0;
      ref_lt = strcmp(c_str1, c_str2) < 0;
    }

    EXPECT_EQ(generic_eq64(key1, key2), ref_eq);
    EXPECT_EQ(generic_lt64(key1, key2), ref_lt);
  }

 protected:
  void TearDown() override {
    for (byte *ptr : loose_pointers_) {
      delete[] ptr;
    }
  }
};

/**
 * Calls compact ints equality for KeySize, i.e. std::equal_to<CompactIntsKey<KeySize>>()
 */
template <uint8_t KeySize>
bool CompactIntsFromProjectedRowEq(const IndexMetadata &metadata, const storage::ProjectedRow &pr_A,
                                   const storage::ProjectedRow &pr_B) {
  auto key_a = CompactIntsKey<KeySize>();
  auto key_b = CompactIntsKey<KeySize>();
  key_a.SetFromProjectedRow(pr_A, metadata, metadata.GetSchema().GetColumns().size());
  key_b.SetFromProjectedRow(pr_B, metadata, metadata.GetSchema().GetColumns().size());
  return std::equal_to<CompactIntsKey<KeySize>>()(key_a, key_b);
}

/**
 * Calls compact ints comparator for KeySize, i.e. std::less<CompactIntsKey<KeySize>>()
 */
template <uint8_t KeySize>
bool CompactIntsFromProjectedRowCmp(const IndexMetadata &metadata, const storage::ProjectedRow &pr_A,
                                    const storage::ProjectedRow &pr_B) {
  auto key_a = CompactIntsKey<KeySize>();
  auto key_b = CompactIntsKey<KeySize>();
  key_a.SetFromProjectedRow(pr_A, metadata, metadata.GetSchema().GetColumns().size());
  key_b.SetFromProjectedRow(pr_B, metadata, metadata.GetSchema().GetColumns().size());
  return std::less<CompactIntsKey<KeySize>>()(key_a, key_b);
}

// Test that we generate the right metadata for CompactIntsKey compatible schemas
// NOLINTNEXTLINE
TEST_F(IndexKeyTests, IndexMetadataCompactIntsKeyTest) {
  // INPUT:
  //    key_schema            {INTEGER, INTEGER, BIGINT, TINYINT, SMALLINT}
  //    oids                  {20, 21, 22, 23, 24}
  // EXPECT:
  //    identical key schema
  //    attr_sizes            { 4,  4,  8,  1,  2}
  //    inlined_attr_sizes    { 4,  4,  8,  1,  2}
  //    must_inline_varlens   false
  //    compact_ints_offsets  { 0,  4,  8, 16, 17}
  //    key_oid_to_offset     {20:1, 21:2, 22:0, 23:4, 24:3}
  //    pr_offsets            { 1,  2,  0,  4,  3}

  catalog::indexkeycol_oid_t oid(20);
  std::vector<catalog::IndexSchema::Column> key_cols;

  // key_schema            {INTEGER, INTEGER, BIGINT, TINYINT, SMALLINT}
  // oids                  {20, 21, 22, 23, 24}
  key_cols.emplace_back("", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);
  key_cols.emplace_back("", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);
  key_cols.emplace_back("", type::TypeId::BIGINT, false, parser::ConstantValueExpression(type::TypeId::BIGINT));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);
  key_cols.emplace_back("", type::TypeId::TINYINT, false, parser::ConstantValueExpression(type::TypeId::TINYINT));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);
  key_cols.emplace_back("", type::TypeId::SMALLINT, false, parser::ConstantValueExpression(type::TypeId::SMALLINT));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);

  IndexMetadata metadata(catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true));

  // identical key schema
  const auto &metadata_key_schema = metadata.GetSchema().GetColumns();
  EXPECT_EQ(metadata_key_schema.size(), 5);
  EXPECT_EQ(metadata_key_schema[0].Type(), type::TypeId::INTEGER);
  EXPECT_EQ(metadata_key_schema[1].Type(), type::TypeId::INTEGER);
  EXPECT_EQ(metadata_key_schema[2].Type(), type::TypeId::BIGINT);
  EXPECT_EQ(metadata_key_schema[3].Type(), type::TypeId::TINYINT);
  EXPECT_EQ(metadata_key_schema[4].Type(), type::TypeId::SMALLINT);
  EXPECT_EQ(metadata_key_schema[0].Oid().UnderlyingValue(), 20);
  EXPECT_EQ(metadata_key_schema[1].Oid().UnderlyingValue(), 21);
  EXPECT_EQ(metadata_key_schema[2].Oid().UnderlyingValue(), 22);
  EXPECT_EQ(metadata_key_schema[3].Oid().UnderlyingValue(), 23);
  EXPECT_EQ(metadata_key_schema[4].Oid().UnderlyingValue(), 24);
  EXPECT_FALSE(metadata_key_schema[0].Nullable());
  EXPECT_FALSE(metadata_key_schema[1].Nullable());
  EXPECT_FALSE(metadata_key_schema[2].Nullable());
  EXPECT_FALSE(metadata_key_schema[3].Nullable());
  EXPECT_FALSE(metadata_key_schema[4].Nullable());

  // attr_sizes            { 4,  4,  8,  1,  2}
  const auto &attr_sizes = metadata.GetAttributeSizes();
  EXPECT_EQ(attr_sizes[0], 4);
  EXPECT_EQ(attr_sizes[1], 4);
  EXPECT_EQ(attr_sizes[2], 8);
  EXPECT_EQ(attr_sizes[3], 1);
  EXPECT_EQ(attr_sizes[4], 2);

  // inlined_attr_sizes    { 4,  4,  8,  1,  2}
  const auto &inlined_attr_sizes = metadata.GetInlinedAttributeSizes();
  EXPECT_EQ(inlined_attr_sizes[0], 4);
  EXPECT_EQ(inlined_attr_sizes[1], 4);
  EXPECT_EQ(inlined_attr_sizes[2], 8);
  EXPECT_EQ(inlined_attr_sizes[3], 1);
  EXPECT_EQ(inlined_attr_sizes[4], 2);

  // must_inline_varlen   false
  EXPECT_FALSE(metadata.MustInlineVarlen());

  // compact_ints_offsets  { 0,  4,  8, 16, 17}
  const auto &compact_ints_offsets = metadata.GetCompactIntsOffsets();
  EXPECT_EQ(compact_ints_offsets[0], 0);
  EXPECT_EQ(compact_ints_offsets[1], 4);
  EXPECT_EQ(compact_ints_offsets[2], 8);
  EXPECT_EQ(compact_ints_offsets[3], 16);
  EXPECT_EQ(compact_ints_offsets[4], 17);

  // key_oid_to_offset     {20:1, 21:2, 22:0, 23:4, 24:3}
  const auto &key_oid_to_offset = metadata.GetKeyOidToOffsetMap();
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(20)), 1);
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(21)), 2);
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(22)), 0);
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(23)), 4);
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(24)), 3);

  // pr_offsets            { 1,  2,  0,  4,  3}
  const auto &pr_offsets = IndexMetadata::ComputePROffsets(metadata.inlined_attr_sizes_);
  EXPECT_EQ(pr_offsets[0], 1);
  EXPECT_EQ(pr_offsets[1], 2);
  EXPECT_EQ(pr_offsets[2], 0);
  EXPECT_EQ(pr_offsets[3], 4);
  EXPECT_EQ(pr_offsets[4], 3);
}

// Test that we generate the right metadata for GenericKey schemas that don't need to manually inline varlens
// NOLINTNEXTLINE
TEST_F(IndexKeyTests, IndexMetadataGenericKeyNoMustInlineVarlenTest) {
  // INPUT:
  //    key_schema            {INTEGER, VARCHAR(8), VARCHAR(0), TINYINT, VARCHAR(12)}
  //    oids                  {20, 21, 22, 23, 24}
  // EXPECT:
  //    identical key schema
  //    attr_sizes            {4, VARLEN_COLUMN, VARLEN_COLUMN, 1, VARLEN_COLUMN}
  //    inlined_attr_sizes    { 4, 16, 16,  1, 16}
  //    must_inline_varlens   false
  //    key_oid_to_offset     {20:3, 21:0, 22:1, 23:4, 24:2}
  //    pr_offsets            { 3,  0,  1,  4,  2}

  catalog::indexkeycol_oid_t oid(20);
  std::vector<catalog::IndexSchema::Column> key_cols;

  // key_schema            {INTEGER, VARCHAR(8), VARCHAR(0), TINYINT, VARCHAR(12)}
  // oids                  {20, 21, 22, 23, 24}
  key_cols.emplace_back("", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);
  key_cols.emplace_back("", type::TypeId::VARCHAR, 8, false, parser::ConstantValueExpression(type::TypeId::VARCHAR));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);
  key_cols.emplace_back("", type::TypeId::VARCHAR, 0, false, parser::ConstantValueExpression(type::TypeId::VARCHAR));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);
  key_cols.emplace_back("", type::TypeId::TINYINT, false, parser::ConstantValueExpression(type::TypeId::TINYINT));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);
  key_cols.emplace_back("", type::TypeId::VARCHAR, 12, false, parser::ConstantValueExpression(type::TypeId::VARCHAR));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);

  IndexMetadata metadata(catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true));

  // identical key schema
  const auto &metadata_key_schema = metadata.GetSchema().GetColumns();
  EXPECT_EQ(metadata_key_schema.size(), 5);
  EXPECT_EQ(metadata_key_schema[0].Type(), type::TypeId::INTEGER);
  EXPECT_EQ(metadata_key_schema[1].Type(), type::TypeId::VARCHAR);
  EXPECT_EQ(metadata_key_schema[2].Type(), type::TypeId::VARCHAR);
  EXPECT_EQ(metadata_key_schema[3].Type(), type::TypeId::TINYINT);
  EXPECT_EQ(metadata_key_schema[4].Type(), type::TypeId::VARCHAR);
  EXPECT_EQ(metadata_key_schema[0].Oid().UnderlyingValue(), 20);
  EXPECT_EQ(metadata_key_schema[1].Oid().UnderlyingValue(), 21);
  EXPECT_EQ(metadata_key_schema[2].Oid().UnderlyingValue(), 22);
  EXPECT_EQ(metadata_key_schema[3].Oid().UnderlyingValue(), 23);
  EXPECT_EQ(metadata_key_schema[4].Oid().UnderlyingValue(), 24);
  EXPECT_FALSE(metadata_key_schema[0].Nullable());
  EXPECT_FALSE(metadata_key_schema[1].Nullable());
  EXPECT_FALSE(metadata_key_schema[2].Nullable());
  EXPECT_FALSE(metadata_key_schema[3].Nullable());
  EXPECT_FALSE(metadata_key_schema[4].Nullable());
  EXPECT_EQ(metadata_key_schema[1].MaxVarlenSize(), 8);
  EXPECT_EQ(metadata_key_schema[2].MaxVarlenSize(), 0);
  EXPECT_EQ(metadata_key_schema[4].MaxVarlenSize(), 12);

  // attr_sizes            {4, VARLEN_COLUMN, VARLEN_COLUMN, 1, VARLEN_COLUMN}
  const auto &attr_sizes = metadata.GetAttributeSizes();
  EXPECT_EQ(attr_sizes[0], 4);
  EXPECT_EQ(attr_sizes[1], VARLEN_COLUMN);
  EXPECT_EQ(attr_sizes[2], VARLEN_COLUMN);
  EXPECT_EQ(attr_sizes[3], 1);
  EXPECT_EQ(attr_sizes[4], VARLEN_COLUMN);

  // inlined_attr_sizes    { 4, 16, 16,  1, 16}
  const auto &inlined_attr_sizes = metadata.GetInlinedAttributeSizes();
  EXPECT_EQ(inlined_attr_sizes[0], 4);
  EXPECT_EQ(inlined_attr_sizes[1], 16);
  EXPECT_EQ(inlined_attr_sizes[2], 16);
  EXPECT_EQ(inlined_attr_sizes[3], 1);
  EXPECT_EQ(inlined_attr_sizes[4], 16);

  // must_inline_varlen    false
  EXPECT_FALSE(metadata.MustInlineVarlen());

  // key_oid_to_offset     {20:3, 21:0, 22:1, 23:4, 24:2}
  const auto &key_oid_to_offset = metadata.GetKeyOidToOffsetMap();
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(20)), 3);
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(21)), 0);
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(22)), 1);
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(23)), 4);
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(24)), 2);

  // pr_offsets            { 3,  0,  1,  4,  2}
  const auto &pr_offsets = IndexMetadata::ComputePROffsets(metadata.inlined_attr_sizes_);
  EXPECT_EQ(pr_offsets[0], 3);
  EXPECT_EQ(pr_offsets[1], 0);
  EXPECT_EQ(pr_offsets[2], 1);
  EXPECT_EQ(pr_offsets[3], 4);
  EXPECT_EQ(pr_offsets[4], 2);
}

// Test that we generate the right metadata for GenericKey schemas that need to manually inline varlens
// NOLINTNEXTLINE
TEST_F(IndexKeyTests, IndexMetadataGenericKeyMustInlineVarlenTest) {
  // INPUT:
  //    key_schema            {INTEGER, VARCHAR(50), VARCHAR(8), TINYINT, VARCHAR(90)}
  //    oids                  {20, 21, 22, 23, 24}
  // EXPECT:
  //    identical key schema
  //    attr_sizes            {4, VARLEN_COLUMN, VARLEN_COLUMN, 1, VARLEN_COLUMN}
  //    inlined_attr_sizes    { 4, 54, 16,  1, 94}
  //    must_inline_varlens   true
  //    key_oid_to_offset     {20:3, 21:1, 22:2, 23:4, 24:0}
  //    pr_offsets            { 3,  1,  2,  4,  0}

  catalog::indexkeycol_oid_t oid(20);
  std::vector<catalog::IndexSchema::Column> key_cols;

  // key_schema            {INTEGER, VARCHAR(50), VARCHAR(8), TINYINT, VARCHAR(90)}
  // oids                  {20, 21, 22, 23, 24}
  key_cols.emplace_back("", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);
  key_cols.emplace_back("", type::TypeId::VARCHAR, 50, false, parser::ConstantValueExpression(type::TypeId::VARCHAR));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);
  key_cols.emplace_back("", type::TypeId::VARCHAR, 8, false, parser::ConstantValueExpression(type::TypeId::VARCHAR));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);
  key_cols.emplace_back("", type::TypeId::TINYINT, false, parser::ConstantValueExpression(type::TypeId::TINYINT));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);
  key_cols.emplace_back("", type::TypeId::VARCHAR, 90, false, parser::ConstantValueExpression(type::TypeId::VARCHAR));
  StorageTestUtil::ForceOid(&(key_cols.back()), oid++);

  IndexMetadata metadata(catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true));

  // identical key schema
  const auto &metadata_key_schema = metadata.GetSchema().GetColumns();
  EXPECT_EQ(metadata_key_schema.size(), 5);
  EXPECT_EQ(metadata_key_schema[0].Type(), type::TypeId::INTEGER);
  EXPECT_EQ(metadata_key_schema[1].Type(), type::TypeId::VARCHAR);
  EXPECT_EQ(metadata_key_schema[2].Type(), type::TypeId::VARCHAR);
  EXPECT_EQ(metadata_key_schema[3].Type(), type::TypeId::TINYINT);
  EXPECT_EQ(metadata_key_schema[4].Type(), type::TypeId::VARCHAR);
  EXPECT_EQ(metadata_key_schema[0].Oid().UnderlyingValue(), 20);
  EXPECT_EQ(metadata_key_schema[1].Oid().UnderlyingValue(), 21);
  EXPECT_EQ(metadata_key_schema[2].Oid().UnderlyingValue(), 22);
  EXPECT_EQ(metadata_key_schema[3].Oid().UnderlyingValue(), 23);
  EXPECT_EQ(metadata_key_schema[4].Oid().UnderlyingValue(), 24);
  EXPECT_FALSE(metadata_key_schema[0].Nullable());
  EXPECT_FALSE(metadata_key_schema[1].Nullable());
  EXPECT_FALSE(metadata_key_schema[2].Nullable());
  EXPECT_FALSE(metadata_key_schema[3].Nullable());
  EXPECT_FALSE(metadata_key_schema[4].Nullable());
  EXPECT_EQ(metadata_key_schema[1].MaxVarlenSize(), 50);
  EXPECT_EQ(metadata_key_schema[2].MaxVarlenSize(), 8);
  EXPECT_EQ(metadata_key_schema[4].MaxVarlenSize(), 90);

  // attr_sizes            {4, VARLEN_COLUMN, VARLEN_COLUMN, 1, VARLEN_COLUMN}
  const auto &attr_sizes = metadata.GetAttributeSizes();
  EXPECT_EQ(attr_sizes[0], 4);
  EXPECT_EQ(attr_sizes[1], VARLEN_COLUMN);
  EXPECT_EQ(attr_sizes[2], VARLEN_COLUMN);
  EXPECT_EQ(attr_sizes[3], 1);
  EXPECT_EQ(attr_sizes[4], VARLEN_COLUMN);

  // inlined_attr_sizes    { 4, 54, 16,  1, 94}
  const auto &inlined_attr_sizes = metadata.GetInlinedAttributeSizes();
  EXPECT_EQ(inlined_attr_sizes[0], 4);
  EXPECT_EQ(inlined_attr_sizes[1], 54);
  EXPECT_EQ(inlined_attr_sizes[2], 16);
  EXPECT_EQ(inlined_attr_sizes[3], 1);
  EXPECT_EQ(inlined_attr_sizes[4], 94);

  // must_inline_varlen    true
  EXPECT_TRUE(metadata.MustInlineVarlen());

  // key_oid_to_offset     {20:3, 21:1, 22:2, 23:4, 24:0}
  const auto &key_oid_to_offset = metadata.GetKeyOidToOffsetMap();
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(20)), 3);
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(21)), 1);
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(22)), 2);
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(23)), 4);
  EXPECT_EQ(key_oid_to_offset.at(catalog::indexkeycol_oid_t(24)), 0);

  // pr_offsets            { 3,  1,  2,  4,  0}
  const auto &pr_offsets = IndexMetadata::ComputePROffsets(metadata.inlined_attr_sizes_);
  EXPECT_EQ(pr_offsets[0], 3);
  EXPECT_EQ(pr_offsets[1], 1);
  EXPECT_EQ(pr_offsets[2], 2);
  EXPECT_EQ(pr_offsets[3], 4);
  EXPECT_EQ(pr_offsets[4], 0);
}

/**
 * 1. Generate a reference key schema.
 * 2. Fill two keys (pr_A, pr_B) with random data (data_A, data_B)
 * 3. Set CompactIntsKeys (key_A, key_B) from (pr_A, pr_B)
 * 4. Expect eq(key_A, key_B) == (data_A == data_B)
 * 5. Expect cmp(key_A, key_B) == (data_A < data_B)
 */
// NOLINTNEXTLINE
TEST_F(IndexKeyTests, RandomCompactIntsKeyTest) {
  const uint32_t num_iterations = 1000;

  for (uint32_t i = 0; i < num_iterations; i++) {
    // generate random key schema
    auto key_schema = StorageTestUtil::RandomSimpleKeySchema(&generator_, COMPACTINTSKEY_MAX_SIZE);
    IndexMetadata metadata(key_schema);
    const auto &initializer = metadata.GetProjectedRowInitializer();

    // figure out which CompactIntsKey template was instantiated
    // this is unpleasant, but seems to be the cleanest way
    uint16_t key_size = 0;
    for (const auto &key : key_schema.GetColumns()) {
      key_size = static_cast<uint16_t>(key_size + type::TypeUtil::GetTypeSize(key.Type()));
    }
    uint8_t key_type = 0;
    for (uint8_t j = 1; j <= 4; j++) {
      if (key_size <= sizeof(uint64_t) * j) {
        key_type = j;
        break;
      }
    }
    NOISEPAGE_ASSERT(1 <= key_type && key_type <= 4, "CompactIntsKey only has 4 possible KeySizes.");

    // create our projected row buffers
    auto *pr_buffer_a = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *pr_buffer_b = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *pr_a = initializer.InitializeRow(pr_buffer_a);
    auto *pr_b = initializer.InitializeRow(pr_buffer_b);
    //
    for (uint8_t j = 0; j < 10; j++) {
      // fill our buffers with random data
      const auto data_a = FillProjectedRowWithRandomCompactInts(metadata, pr_a, &generator_);
      const auto data_b = FillProjectedRowWithRandomCompactInts(metadata, pr_b, &generator_);

      // perform the relevant checks
      switch (key_type) {
        case 1:
          EXPECT_EQ(CompactIntsFromProjectedRowEq<8>(metadata, *pr_a, *pr_b), data_a == data_b);
          EXPECT_EQ(CompactIntsFromProjectedRowCmp<8>(metadata, *pr_a, *pr_b), data_a < data_b);
          break;
        case 2:
          EXPECT_EQ(CompactIntsFromProjectedRowEq<16>(metadata, *pr_a, *pr_b), data_a == data_b);
          EXPECT_EQ(CompactIntsFromProjectedRowCmp<16>(metadata, *pr_a, *pr_b), data_a < data_b);
          break;
        case 3:
          EXPECT_EQ(CompactIntsFromProjectedRowEq<24>(metadata, *pr_a, *pr_b), data_a == data_b);
          EXPECT_EQ(CompactIntsFromProjectedRowCmp<24>(metadata, *pr_a, *pr_b), data_a < data_b);
          break;
        case 4:
          EXPECT_EQ(CompactIntsFromProjectedRowEq<32>(metadata, *pr_a, *pr_b), data_a == data_b);
          EXPECT_EQ(CompactIntsFromProjectedRowCmp<32>(metadata, *pr_a, *pr_b), data_a < data_b);
          break;
        default:
          throw std::runtime_error("Invalid compact ints key type.");
      }
    }

    for (uint8_t j = 0; j < 10; j++) {
      // fill buffer pr_A with random data data_A
      const auto data_a = FillProjectedRow(metadata, pr_a, &generator_);
      float probabilities[] = {0.0, 0.5, 1.0};

      for (float prob : probabilities) {
        // have B copy A. Recast pr_b to workaround -Wclass-memaccess.
        std::memcpy(static_cast<void *>(pr_b), pr_a, initializer.ProjectedRowSize());
        auto *data_b = new byte[key_size];
        std::memcpy(data_b, data_a, key_size);

        // modify a column of B with some probability, this also updates the reference data_B
        bool modified = ModifyRandomColumn(metadata, pr_b, data_b, prob, &generator_);

        // perform the relevant checks
        switch (key_type) {
          case 1:
            EXPECT_EQ(CompactIntsFromProjectedRowEq<8>(metadata, *pr_a, *pr_b),
                      std::memcmp(data_a, data_b, key_size) == 0 && !modified);
            EXPECT_EQ(CompactIntsFromProjectedRowCmp<8>(metadata, *pr_a, *pr_b),
                      std::memcmp(data_a, data_b, key_size) < 0);
            break;
          case 2:
            EXPECT_EQ(CompactIntsFromProjectedRowEq<16>(metadata, *pr_a, *pr_b),
                      std::memcmp(data_a, data_b, key_size) == 0 && !modified);
            EXPECT_EQ(CompactIntsFromProjectedRowCmp<16>(metadata, *pr_a, *pr_b),
                      std::memcmp(data_a, data_b, key_size) < 0);
            break;
          case 3:
            EXPECT_EQ(CompactIntsFromProjectedRowEq<24>(metadata, *pr_a, *pr_b),
                      std::memcmp(data_a, data_b, key_size) == 0 && !modified);
            EXPECT_EQ(CompactIntsFromProjectedRowCmp<24>(metadata, *pr_a, *pr_b),
                      std::memcmp(data_a, data_b, key_size) < 0);
            break;
          case 4:
            EXPECT_EQ(CompactIntsFromProjectedRowEq<32>(metadata, *pr_a, *pr_b),
                      std::memcmp(data_a, data_b, key_size) == 0 && !modified);
            EXPECT_EQ(CompactIntsFromProjectedRowCmp<32>(metadata, *pr_a, *pr_b),
                      std::memcmp(data_a, data_b, key_size) < 0);
            break;
          default:
            throw std::runtime_error("Invalid compact ints key type.");
        }
        delete[] data_b;
      }

      delete[] data_a;
    }

    delete[] pr_buffer_a;
    delete[] pr_buffer_b;
  }
}

template <uint8_t KeySize, typename CType, typename Random>
void CompactIntsKeyBasicTest(type::TypeId type_id, Random *const generator) {
  catalog::IndexSchema key_schema;

  std::vector<catalog::IndexSchema::Column> key_cols;
  const uint8_t num_cols = KeySize / sizeof(CType);

  for (uint8_t i = 0; i < num_cols; i++) {
    key_cols.emplace_back("", type_id, false, parser::ConstantValueExpression(type_id));
    StorageTestUtil::ForceOid(&(key_cols.back()), catalog::indexkeycol_oid_t(i));
  }
  key_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, false, false, false, true);

  const IndexMetadata metadata(key_schema);
  const auto &initializer = metadata.GetProjectedRowInitializer();

  auto *const pr_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  std::memset(pr_buffer, 0, initializer.ProjectedRowSize());
  auto *const pr = initializer.InitializeRow(pr_buffer);

  for (uint8_t i = 0; i < num_cols; i++) {
    pr->AccessForceNotNull(i);
  }

  CompactIntsKey<KeySize> key1, key2;
  key1.SetFromProjectedRow(*pr, metadata, metadata.GetSchema().GetColumns().size());
  key2.SetFromProjectedRow(*pr, metadata, metadata.GetSchema().GetColumns().size());

  EXPECT_TRUE(std::equal_to<CompactIntsKey<KeySize>>()(key1, key2));
  EXPECT_EQ(std::hash<CompactIntsKey<KeySize>>()(key1), std::hash<CompactIntsKey<KeySize>>()(key2));
  EXPECT_FALSE(std::less<CompactIntsKey<KeySize>>()(key1, key2));

  *reinterpret_cast<CType *>(pr->AccessForceNotNull(num_cols - 1)) = static_cast<CType>(1);
  key2.SetFromProjectedRow(*pr, metadata, metadata.GetSchema().GetColumns().size());

  EXPECT_FALSE(std::equal_to<CompactIntsKey<KeySize>>()(key1, key2));
  EXPECT_NE(std::hash<CompactIntsKey<KeySize>>()(key1), std::hash<CompactIntsKey<KeySize>>()(key2));
  EXPECT_TRUE(std::less<CompactIntsKey<KeySize>>()(key1, key2));

  delete[] pr_buffer;
}

// Verify basic key construction and value setting and comparator, equality, and hash operations for various key sizes.
// NOLINTNEXTLINE
TEST_F(IndexKeyTests, CompactIntsKeyBasicTest) {
  CompactIntsKeyBasicTest<8, int8_t>(type::TypeId::TINYINT, &generator_);
  CompactIntsKeyBasicTest<8, int16_t>(type::TypeId::SMALLINT, &generator_);
  CompactIntsKeyBasicTest<8, int32_t>(type::TypeId::INTEGER, &generator_);
  CompactIntsKeyBasicTest<8, int64_t>(type::TypeId::BIGINT, &generator_);

  CompactIntsKeyBasicTest<16, int8_t>(type::TypeId::TINYINT, &generator_);
  CompactIntsKeyBasicTest<16, int16_t>(type::TypeId::SMALLINT, &generator_);
  CompactIntsKeyBasicTest<16, int32_t>(type::TypeId::INTEGER, &generator_);
  CompactIntsKeyBasicTest<16, int64_t>(type::TypeId::BIGINT, &generator_);

  CompactIntsKeyBasicTest<24, int8_t>(type::TypeId::TINYINT, &generator_);
  CompactIntsKeyBasicTest<24, int16_t>(type::TypeId::SMALLINT, &generator_);
  CompactIntsKeyBasicTest<24, int32_t>(type::TypeId::INTEGER, &generator_);
  CompactIntsKeyBasicTest<24, int64_t>(type::TypeId::BIGINT, &generator_);

  CompactIntsKeyBasicTest<32, int8_t>(type::TypeId::TINYINT, &generator_);
  CompactIntsKeyBasicTest<32, int16_t>(type::TypeId::SMALLINT, &generator_);
  CompactIntsKeyBasicTest<32, int32_t>(type::TypeId::INTEGER, &generator_);
  CompactIntsKeyBasicTest<32, int64_t>(type::TypeId::BIGINT, &generator_);
}

template <typename KeyType, typename CType>
void NumericComparisons(const type::TypeId type_id, const bool nullable) {
  std::vector<catalog::IndexSchema::Column> key_cols;
  key_cols.emplace_back("", type_id, nullable, parser::ConstantValueExpression(type_id));
  StorageTestUtil::ForceOid(&(key_cols.back()), catalog::indexkeycol_oid_t(0));

  const IndexMetadata metadata(
      catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, false, false, false, true));
  const auto &initializer = metadata.GetProjectedRowInitializer();

  auto *const pr_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  std::memset(pr_buffer, 0, initializer.ProjectedRowSize());
  auto *const pr = initializer.InitializeRow(pr_buffer);

  CType data = 15;
  *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
  KeyType key1, key2;
  key1.SetFromProjectedRow(*pr, metadata, 1);
  key2.SetFromProjectedRow(*pr, metadata, 1);

  // lhs: 15, rhs: 15
  EXPECT_TRUE(std::equal_to<KeyType>()(key1, key2));
  EXPECT_EQ(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));
  EXPECT_FALSE(std::less<KeyType>()(key1, key2));

  data = 72;
  *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata, 1);

  // lhs: 15, rhs: 72
  EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
  EXPECT_NE(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));
  EXPECT_TRUE(std::less<KeyType>()(key1, key2));

  data = 116;
  *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata, 1);

  // lhs: 116, rhs: 72
  EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
  EXPECT_NE(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));
  EXPECT_FALSE(std::less<KeyType>()(key1, key2));

  if (nullable) {
    pr->SetNull(0);
    key1.SetFromProjectedRow(*pr, metadata, 1);

    // lhs: NULL, rhs: 72
    EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
    EXPECT_NE(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));
    EXPECT_TRUE(std::less<KeyType>()(key1, key2));

    key2.SetFromProjectedRow(*pr, metadata, 1);

    // lhs: NULL, rhs: NULL
    EXPECT_TRUE(std::equal_to<KeyType>()(key1, key2));
    EXPECT_EQ(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));
    EXPECT_FALSE(std::less<KeyType>()(key1, key2));

    data = 15;
    *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
    key1.SetFromProjectedRow(*pr, metadata, 1);

    // lhs: 15, rhs: NULL
    EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
    EXPECT_NE(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));
    EXPECT_FALSE(std::less<KeyType>()(key1, key2));
  }

  delete[] pr_buffer;
}

// NOLINTNEXTLINE
TEST_F(IndexKeyTests, CompactIntsKeyNumericComparisons) {
  NumericComparisons<CompactIntsKey<8>, int8_t>(type::TypeId::TINYINT, false);
  NumericComparisons<CompactIntsKey<8>, int16_t>(type::TypeId::SMALLINT, false);
  NumericComparisons<CompactIntsKey<8>, int32_t>(type::TypeId::INTEGER, false);
  NumericComparisons<CompactIntsKey<8>, int64_t>(type::TypeId::BIGINT, false);
}

// NOLINTNEXTLINE
TEST_F(IndexKeyTests, GenericKeyNumericComparisons) {
  NumericComparisons<GenericKey<64>, int8_t>(type::TypeId::TINYINT, true);
  NumericComparisons<GenericKey<64>, int16_t>(type::TypeId::SMALLINT, true);
  NumericComparisons<GenericKey<64>, int32_t>(type::TypeId::INTEGER, true);
  NumericComparisons<GenericKey<64>, uint32_t>(type::TypeId::DATE, true);
  NumericComparisons<GenericKey<64>, int64_t>(type::TypeId::BIGINT, true);
  NumericComparisons<GenericKey<64>, double>(type::TypeId::DECIMAL, true);
  NumericComparisons<GenericKey<64>, uint64_t>(type::TypeId::TIMESTAMP, true);
}

template <typename KeyType, typename CType>
void UnorderedNumericComparisons(const type::TypeId type_id, const bool nullable) {
  std::vector<catalog::IndexSchema::Column> key_cols;
  key_cols.emplace_back("", type_id, nullable, parser::ConstantValueExpression(type_id));
  StorageTestUtil::ForceOid(&(key_cols.back()), catalog::indexkeycol_oid_t(0));

  const IndexMetadata metadata(
      catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, false, false, false, true));
  const auto &initializer = metadata.GetProjectedRowInitializer();

  auto *const pr_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  std::memset(pr_buffer, 0, initializer.ProjectedRowSize());
  auto *const pr = initializer.InitializeRow(pr_buffer);

  CType data = 15;
  *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
  KeyType key1, key2;
  key1.SetFromProjectedRow(*pr, metadata, 1);
  key2.SetFromProjectedRow(*pr, metadata, 1);

  // lhs: 15, rhs: 15
  EXPECT_TRUE(std::equal_to<KeyType>()(key1, key2));
  EXPECT_EQ(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));

  data = 72;
  *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata, 1);

  // lhs: 15, rhs: 72
  EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
  EXPECT_NE(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));

  data = 116;
  *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata, 1);

  // lhs: 116, rhs: 72
  EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
  EXPECT_NE(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));

  if (nullable) {
    pr->SetNull(0);
    key1.SetFromProjectedRow(*pr, metadata, 1);

    // lhs: NULL, rhs: 72
    EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
    EXPECT_NE(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));

    key2.SetFromProjectedRow(*pr, metadata, 1);

    // lhs: NULL, rhs: NULL
    EXPECT_TRUE(std::equal_to<KeyType>()(key1, key2));
    EXPECT_EQ(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));

    data = 15;
    *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
    key1.SetFromProjectedRow(*pr, metadata, 1);

    // lhs: 15, rhs: NULL
    EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
    EXPECT_NE(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));
  }

  delete[] pr_buffer;
}

// NOLINTNEXTLINE
TEST_F(IndexKeyTests, HashKeyNumericComparisons) {
  UnorderedNumericComparisons<HashKey<8>, int8_t>(type::TypeId::TINYINT, false);
  UnorderedNumericComparisons<HashKey<8>, int16_t>(type::TypeId::SMALLINT, false);
  UnorderedNumericComparisons<HashKey<8>, int32_t>(type::TypeId::INTEGER, false);
  UnorderedNumericComparisons<HashKey<8>, uint32_t>(type::TypeId::DATE, false);
  UnorderedNumericComparisons<HashKey<8>, int64_t>(type::TypeId::BIGINT, false);
  UnorderedNumericComparisons<HashKey<8>, double>(type::TypeId::DECIMAL, false);
  UnorderedNumericComparisons<HashKey<8>, uint64_t>(type::TypeId::TIMESTAMP, false);
}

// NOLINTNEXTLINE
TEST_F(IndexKeyTests, GenericKeyInlineVarlenComparisons) {
  std::vector<catalog::IndexSchema::Column> key_cols;
  key_cols.emplace_back("", type::TypeId::VARCHAR, 12, true, parser::ConstantValueExpression(type::TypeId::VARCHAR));
  StorageTestUtil::ForceOid(&(key_cols.back()), catalog::indexkeycol_oid_t(0));

  const IndexMetadata metadata(
      catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, false, false, false, true));
  const auto &initializer = metadata.GetProjectedRowInitializer();

  auto *const pr_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *const pr = initializer.InitializeRow(pr_buffer);

  char john[5] = "john";
  char johnny[7] = "johnny";
  char johnathan[10] = "johnathan";

  VarlenEntry data =
      VarlenEntry::CreateInline(reinterpret_cast<byte *>(john), static_cast<uint32_t>(std::strlen(john)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  GenericKey<64> key1, key2;
  key1.SetFromProjectedRow(*pr, metadata, 1);
  key2.SetFromProjectedRow(*pr, metadata, 1);

  const auto generic_eq64 = std::equal_to<GenericKey<64>>();  // NOLINT transparent functors can't figure out template
  const auto generic_lt64 = std::less<GenericKey<64>>();      // NOLINT transparent functors can't figure out template
  const auto generic_hash64 = std::hash<GenericKey<64>>();    // NOLINT transparent functors can't figure out template

  // lhs: "john", rhs: "john" (same prefixes, same strings (both <= prefix))
  EXPECT_TRUE(generic_eq64(key1, key2));
  EXPECT_EQ(generic_hash64(key1), generic_hash64(key2));
  EXPECT_FALSE(generic_lt64(key1, key2));

  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnny), static_cast<uint32_t>(std::strlen(johnny)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata, 1);

  // lhs: "john", rhs: "johnny" (same prefixes, different string (one <=prefix))
  EXPECT_FALSE(generic_eq64(key1, key2));
  EXPECT_NE(generic_hash64(key1), generic_hash64(key2));
  EXPECT_TRUE(generic_lt64(key1, key2));

  key1.SetFromProjectedRow(*pr, metadata, 1);
  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(john), static_cast<uint32_t>(std::strlen(john)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata, 1);

  // lhs: "johnny", rhs: "john" (same prefixes, different strings (one <=prefix))
  EXPECT_FALSE(generic_eq64(key1, key2));
  EXPECT_NE(generic_hash64(key1), generic_hash64(key2));
  EXPECT_FALSE(generic_lt64(key1, key2));

  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnny), static_cast<uint32_t>(std::strlen(johnny)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata, 1);
  key2.SetFromProjectedRow(*pr, metadata, 1);

  // lhs: "johnny", rhs: "johnny" (same prefixes, same strings (> prefix))
  EXPECT_TRUE(generic_eq64(key1, key2));
  EXPECT_EQ(generic_hash64(key1), generic_hash64(key2));
  EXPECT_FALSE(generic_lt64(key1, key2));

  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnathan), static_cast<uint32_t>(std::strlen(johnathan)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata, 1);

  // lhs: "johnathan", rhs: "johnny" (different prefixes, different strings (> prefix))
  EXPECT_FALSE(generic_eq64(key1, key2));
  EXPECT_NE(generic_hash64(key1), generic_hash64(key2));
  EXPECT_TRUE(generic_lt64(key1, key2));

  key2.SetFromProjectedRow(*pr, metadata, 1);
  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnny), static_cast<uint32_t>(std::strlen(johnny)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata, 1);

  // lhs: "johnny", rhs: "johnathan" (different prefixes, different strings (> prefix))
  EXPECT_FALSE(generic_eq64(key1, key2));
  EXPECT_NE(generic_hash64(key1), generic_hash64(key2));
  EXPECT_FALSE(generic_lt64(key1, key2));

  pr->SetNull(0);
  key1.SetFromProjectedRow(*pr, metadata, 1);

  // lhs: NULL, rhs: "johnathan"
  EXPECT_FALSE(generic_eq64(key1, key2));
  EXPECT_TRUE(generic_lt64(key1, key2));

  key1.SetFromProjectedRow(*pr, metadata, 1);
  key2.SetFromProjectedRow(*pr, metadata, 1);

  // lhs: NULL, rhs: NULL
  EXPECT_TRUE(generic_eq64(key1, key2));
  EXPECT_EQ(generic_hash64(key1), generic_hash64(key2));
  EXPECT_FALSE(generic_lt64(key1, key2));

  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnathan), static_cast<uint32_t>(std::strlen(johnathan)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata, 1);

  // lhs: "johnathan", rhs: NULL
  EXPECT_FALSE(generic_eq64(key1, key2));
  EXPECT_NE(generic_hash64(key1), generic_hash64(key2));
  EXPECT_FALSE(generic_lt64(key1, key2));

  delete[] pr_buffer;
}

// NOLINTNEXTLINE
TEST_F(IndexKeyTests, GenericKeyNonInlineVarlenComparisons) {
  std::vector<catalog::IndexSchema::Column> key_cols;
  key_cols.emplace_back("", type::TypeId::VARCHAR, 20, true, parser::ConstantValueExpression(type::TypeId::VARCHAR));
  StorageTestUtil::ForceOid(&(key_cols.back()), catalog::indexkeycol_oid_t(0));

  const IndexMetadata metadata(
      catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, false, false, false, true));
  const auto &initializer = metadata.GetProjectedRowInitializer();

  auto *const pr_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *const pr = initializer.InitializeRow(pr_buffer);

  char john[5] = "john";
  char johnny[7] = "johnny";
  char johnny_johnny[14] = "johnny_johnny";
  char johnathan_johnathan[20] = "johnathan_johnathan";

  // lhs: "johnathan_johnathan", rhs: "johnathan_johnathan" (same prefixes, same strings (both non-inline))
  TestGenericKeyStrings<64>(metadata, pr, johnathan_johnathan, johnathan_johnathan);

  // lhs: "johnathan_johnathan", rhs: "johnny_johnny" (same prefixes, different strings (both non-inline))
  TestGenericKeyStrings<64>(metadata, pr, johnathan_johnathan, johnny_johnny);

  // lhs: "johnny_johnny", rhs: "johnathan_johnathan" (same prefixes, different strings (both non-inline))
  TestGenericKeyStrings<64>(metadata, pr, johnny_johnny, johnathan_johnathan);

  // lhs: "johnny_johnny", rhs: "john" (same prefixes, different strings (one <=prefix))
  TestGenericKeyStrings<64>(metadata, pr, johnny_johnny, john);

  // lhs: "john", rhs: "johnny_johnny" (same prefixes, different strings (one <=prefix))
  TestGenericKeyStrings<64>(metadata, pr, john, johnny_johnny);

  // lhs: "johnny", rhs: "johnny_johnny" (same prefixes, different strings (one inline))
  TestGenericKeyStrings<64>(metadata, pr, johnny, johnny_johnny);

  // lhs: "johnny_johnny", rhs: "johnny" (same prefixes, different strings (one inline))
  TestGenericKeyStrings<64>(metadata, pr, johnny_johnny, johnny);

  // lhs: "johnny_johnny", rhs: NULL
  TestGenericKeyStrings<64>(metadata, pr, johnny_johnny, nullptr);

  // lhs: NULL, rhs: NULL
  TestGenericKeyStrings<64>(metadata, pr, nullptr, nullptr);

  // lhs: NULL, rhs: "johnny_johnny"
  TestGenericKeyStrings<64>(metadata, pr, nullptr, johnny_johnny);

  delete[] pr_buffer;
}

// NOLINTNEXTLINE
TEST_F(IndexKeyTests, CompactIntsKeyBuilderTest) {
  const uint32_t num_iters = 100;

  for (uint32_t i = 0; i < num_iters; i++) {
    const auto key_schema = StorageTestUtil::RandomSimpleKeySchema(&generator_, COMPACTINTSKEY_MAX_SIZE);

    IndexBuilder builder;
    builder.SetKeySchema(key_schema);
    auto *index = builder.Build();
    EXPECT_EQ(index->KeyKind(), storage::index::IndexKeyKind::COMPACTINTSKEY);
    BasicOps(index);

    delete index;
  }
}

// NOLINTNEXTLINE
TEST_F(IndexKeyTests, GenericKeyBuilderTest) {
  const uint32_t num_iters = 100;

  const std::vector<type::TypeId> generic_key_types{
      type::TypeId::BOOLEAN, type::TypeId::TINYINT,  type::TypeId::SMALLINT,  type::TypeId::INTEGER,
      type::TypeId::BIGINT,  type::TypeId::DECIMAL,  type::TypeId::TIMESTAMP, type::TypeId::DATE,
      type::TypeId::VARCHAR, type::TypeId::VARBINARY};

  for (uint32_t i = 0; i < num_iters; i++) {
    const auto key_schema = StorageTestUtil::RandomGenericKeySchema(10, generic_key_types, &generator_);

    IndexBuilder builder;
    builder.SetKeySchema(key_schema);
    auto *index = builder.Build();
    EXPECT_EQ(index->KeyKind(), storage::index::IndexKeyKind::GENERICKEY);
    BasicOps(index);

    delete index;
  }
}

// NOLINTNEXTLINE
TEST_F(IndexKeyTests, HashKeyBuilderTest) {
  const uint32_t num_iters = 100;

  for (uint32_t i = 0; i < num_iters; i++) {
    auto key_schema = StorageTestUtil::RandomSimpleKeySchema(&generator_, HASHKEY_MAX_SIZE);

    key_schema.SetType(storage::index::IndexType::HASHMAP);

    IndexBuilder builder;
    builder.SetKeySchema(key_schema);
    auto *index = builder.Build();
    EXPECT_EQ(index->KeyKind(), storage::index::IndexKeyKind::HASHKEY);
    BasicOps(index);

    delete index;
  }
}

/**
 * This test exercises an edge case detected while incorporating the catalog that had a VARCHAR(63) attribute. The
 * IndexBuilder was looking at the user-facing PR size rather than the inlined PR size, so the computation of the key
 * size to use was wrong, but really only manifested for VARLEN attributes that are close to the key size limit. The fix
 * was to add a stronger assert in GenericKey's SetFromProjectedRow for bounds-checking, and adjust the computation in
 * IndexBuilder.
 */
// NOLINTNEXTLINE
TEST_F(IndexKeyTests, GenericKeyBuilderVarlenSizeEdgeCaseTest) {
  std::vector<catalog::IndexSchema::Column> key_cols;
  key_cols.emplace_back("", type::TypeId::VARCHAR, 64, false, parser::ConstantValueExpression(type::TypeId::VARCHAR));
  StorageTestUtil::ForceOid(&(key_cols.back()), catalog::indexkeycol_oid_t(15445));
  const auto key_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, false, false, false, true);

  IndexBuilder builder;
  builder.SetKeySchema(key_schema);
  auto *index = builder.Build();
  BasicOps(index);

  delete index;
}

}  // namespace noisepage::storage::index
