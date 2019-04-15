#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>
#include <random>
#include <vector>
#include "portable_endian/portable_endian.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/index_builder.h"
#include "storage/projected_row.h"
#include "type/type_id.h"
#include "type/type_util.h"
#include "util/random_test_util.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"

namespace terrier::storage::index {

class BwTreeIndexTests : public TerrierTest {
 public:
  std::default_random_engine generator_;
  std::vector<byte *> loose_pointers_;

  /**
   * Generates a random GenericKey-compatible schema with the given number of columns using the given types.
   */
  template <typename Random>
  IndexKeySchema RandomGenericKeySchema(const uint32_t num_cols, const std::vector<type::TypeId> &types,
                                        Random *generator) {
    uint32_t max_varlen_size = 20;
    TERRIER_ASSERT(num_cols > 0, "Must have at least one column in your key schema.");

    std::vector<catalog::indexkeycol_oid_t> key_oids;
    key_oids.reserve(num_cols);

    for (uint32_t i = 0; i < num_cols; i++) {
      key_oids.emplace_back(i);
    }

    std::shuffle(key_oids.begin(), key_oids.end(), *generator);

    IndexKeySchema key_schema;

    for (uint32_t i = 0; i < num_cols; i++) {
      auto key_oid = key_oids[i];
      auto type = *RandomTestUtil::UniformRandomElement(types, generator);
      auto is_nullable = static_cast<bool>(std::uniform_int_distribution(0, 1)(*generator));

      switch (type) {
        case type::TypeId::VARBINARY:
        case type::TypeId::VARCHAR: {
          auto varlen_size = std::uniform_int_distribution(0u, max_varlen_size)(*generator);
          key_schema.emplace_back(key_oid, type, is_nullable, varlen_size);
          break;
        }
        default:
          key_schema.emplace_back(key_oid, type, is_nullable);
          break;
      }
    }

    return key_schema;
  }

  /**
   * Generates a random CompactIntsKey-compatible schema.
   */
  template <typename Random>
  IndexKeySchema RandomCompactIntsKeySchema(Random *generator) {
    const uint16_t max_bytes = sizeof(uint64_t) * INTSKEY_MAX_SLOTS;
    const auto key_size = std::uniform_int_distribution(static_cast<uint16_t>(1), max_bytes)(*generator);

    const std::vector<type::TypeId> types{type::TypeId::TINYINT, type::TypeId::SMALLINT, type::TypeId::INTEGER,
                                          type::TypeId::BIGINT};  // has to be sorted in ascending type size order

    const uint16_t max_cols = max_bytes;  // could have up to max_bytes TINYINTs
    std::vector<catalog::indexkeycol_oid_t> key_oids;
    key_oids.reserve(max_cols);

    for (auto i = 0; i < max_cols; i++) {
      key_oids.emplace_back(i);
    }

    std::shuffle(key_oids.begin(), key_oids.end(), *generator);

    IndexKeySchema key_schema;

    uint8_t col = 0;

    for (uint16_t bytes_used = 0; bytes_used != key_size;) {
      auto max_offset = static_cast<uint8_t>(types.size() - 1);
      for (const auto &type : types) {
        if (key_size - bytes_used < type::TypeUtil::GetTypeSize(type)) {
          max_offset--;
        }
      }
      const uint8_t type_offset = std::uniform_int_distribution(static_cast<uint8_t>(0), max_offset)(*generator);
      const auto type = types[type_offset];

      key_schema.emplace_back(key_oids[col++], type, false);
      bytes_used = static_cast<uint16_t>(bytes_used + type::TypeUtil::GetTypeSize(type));
    }

    return key_schema;
  }

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

  /**
   * Strictly speaking, this function is a less general version of FillProjectedRow above.
   * But it is easier to reason about and useful in a debugger, so we keep it around.
   */
  template <typename Random>
  std::vector<int64_t> FillProjectedRowWithRandomCompactInts(const IndexMetadata &metadata, storage::ProjectedRow *pr,
                                                             Random *generator) {
    std::uniform_int_distribution<int64_t> rng(std::numeric_limits<int64_t>::min(),
                                               std::numeric_limits<int64_t>::max());
    const auto &key_schema = metadata.GetKeySchema();
    const auto &oid_offset_map = metadata.GetKeyOidToOffsetMap();

    std::vector<int64_t> data;
    data.reserve(key_schema.size());
    for (const auto &key : key_schema) {
      auto key_type = key.GetType();
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

      auto pr_offset = static_cast<uint16_t>(oid_offset_map.at(key.GetOid()));
      auto attr = pr->AccessForceNotNull(pr_offset);
      std::memcpy(attr, &rand_int, type_size);
      data.emplace_back(rand_int);
    }
    return data;
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
      const auto &key_schema = metadata.GetKeySchema();
      std::uniform_int_distribution<uint16_t> rng(0, static_cast<uint16_t>(key_schema.size() - 1));
      const auto column = rng(*generator);

      uint16_t offset = 0;
      for (uint32_t i = 0; i < column; i++) {
        offset = static_cast<uint16_t>(offset + type::TypeUtil::GetTypeSize(key_schema[i].GetType()));
      }

      const auto type = key_schema[column].GetType();
      const auto type_size = type::TypeUtil::GetTypeSize(type);

      auto pr_offset = static_cast<uint16_t>(oid_offset_map.at(key_schema[column].GetOid()));
      auto attr = pr->AccessForceNotNull(pr_offset);
      auto *old_value = new byte[type_size];
      std::memcpy(old_value, attr, type_size);
      // force the value to change
      while (std::memcmp(old_value, attr, type_size) == 0) {
        WriteRandomAttribute(key_schema[column], attr, reference + offset, generator);
      }
      delete[] old_value;
      return true;
    }

    return false;
  }

  /**
   * Tests:
   * 1. Scan -> Insert -> Scan -> Delete -> Scan
   * 2. If primary key or unique index, 2 x Conditional Insert -> Scan -> Delete -> Scan
   */
  void BasicOps(Index *const index) {
    // instantiate projected row and key
    const auto &metadata = index->metadata_;
    const auto &initializer = metadata.GetProjectedRowInitializer();
    auto *key_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *key = initializer.InitializeRow(key_buffer);

    auto *ref = FillProjectedRow(metadata, key, &generator_);
    delete[] ref;

    // 1. Scan -> Insert -> Scan -> Delete -> Scan
    std::vector<storage::TupleSlot> results;
    index->ScanKey(*key, &results);
    EXPECT_TRUE(results.empty());

    EXPECT_TRUE(index->Insert(*key, storage::TupleSlot()));

    index->ScanKey(*key, &results);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], storage::TupleSlot());

    EXPECT_TRUE(index->Delete(*key, storage::TupleSlot()));

    results.clear();
    index->ScanKey(*key, &results);
    EXPECT_TRUE(results.empty());

    // 2. If primary key or unique index, 2 x Conditional Insert -> Scan -> Delete -> Scan
    switch (index->GetConstraintType()) {
      case ConstraintType::UNIQUE: {
        EXPECT_TRUE(index->ConditionalInsert(*key, storage::TupleSlot(), [](const TupleSlot &) { return false; }));
        EXPECT_TRUE(index->ConditionalInsert(*key, storage::TupleSlot(), [](const TupleSlot &) { return false; }));

        results.clear();
        index->ScanKey(*key, &results);
        EXPECT_TRUE(results.empty());
        EXPECT_EQ(results.size(), 2);

        EXPECT_TRUE(index->Delete(*key, storage::TupleSlot()));

        results.clear();
        index->ScanKey(*key, &results);
        EXPECT_TRUE(results.empty());
        EXPECT_EQ(results.size(), 2);
      }
      default:
        break;
    }

    delete[] key_buffer;
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
      (*key).SetFromProjectedRow(*pr, metadata);
    } else {
      pr->SetNull(0);
      (*key).SetFromProjectedRow(*pr, metadata);
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
    TerrierTest::TearDown();
  }
};

/**
 * Calls compact ints equality for KeySize, i.e. std::equal_to<CompactIntsKey<KeySize>>()
 */
template <uint8_t KeySize>
bool CompactIntsFromProjectedRowEq(const IndexMetadata &metadata, const storage::ProjectedRow &pr_A,
                                   const storage::ProjectedRow &pr_B) {
  auto key_A = CompactIntsKey<KeySize>();
  auto key_B = CompactIntsKey<KeySize>();
  key_A.SetFromProjectedRow(pr_A, metadata);
  key_B.SetFromProjectedRow(pr_B, metadata);
  return std::equal_to<CompactIntsKey<KeySize>>()(key_A, key_B);
}

/**
 * Calls compact ints comparator for KeySize, i.e. std::less<CompactIntsKey<KeySize>>()
 */
template <uint8_t KeySize>
bool CompactIntsFromProjectedRowCmp(const IndexMetadata &metadata, const storage::ProjectedRow &pr_A,
                                    const storage::ProjectedRow &pr_B) {
  auto key_A = CompactIntsKey<KeySize>();
  auto key_B = CompactIntsKey<KeySize>();
  key_A.SetFromProjectedRow(pr_A, metadata);
  key_B.SetFromProjectedRow(pr_B, metadata);
  return std::less<CompactIntsKey<KeySize>>()(key_A, key_B);
}

// Test that we generate the right metadata for CompactIntsKey compatible schemas
// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, IndexMetadataCompactIntsKeyTest) {
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
  IndexKeySchema key_schema;

  // key_schema            {INTEGER, INTEGER, BIGINT, TINYINT, SMALLINT}
  // oids                  {20, 21, 22, 23, 24}
  key_schema.emplace_back(oid++, type::TypeId::INTEGER, false);
  key_schema.emplace_back(oid++, type::TypeId::INTEGER, false);
  key_schema.emplace_back(oid++, type::TypeId::BIGINT, false);
  key_schema.emplace_back(oid++, type::TypeId::TINYINT, false);
  key_schema.emplace_back(oid++, type::TypeId::SMALLINT, false);

  IndexMetadata metadata(key_schema);

  // identical key schema
  const auto &metadata_key_schema = metadata.GetKeySchema();
  EXPECT_EQ(metadata_key_schema.size(), 5);
  EXPECT_EQ(metadata_key_schema[0].GetType(), type::TypeId::INTEGER);
  EXPECT_EQ(metadata_key_schema[1].GetType(), type::TypeId::INTEGER);
  EXPECT_EQ(metadata_key_schema[2].GetType(), type::TypeId::BIGINT);
  EXPECT_EQ(metadata_key_schema[3].GetType(), type::TypeId::TINYINT);
  EXPECT_EQ(metadata_key_schema[4].GetType(), type::TypeId::SMALLINT);
  EXPECT_EQ(!metadata_key_schema[0].GetOid(), 20);
  EXPECT_EQ(!metadata_key_schema[1].GetOid(), 21);
  EXPECT_EQ(!metadata_key_schema[2].GetOid(), 22);
  EXPECT_EQ(!metadata_key_schema[3].GetOid(), 23);
  EXPECT_EQ(!metadata_key_schema[4].GetOid(), 24);
  EXPECT_FALSE(metadata_key_schema[0].IsNullable());
  EXPECT_FALSE(metadata_key_schema[1].IsNullable());
  EXPECT_FALSE(metadata_key_schema[2].IsNullable());
  EXPECT_FALSE(metadata_key_schema[3].IsNullable());
  EXPECT_FALSE(metadata_key_schema[4].IsNullable());

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
TEST_F(BwTreeIndexTests, IndexMetadataGenericKeyNoMustInlineVarlenTest) {
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
  IndexKeySchema key_schema;

  // key_schema            {INTEGER, VARCHAR(8), VARCHAR(0), TINYINT, VARCHAR(12)}
  // oids                  {20, 21, 22, 23, 24}
  key_schema.emplace_back(oid++, type::TypeId::INTEGER, false);
  key_schema.emplace_back(oid++, type::TypeId::VARCHAR, false, 8);
  key_schema.emplace_back(oid++, type::TypeId::VARCHAR, false, 0);
  key_schema.emplace_back(oid++, type::TypeId::TINYINT, false);
  key_schema.emplace_back(oid++, type::TypeId::VARCHAR, false, 12);

  IndexMetadata metadata(key_schema);

  // identical key schema
  const auto &metadata_key_schema = metadata.GetKeySchema();
  EXPECT_EQ(metadata_key_schema.size(), 5);
  EXPECT_EQ(metadata_key_schema[0].GetType(), type::TypeId::INTEGER);
  EXPECT_EQ(metadata_key_schema[1].GetType(), type::TypeId::VARCHAR);
  EXPECT_EQ(metadata_key_schema[2].GetType(), type::TypeId::VARCHAR);
  EXPECT_EQ(metadata_key_schema[3].GetType(), type::TypeId::TINYINT);
  EXPECT_EQ(metadata_key_schema[4].GetType(), type::TypeId::VARCHAR);
  EXPECT_EQ(!metadata_key_schema[0].GetOid(), 20);
  EXPECT_EQ(!metadata_key_schema[1].GetOid(), 21);
  EXPECT_EQ(!metadata_key_schema[2].GetOid(), 22);
  EXPECT_EQ(!metadata_key_schema[3].GetOid(), 23);
  EXPECT_EQ(!metadata_key_schema[4].GetOid(), 24);
  EXPECT_FALSE(metadata_key_schema[0].IsNullable());
  EXPECT_FALSE(metadata_key_schema[1].IsNullable());
  EXPECT_FALSE(metadata_key_schema[2].IsNullable());
  EXPECT_FALSE(metadata_key_schema[3].IsNullable());
  EXPECT_FALSE(metadata_key_schema[4].IsNullable());
  EXPECT_EQ(metadata_key_schema[1].GetMaxVarlenSize(), 8);
  EXPECT_EQ(metadata_key_schema[2].GetMaxVarlenSize(), 0);
  EXPECT_EQ(metadata_key_schema[4].GetMaxVarlenSize(), 12);

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
TEST_F(BwTreeIndexTests, IndexMetadataGenericKeyMustInlineVarlenTest) {
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
  IndexKeySchema key_schema;

  // key_schema            {INTEGER, VARCHAR(50), VARCHAR(8), TINYINT, VARCHAR(90)}
  // oids                  {20, 21, 22, 23, 24}
  key_schema.emplace_back(oid++, type::TypeId::INTEGER, false);
  key_schema.emplace_back(oid++, type::TypeId::VARCHAR, false, 50);
  key_schema.emplace_back(oid++, type::TypeId::VARCHAR, false, 8);
  key_schema.emplace_back(oid++, type::TypeId::TINYINT, false);
  key_schema.emplace_back(oid++, type::TypeId::VARCHAR, false, 90);

  IndexMetadata metadata(key_schema);

  // identical key schema
  const auto &metadata_key_schema = metadata.GetKeySchema();
  EXPECT_EQ(metadata_key_schema.size(), 5);
  EXPECT_EQ(metadata_key_schema[0].GetType(), type::TypeId::INTEGER);
  EXPECT_EQ(metadata_key_schema[1].GetType(), type::TypeId::VARCHAR);
  EXPECT_EQ(metadata_key_schema[2].GetType(), type::TypeId::VARCHAR);
  EXPECT_EQ(metadata_key_schema[3].GetType(), type::TypeId::TINYINT);
  EXPECT_EQ(metadata_key_schema[4].GetType(), type::TypeId::VARCHAR);
  EXPECT_EQ(!metadata_key_schema[0].GetOid(), 20);
  EXPECT_EQ(!metadata_key_schema[1].GetOid(), 21);
  EXPECT_EQ(!metadata_key_schema[2].GetOid(), 22);
  EXPECT_EQ(!metadata_key_schema[3].GetOid(), 23);
  EXPECT_EQ(!metadata_key_schema[4].GetOid(), 24);
  EXPECT_FALSE(metadata_key_schema[0].IsNullable());
  EXPECT_FALSE(metadata_key_schema[1].IsNullable());
  EXPECT_FALSE(metadata_key_schema[2].IsNullable());
  EXPECT_FALSE(metadata_key_schema[3].IsNullable());
  EXPECT_FALSE(metadata_key_schema[4].IsNullable());
  EXPECT_EQ(metadata_key_schema[1].GetMaxVarlenSize(), 50);
  EXPECT_EQ(metadata_key_schema[2].GetMaxVarlenSize(), 8);
  EXPECT_EQ(metadata_key_schema[4].GetMaxVarlenSize(), 90);

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
TEST_F(BwTreeIndexTests, RandomCompactIntsKeyTest) {
  const uint32_t num_iterations = 1000;

  for (uint32_t i = 0; i < num_iterations; i++) {
    // generate random key schema
    auto key_schema = RandomCompactIntsKeySchema(&generator_);
    IndexMetadata metadata(key_schema);
    const auto &initializer = metadata.GetProjectedRowInitializer();

    // figure out which CompactIntsKey template was instantiated
    // this is unpleasant, but seems to be the cleanest way
    uint16_t key_size = 0;
    for (const auto key : key_schema) {
      key_size = static_cast<uint16_t>(key_size + type::TypeUtil::GetTypeSize(key.GetType()));
    }
    uint8_t key_type = 0;
    for (uint8_t j = 1; j <= 4; j++) {
      if (key_size <= sizeof(uint64_t) * j) {
        key_type = j;
        break;
      }
    }
    TERRIER_ASSERT(1 <= key_type && key_type <= 4, "CompactIntsKey only has 4 possible KeySizes.");

    // create our projected row buffers
    auto *pr_buffer_A = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *pr_buffer_B = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *pr_A = initializer.InitializeRow(pr_buffer_A);
    auto *pr_B = initializer.InitializeRow(pr_buffer_B);
    //
    for (uint8_t j = 0; j < 10; j++) {
      // fill our buffers with random data
      const auto data_A = FillProjectedRowWithRandomCompactInts(metadata, pr_A, &generator_);
      const auto data_B = FillProjectedRowWithRandomCompactInts(metadata, pr_B, &generator_);

      // perform the relevant checks
      switch (key_type) {
        case 1:
          EXPECT_EQ(CompactIntsFromProjectedRowEq<1>(metadata, *pr_A, *pr_B), data_A == data_B);
          EXPECT_EQ(CompactIntsFromProjectedRowCmp<1>(metadata, *pr_A, *pr_B), data_A < data_B);
          break;
        case 2:
          EXPECT_EQ(CompactIntsFromProjectedRowEq<2>(metadata, *pr_A, *pr_B), data_A == data_B);
          EXPECT_EQ(CompactIntsFromProjectedRowCmp<2>(metadata, *pr_A, *pr_B), data_A < data_B);
          break;
        case 3:
          EXPECT_EQ(CompactIntsFromProjectedRowEq<3>(metadata, *pr_A, *pr_B), data_A == data_B);
          EXPECT_EQ(CompactIntsFromProjectedRowCmp<3>(metadata, *pr_A, *pr_B), data_A < data_B);
          break;
        case 4:
          EXPECT_EQ(CompactIntsFromProjectedRowEq<4>(metadata, *pr_A, *pr_B), data_A == data_B);
          EXPECT_EQ(CompactIntsFromProjectedRowCmp<4>(metadata, *pr_A, *pr_B), data_A < data_B);
          break;
        default:
          throw std::runtime_error("Invalid compact ints key type.");
      }
    }

    for (uint8_t j = 0; j < 10; j++) {
      // fill buffer pr_A with random data data_A
      const auto data_A = FillProjectedRow(metadata, pr_A, &generator_);
      float probabilities[] = {0.0, 0.5, 1.0};

      for (float prob : probabilities) {
        // have B copy A
        std::memcpy(pr_B, pr_A, initializer.ProjectedRowSize());
        auto *data_B = new byte[key_size];
        std::memcpy(data_B, data_A, key_size);

        // modify a column of B with some probability, this also updates the reference data_B
        bool modified = ModifyRandomColumn(metadata, pr_B, data_B, prob, &generator_);

        // perform the relevant checks
        switch (key_type) {
          case 1:
            EXPECT_EQ(CompactIntsFromProjectedRowEq<1>(metadata, *pr_A, *pr_B),
                      std::memcmp(data_A, data_B, key_size) == 0 && !modified);
            EXPECT_EQ(CompactIntsFromProjectedRowCmp<1>(metadata, *pr_A, *pr_B),
                      std::memcmp(data_A, data_B, key_size) < 0);
            break;
          case 2:
            EXPECT_EQ(CompactIntsFromProjectedRowEq<2>(metadata, *pr_A, *pr_B),
                      std::memcmp(data_A, data_B, key_size) == 0 && !modified);
            EXPECT_EQ(CompactIntsFromProjectedRowCmp<2>(metadata, *pr_A, *pr_B),
                      std::memcmp(data_A, data_B, key_size) < 0);
            break;
          case 3:
            EXPECT_EQ(CompactIntsFromProjectedRowEq<3>(metadata, *pr_A, *pr_B),
                      std::memcmp(data_A, data_B, key_size) == 0 && !modified);
            EXPECT_EQ(CompactIntsFromProjectedRowCmp<3>(metadata, *pr_A, *pr_B),
                      std::memcmp(data_A, data_B, key_size) < 0);
            break;
          case 4:
            EXPECT_EQ(CompactIntsFromProjectedRowEq<4>(metadata, *pr_A, *pr_B),
                      std::memcmp(data_A, data_B, key_size) == 0 && !modified);
            EXPECT_EQ(CompactIntsFromProjectedRowCmp<4>(metadata, *pr_A, *pr_B),
                      std::memcmp(data_A, data_B, key_size) < 0);
            break;
          default:
            throw std::runtime_error("Invalid compact ints key type.");
        }
        delete[] data_B;
      }

      delete[] data_A;
    }

    delete[] pr_buffer_A;
    delete[] pr_buffer_B;
  }
}

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, CompactIntsBuilderTest) {
  const uint32_t num_iters = 100;

  for (uint32_t i = 0; i < num_iters; i++) {
    const auto key_schema = RandomCompactIntsKeySchema(&generator_);

    IndexBuilder builder;
    builder.SetConstraintType(ConstraintType::DEFAULT).SetKeySchema(key_schema).SetOid(catalog::index_oid_t(i));
    auto *index = builder.Build();
    BasicOps(index);

    delete index;
  }
}

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, GenericKeyBuilderTest) {
  const uint32_t num_iters = 100;

  const std::vector<type::TypeId> generic_key_types{
      type::TypeId::BOOLEAN, type::TypeId::TINYINT,  type::TypeId::SMALLINT,  type::TypeId::INTEGER,
      type::TypeId::BIGINT,  type::TypeId::DECIMAL,  type::TypeId::TIMESTAMP, type::TypeId::DATE,
      type::TypeId::VARCHAR, type::TypeId::VARBINARY};

  for (uint32_t i = 0; i < num_iters; i++) {
    const auto key_schema = RandomGenericKeySchema(10, generic_key_types, &generator_);

    IndexBuilder builder;
    builder.SetConstraintType(ConstraintType::DEFAULT).SetKeySchema(key_schema).SetOid(catalog::index_oid_t(i));
    auto *index = builder.Build();
    BasicOps(index);

    delete index;
  }
}

template <uint8_t KeySize, typename CType, typename Random>
void CompactIntsKeyBasicTest(type::TypeId type_id, Random *const generator) {
  IndexKeySchema key_schema;
  const uint8_t num_cols = (sizeof(uint64_t) * KeySize) / sizeof(CType);

  for (uint8_t i = 0; i < num_cols; i++) {
    key_schema.emplace_back(catalog::indexkeycol_oid_t(i), type_id, false);
  }

  const IndexMetadata metadata(key_schema);
  const auto &initializer = metadata.GetProjectedRowInitializer();

  auto *const pr_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  std::memset(pr_buffer, 0, initializer.ProjectedRowSize());
  auto *const pr = initializer.InitializeRow(pr_buffer);

  for (uint8_t i = 0; i < num_cols; i++) {
    pr->AccessForceNotNull(i);
  }

  CompactIntsKey<KeySize> key1, key2;
  key1.SetFromProjectedRow(*pr, metadata);
  key2.SetFromProjectedRow(*pr, metadata);

  EXPECT_TRUE(std::equal_to<CompactIntsKey<KeySize>>()(key1, key2));
  EXPECT_EQ(std::hash<CompactIntsKey<KeySize>>()(key1), std::hash<CompactIntsKey<KeySize>>()(key2));
  EXPECT_FALSE(std::less<CompactIntsKey<KeySize>>()(key1, key2));

  *reinterpret_cast<CType *>(pr->AccessForceNotNull(num_cols - 1)) = static_cast<CType>(1);
  key2.SetFromProjectedRow(*pr, metadata);

  EXPECT_FALSE(std::equal_to<CompactIntsKey<KeySize>>()(key1, key2));
  EXPECT_NE(std::hash<CompactIntsKey<KeySize>>()(key1), std::hash<CompactIntsKey<KeySize>>()(key2));
  EXPECT_TRUE(std::less<CompactIntsKey<KeySize>>()(key1, key2));

  delete[] pr_buffer;
}

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, CompactIntsKeyBasicTest) {
  CompactIntsKeyBasicTest<1, int8_t>(type::TypeId::TINYINT, &generator_);
  CompactIntsKeyBasicTest<1, int16_t>(type::TypeId::SMALLINT, &generator_);
  CompactIntsKeyBasicTest<1, int32_t>(type::TypeId::INTEGER, &generator_);
  CompactIntsKeyBasicTest<1, int64_t>(type::TypeId::BIGINT, &generator_);

  CompactIntsKeyBasicTest<2, int8_t>(type::TypeId::TINYINT, &generator_);
  CompactIntsKeyBasicTest<2, int16_t>(type::TypeId::SMALLINT, &generator_);
  CompactIntsKeyBasicTest<2, int32_t>(type::TypeId::INTEGER, &generator_);
  CompactIntsKeyBasicTest<2, int64_t>(type::TypeId::BIGINT, &generator_);

  CompactIntsKeyBasicTest<3, int8_t>(type::TypeId::TINYINT, &generator_);
  CompactIntsKeyBasicTest<3, int16_t>(type::TypeId::SMALLINT, &generator_);
  CompactIntsKeyBasicTest<3, int32_t>(type::TypeId::INTEGER, &generator_);
  CompactIntsKeyBasicTest<3, int64_t>(type::TypeId::BIGINT, &generator_);

  CompactIntsKeyBasicTest<4, int8_t>(type::TypeId::TINYINT, &generator_);
  CompactIntsKeyBasicTest<4, int16_t>(type::TypeId::SMALLINT, &generator_);
  CompactIntsKeyBasicTest<4, int32_t>(type::TypeId::INTEGER, &generator_);
  CompactIntsKeyBasicTest<4, int64_t>(type::TypeId::BIGINT, &generator_);
}

template <typename KeyType, typename CType>
void NumericComparisons(const type::TypeId type_id, const bool nullable) {
  IndexKeySchema key_schema;
  key_schema.emplace_back(catalog::indexkeycol_oid_t(0), type_id, true);

  const IndexMetadata metadata(key_schema);
  const auto &initializer = metadata.GetProjectedRowInitializer();

  auto *const pr_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  std::memset(pr_buffer, 0, initializer.ProjectedRowSize());
  auto *const pr = initializer.InitializeRow(pr_buffer);

  CType data = 15;
  *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
  KeyType key1, key2;
  key1.SetFromProjectedRow(*pr, metadata);
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: 15, rhs: 15
  EXPECT_TRUE(std::equal_to<KeyType>()(key1, key2));
  EXPECT_EQ(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));
  EXPECT_FALSE(std::less<KeyType>()(key1, key2));

  data = 72;
  *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: 15, rhs: 72
  EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
  EXPECT_NE(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));
  EXPECT_TRUE(std::less<KeyType>()(key1, key2));

  data = 116;
  *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata);

  // lhs: 116, rhs: 72
  EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
  EXPECT_NE(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));
  EXPECT_FALSE(std::less<KeyType>()(key1, key2));

  if (nullable) {
    pr->SetNull(0);
    key1.SetFromProjectedRow(*pr, metadata);

    // lhs: NULL, rhs: 72
    EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
    EXPECT_NE(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));
    EXPECT_TRUE(std::less<KeyType>()(key1, key2));

    key2.SetFromProjectedRow(*pr, metadata);

    // lhs: NULL, rhs: NULL
    EXPECT_TRUE(std::equal_to<KeyType>()(key1, key2));
    EXPECT_EQ(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));
    EXPECT_FALSE(std::less<KeyType>()(key1, key2));

    data = 15;
    *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
    key1.SetFromProjectedRow(*pr, metadata);

    // lhs: 15, rhs: NULL
    EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
    EXPECT_NE(std::hash<KeyType>()(key1), std::hash<KeyType>()(key2));
    EXPECT_FALSE(std::less<KeyType>()(key1, key2));
  }

  delete[] pr_buffer;
}

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, CompactIntsKeyNumericComparisons) {
  NumericComparisons<CompactIntsKey<1>, int8_t>(type::TypeId::TINYINT, false);
  NumericComparisons<CompactIntsKey<1>, int16_t>(type::TypeId::SMALLINT, false);
  NumericComparisons<CompactIntsKey<1>, int32_t>(type::TypeId::INTEGER, false);
  NumericComparisons<CompactIntsKey<1>, int64_t>(type::TypeId::BIGINT, false);
}

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, GenericKeyNumericComparisons) {
  NumericComparisons<GenericKey<64>, int8_t>(type::TypeId::TINYINT, true);
  NumericComparisons<GenericKey<64>, int16_t>(type::TypeId::SMALLINT, true);
  NumericComparisons<GenericKey<64>, int32_t>(type::TypeId::INTEGER, true);
  NumericComparisons<GenericKey<64>, uint32_t>(type::TypeId::DATE, true);
  NumericComparisons<GenericKey<64>, int64_t>(type::TypeId::BIGINT, true);
  NumericComparisons<GenericKey<64>, double>(type::TypeId::DECIMAL, true);
  NumericComparisons<GenericKey<64>, uint64_t>(type::TypeId::TIMESTAMP, true);
}

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, GenericKeyInlineVarlenComparisons) {
  IndexKeySchema key_schema;
  key_schema.emplace_back(catalog::indexkeycol_oid_t(0), type::TypeId::VARCHAR, true, 12);

  const IndexMetadata metadata(key_schema);
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
  key1.SetFromProjectedRow(*pr, metadata);
  key2.SetFromProjectedRow(*pr, metadata);

  const auto generic_eq64 = std::equal_to<GenericKey<64>>();  // NOLINT transparent functors can't figure out template
  const auto generic_lt64 = std::less<GenericKey<64>>();      // NOLINT transparent functors can't figure out template
  const auto generic_hash64 = std::hash<GenericKey<64>>();    // NOLINT transparent functors can't figure out template

  // lhs: "john", rhs: "john" (same prefixes, same strings (both <= prefix))
  EXPECT_TRUE(generic_eq64(key1, key2));
  EXPECT_EQ(generic_hash64(key1), generic_hash64(key2));
  EXPECT_FALSE(generic_lt64(key1, key2));

  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnny), static_cast<uint32_t>(std::strlen(johnny)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: "john", rhs: "johnny" (same prefixes, different string (one <=prefix))
  EXPECT_FALSE(generic_eq64(key1, key2));
  EXPECT_NE(generic_hash64(key1), generic_hash64(key2));
  EXPECT_TRUE(generic_lt64(key1, key2));

  key1.SetFromProjectedRow(*pr, metadata);
  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(john), static_cast<uint32_t>(std::strlen(john)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnny", rhs: "john" (same prefixes, different strings (one <=prefix))
  EXPECT_FALSE(generic_eq64(key1, key2));
  EXPECT_NE(generic_hash64(key1), generic_hash64(key2));
  EXPECT_FALSE(generic_lt64(key1, key2));

  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnny), static_cast<uint32_t>(std::strlen(johnny)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata);
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnny", rhs: "johnny" (same prefixes, same strings (> prefix))
  EXPECT_TRUE(generic_eq64(key1, key2));
  EXPECT_EQ(generic_hash64(key1), generic_hash64(key2));
  EXPECT_FALSE(generic_lt64(key1, key2));

  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnathan), static_cast<uint32_t>(std::strlen(johnathan)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnathan", rhs: "johnny" (different prefixes, different strings (> prefix))
  EXPECT_FALSE(generic_eq64(key1, key2));
  EXPECT_NE(generic_hash64(key1), generic_hash64(key2));
  EXPECT_TRUE(generic_lt64(key1, key2));

  key2.SetFromProjectedRow(*pr, metadata);
  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnny), static_cast<uint32_t>(std::strlen(johnny)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnny", rhs: "johnathan" (different prefixes, different strings (> prefix))
  EXPECT_FALSE(generic_eq64(key1, key2));
  EXPECT_NE(generic_hash64(key1), generic_hash64(key2));
  EXPECT_FALSE(generic_lt64(key1, key2));

  pr->SetNull(0);
  key1.SetFromProjectedRow(*pr, metadata);

  // lhs: NULL, rhs: "johnathan"
  EXPECT_FALSE(generic_eq64(key1, key2));
  EXPECT_TRUE(generic_lt64(key1, key2));

  key1.SetFromProjectedRow(*pr, metadata);
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: NULL, rhs: NULL
  EXPECT_TRUE(generic_eq64(key1, key2));
  EXPECT_EQ(generic_hash64(key1), generic_hash64(key2));
  EXPECT_FALSE(generic_lt64(key1, key2));

  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnathan), static_cast<uint32_t>(std::strlen(johnathan)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnathan", rhs: NULL
  EXPECT_FALSE(generic_eq64(key1, key2));
  EXPECT_NE(generic_hash64(key1), generic_hash64(key2));
  EXPECT_FALSE(generic_lt64(key1, key2));

  delete[] pr_buffer;
}

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, GenericKeyNonInlineVarlenComparisons) {
  IndexKeySchema key_schema;
  key_schema.emplace_back(catalog::indexkeycol_oid_t(0), type::TypeId::VARCHAR, true, 20);

  const IndexMetadata metadata(key_schema);
  const auto &initializer = metadata.GetProjectedRowInitializer();

  auto *const pr_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *const pr = initializer.InitializeRow(pr_buffer);

  char john[5] = "john";
  char johnny[7] = "johnny";
  char johnny_johnny[14] = "johnny_johnny";
  char johnathan_johnathan[20] = "johnathan_johnathan";

  GenericKey<64> key1, key2;

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
TEST_F(BwTreeIndexTests, BasicScan) {
  IndexKeySchema key_schema;
  key_schema.emplace_back(catalog::indexkeycol_oid_t(0), type::TypeId::INTEGER, false);

  IndexBuilder builder;
  builder.SetConstraintType(ConstraintType::DEFAULT).SetKeySchema(key_schema).SetOid(catalog::index_oid_t(1));
  auto *index = builder.Build();

  const auto &initializer = index->GetProjectedRowInitializer();
  auto *const insert_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *const insert_pr = initializer.InitializeRow(insert_buffer);

  for (uint32_t i = 1; i <= 50; i++) {
    const uint32_t key = i * 2;
    *reinterpret_cast<uint32_t *>(insert_pr->AccessForceNotNull(0)) = key;
    storage::TupleSlot index_value;
    *reinterpret_cast<uint64_t *>(&index_value) = key - 1;
    index->Insert(*insert_pr, index_value);
  }

  std::vector<storage::TupleSlot> results;

  auto *const low_key_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *const low_key_pr = initializer.InitializeRow(low_key_buffer);
  auto *const high_key_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *const high_key_pr = initializer.InitializeRow(high_key_buffer);

  *reinterpret_cast<uint32_t *>(low_key_pr->AccessForceNotNull(0)) = 10;
  *reinterpret_cast<uint32_t *>(high_key_pr->AccessForceNotNull(0)) = 20;
  results.clear();
  index->Scan(*low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 6);  // 10, 12, 14, 16, 18, 20
  uint32_t j = 0;
  for (uint32_t i = 10; i <= 20; i += 2) {
    storage::TupleSlot index_value = results.at(j);
    EXPECT_EQ(*reinterpret_cast<uint64_t *>(&index_value), i - 1);
    j++;
  }

  results.clear();
  *reinterpret_cast<uint32_t *>(low_key_pr->AccessForceNotNull(0)) = 11;
  *reinterpret_cast<uint32_t *>(high_key_pr->AccessForceNotNull(0)) = 20;
  results.clear();
  index->Scan(*low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 5);  // 12, 14, 16, 18, 20
  j = 0;
  for (uint32_t i = 12; i <= 20; i += 2) {
    storage::TupleSlot index_value = results.at(j);
    EXPECT_EQ(*reinterpret_cast<uint64_t *>(&index_value), i - 1);
    j++;
  }

  *reinterpret_cast<uint32_t *>(low_key_pr->AccessForceNotNull(0)) = 10;
  *reinterpret_cast<uint32_t *>(high_key_pr->AccessForceNotNull(0)) = 19;
  results.clear();
  index->Scan(*low_key_pr, *high_key_pr, &results);
  EXPECT_EQ(results.size(), 5);  // 10, 12, 14, 16, 18
  j = 0;
  for (uint32_t i = 10; i <= 18; i += 2) {
    storage::TupleSlot index_value = results.at(j);
    EXPECT_EQ(*reinterpret_cast<uint64_t *>(&index_value), i - 1);
    j++;
  }

  delete[] insert_buffer;
  delete[] low_key_buffer;
  delete[] high_key_buffer;
  delete index;
}

}  // namespace terrier::storage::index
