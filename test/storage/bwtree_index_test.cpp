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
};

template <typename Random>
KeySchema RandomGenericKeySchema(const uint32_t num_cols, const std::vector<type::TypeId> &types, Random *generator) {
  TERRIER_ASSERT(num_cols > 0, "Must have at least one column in your key schema.");

  std::vector<key_oid_t> key_oids;
  key_oids.reserve(num_cols);

  for (auto i = 0; i < num_cols; i++) {
    key_oids.emplace_back(i);
  }

  std::shuffle(key_oids.begin(), key_oids.end(), *generator);

  KeySchema key_schema;

  for (auto i = 0; i < num_cols; i++) {
    auto key_oid = key_oids[i];
    auto type = *RandomTestUtil::UniformRandomElement(types, generator);
    auto is_nullable = static_cast<bool>(std::uniform_int_distribution(0, 1)(*generator));
    key_schema.emplace_back(key_oid, type, is_nullable);
  }

  return key_schema;
}

template <typename Random>
KeySchema RandomCompactIntsKeySchema(Random *generator) {
  const uint16_t max_bytes = sizeof(uint64_t) * INTSKEY_MAX_SLOTS;
  const auto key_size = std::uniform_int_distribution(static_cast<uint16_t>(1), max_bytes)(*generator);

  const std::vector<type::TypeId> types{type::TypeId::TINYINT, type::TypeId::SMALLINT, type::TypeId::INTEGER,
                                        type::TypeId::BIGINT};  // has to be sorted in ascending type size order

  const uint16_t max_cols = max_bytes;  // could have up to max_bytes TINYINTs
  std::vector<key_oid_t> key_oids;
  key_oids.reserve(max_cols);

  for (auto i = 0; i < max_cols; i++) {
    key_oids.emplace_back(i);
  }

  std::shuffle(key_oids.begin(), key_oids.end(), *generator);

  KeySchema key_schema;

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
    bytes_used += type::TypeUtil::GetTypeSize(type);
  }

  return key_schema;
}

/**
 * Generates random data for the given type and writes it to both attr and reference.
 */
template <typename Random>
void WriteRandomAttribute(type::TypeId type, void *attr, void *reference, Random *generator) {
  std::uniform_int_distribution<int64_t> rng(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
  const auto type_size = type::TypeUtil::GetTypeSize(type);
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
      uint8_t varlen_sizes[] = {2, 10, 20};  // meant to hit the inline (prefix), inline (prefix+content), content cases
      auto varlen_size = varlen_sizes[static_cast<uint8_t>(rng(*generator)) % 3];
      byte *varlen_content = new byte[varlen_size];
      auto random_content = static_cast<int64_t>(rng(*generator));
      std::memcpy(varlen_content, &random_content, varlen_size);
      VarlenEntry varlen_entry{};
      if (varlen_size <= VarlenEntry::InlineThreshold()) {
        varlen_entry = VarlenEntry::CreateInline(varlen_content, varlen_size);
      } else {
        varlen_entry = VarlenEntry::Create(varlen_content, varlen_size, true);
      }
      std::memcpy(attr, &varlen_entry, type_size);
      std::memcpy(reference, &varlen_entry, type_size);
      break;
    }
    default:
      throw new std::runtime_error("Unsupported type");
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

  byte *reference = new byte[key_size];
  uint32_t offset = 0;
  for (const auto &key : key_schema) {
    auto attr = pr->AccessForceNotNull(static_cast<uint16_t>(oid_offset_map.at(key.key_oid)));
    WriteRandomAttribute(key.type_id, attr, reference + offset, generator);
    offset += type::TypeUtil::GetTypeSize(key.type_id);
  }
  return reference;
}

/**
 * This function, strictly speaking, is a less-general version of the FillProjectedRow above.
 * But it is easier to reason about and useful in a debugger, so we keep it around.
 */
template <typename Random>
std::vector<int64_t> FillProjectedRowWithRandomCompactInts(const IndexMetadata &metadata, storage::ProjectedRow *pr,
                                                           Random *generator) {
  std::uniform_int_distribution<int64_t> rng(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
  const auto &key_schema = metadata.GetKeySchema();
  const auto &oid_offset_map = metadata.GetKeyOidToOffsetMap();

  std::vector<int64_t> data;
  data.reserve(key_schema.size());
  for (const auto &key : key_schema) {
    const auto type_size = type::TypeUtil::GetTypeSize(key.type_id);
    int64_t rand_int;

    switch (key.type_id) {
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

    auto attr = pr->AccessForceNotNull(static_cast<uint16_t>(oid_offset_map.at(key.key_oid)));
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
    std::uniform_int_distribution<uint16_t> rng(0, key_schema.size() - 1);
    const auto column = rng(*generator);

    uint16_t offset = 0;
    for (uint32_t i = 0; i < column; i++) {
      offset += type::TypeUtil::GetTypeSize(key_schema[i].type_id);
    }

    const auto type = key_schema[column].type_id;
    const auto type_size = type::TypeUtil::GetTypeSize(type);

    auto attr = pr->AccessForceNotNull(static_cast<uint16_t>(oid_offset_map.at(key_schema[column].key_oid)));
    byte *old_value = new byte[type_size];
    std::memcpy(old_value, attr, type_size);
    // force the value to change
    while (std::memcmp(old_value, attr, type_size) == 0) {
      WriteRandomAttribute(type, attr, reference + offset, generator);
    }
    delete[] old_value;
    return true;
  }

  return false;
}

template <uint8_t KeySize, typename AttrType, typename Random>
void CompactIntsKeyTest(const uint32_t num_iters, Random *generator) {
  //  const uint8_t num_cols = KeySize * sizeof(AttrType);
  //  TERRIER_ASSERT(num_cols <= 32, "You can't have more than 32 TINYINTs in a CompactIntsKey.");
  //
  //  std::uniform_int_distribution<int8_t> val_dis(std::numeric_limits<int8_t>::min(),
  //  std::numeric_limits<int8_t>::max());
  //
  //  // Build two random keys and compare verify that equality and comparator helpers give correct results
  //  for (uint32_t i = 0; i < num_iters; i++) {
  //    uint8_t offset = 0;
  //
  //    auto key1 = CompactIntsKey<KeySize>();
  //    auto key2 = CompactIntsKey<KeySize>();
  //    std::vector<int8_t> key1_ref(num_cols);
  //    std::vector<int8_t> key2_ref(num_cols);
  //
  //    for (uint8_t j = 0; j < num_cols; j++) {
  //      const int8_t val1 = val_dis(*generator);
  //      const int8_t val2 = val_dis(*generator);
  //      key1.AddInteger(val1, offset);
  //      key2.AddInteger(val2, offset);
  //      key1_ref[j] = val1;
  //      key2_ref[j] = val2;
  //      offset += sizeof(val1);
  //    }
  //
  //    EXPECT_EQ(std::equal_to<CompactIntsKey<KeySize>>()(key1, key2), key1_ref == key2_ref);
  //    EXPECT_EQ(std::less<CompactIntsKey<KeySize>>()(key1, key2), key1_ref < key2_ref);
  //  }
}

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, CompactIntsKeyBasicTest) {
  const uint32_t num_iters = 100000;

  CompactIntsKeyTest<1, int8_t>(num_iters, &generator_);
  CompactIntsKeyTest<1, int16_t>(num_iters, &generator_);
  CompactIntsKeyTest<1, int32_t>(num_iters, &generator_);
  CompactIntsKeyTest<1, int64_t>(num_iters, &generator_);

  CompactIntsKeyTest<2, int8_t>(num_iters, &generator_);
  CompactIntsKeyTest<2, int16_t>(num_iters, &generator_);
  CompactIntsKeyTest<2, int32_t>(num_iters, &generator_);
  CompactIntsKeyTest<2, int64_t>(num_iters, &generator_);

  CompactIntsKeyTest<3, int8_t>(num_iters, &generator_);
  CompactIntsKeyTest<3, int16_t>(num_iters, &generator_);
  CompactIntsKeyTest<3, int32_t>(num_iters, &generator_);
  CompactIntsKeyTest<3, int64_t>(num_iters, &generator_);

  CompactIntsKeyTest<4, int8_t>(num_iters, &generator_);   // test 32 int8_ts
  CompactIntsKeyTest<4, int16_t>(num_iters, &generator_);  // test 16 int16_ts
  CompactIntsKeyTest<4, int32_t>(num_iters, &generator_);  // test 8 int32_ts
  CompactIntsKeyTest<4, int64_t>(num_iters, &generator_);  // test 4 int64_ts
}

template <typename Random>
void BasicOps(Index *const index, const Random &generator) {
  //  auto initializer = index->GetProjectedRowInitializer();
  //
  //  auto *key_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  //
  //  auto *key = initializer.InitializeRow(key_buffer);
  //
  //  const auto &cmp_order = index->GetComparisonOrder();
  //
  //  for (uint16_t j = 0; j < cmp_order.size(); j++) {
  //    key->AccessForceNotNull(j);
  //  }
  //
  //  std::vector<storage::TupleSlot> results;
  //  index->ScanKey(*key, &results);
  //  EXPECT_TRUE(results.empty());
  //
  //  EXPECT_TRUE(index->Insert(*key, storage::TupleSlot()));
  //
  //  index->ScanKey(*key, &results);
  //  EXPECT_EQ(results.size(), 1);
  //  EXPECT_EQ(results[0], storage::TupleSlot());
  //
  //  EXPECT_TRUE(index->Delete(*key, storage::TupleSlot()));
  //
  //  results.clear();
  //  index->ScanKey(*key, &results);
  //  EXPECT_TRUE(results.empty());
  //
  //  delete[] key_buffer;
}

template <uint8_t KeySize>
bool CompactIntsFromProjectedRowEq(const IndexMetadata &metadata, const storage::ProjectedRow &pr_A,
                                   const storage::ProjectedRow &pr_B) {
  auto key_A = CompactIntsKey<KeySize>();
  auto key_B = CompactIntsKey<KeySize>();
  key_A.SetFromProjectedRow(pr_A, metadata);
  key_B.SetFromProjectedRow(pr_B, metadata);
  return std::equal_to<CompactIntsKey<KeySize>>()(key_A, key_B);
}

template <uint8_t KeySize>
bool CompactIntsFromProjectedRowCmp(const IndexMetadata &metadata, const storage::ProjectedRow &pr_A,
                                    const storage::ProjectedRow &pr_B) {
  auto key_A = CompactIntsKey<KeySize>();
  auto key_B = CompactIntsKey<KeySize>();
  key_A.SetFromProjectedRow(pr_A, metadata);
  key_B.SetFromProjectedRow(pr_B, metadata);
  return std::less<CompactIntsKey<KeySize>>()(key_A, key_B);
}

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, RandomCompactIntsKeyTest) {
  const uint32_t num_iterations = 1000;
  // 1. generate a reference key schema
  // 2. fill two keys pr_A and pr_B with random data_A and data_B
  // 3. use pr_A and pr_B to set CompactIntsKeys key_A and key_B
  // 5. expect eq(key_A, key_B) == (ref_A == ref_B)
  // 6. expect cmp(key_A, key_B) == (ref_A < ref_B)

  for (uint32_t i = 0; i < num_iterations; i++) {
    // generate random key schema
    auto key_schema = RandomCompactIntsKeySchema(&generator_);
    IndexMetadata metadata(key_schema);
    const auto &initializer = metadata.GetProjectedRowInitializer();

    // figure out which CompactIntsKey template was instantiated
    // this is unpleasant, but seems to be the cleanest way
    uint16_t key_size = 0;
    for (const auto key : key_schema) {
      key_size += type::TypeUtil::GetTypeSize(key.type_id);
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

      for (uint8_t pr_i = 0; pr_i < 3; pr_i++) {
        // have B copy A
        std::memcpy(pr_B, pr_A, initializer.ProjectedRowSize());
        byte *data_B = new byte[key_size];
        std::memcpy(data_B, data_A, key_size);

        // modify a column of B with some probability, this also updates the reference data_B
        bool modified = ModifyRandomColumn(metadata, pr_B, data_B, probabilities[pr_i], &generator_);

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

    // BasicOps(index, &generator_);

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

    // BasicOps(index, &generator_);

    delete index;
  }
}

template <typename KeyType, typename CType>
void NumericComparisons(const type::TypeId type_id, const bool nullable) {
  KeySchema key_schema;
  key_schema.emplace_back(key_oid_t(0), type_id, true);

  const IndexMetadata metadata(key_schema);
  const auto &initializer = metadata.GetProjectedRowInitializer();

  auto *const pr_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *const pr = initializer.InitializeRow(pr_buffer);

  CType data = 15;
  *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
  KeyType key1, key2;
  key1.SetFromProjectedRow(*pr, metadata);
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: 15, rhs: 15
  EXPECT_TRUE(std::equal_to<KeyType>()(key1, key2));
  EXPECT_FALSE(std::less<KeyType>()(key1, key2));

  data = 72;
  *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: 15, rhs: 72
  EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
  EXPECT_TRUE(std::less<KeyType>()(key1, key2));

  data = 116;
  *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata);

  // lhs: 116, rhs: 72
  EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
  EXPECT_FALSE(std::less<KeyType>()(key1, key2));

  if (nullable) {
    pr->SetNull(0);
    key1.SetFromProjectedRow(*pr, metadata);

    // lhs: NULL, rhs: 72
    EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
    EXPECT_TRUE(std::less<KeyType>()(key1, key2));

    key2.SetFromProjectedRow(*pr, metadata);

    // lhs: NULL, rhs: NULL
    EXPECT_TRUE(std::equal_to<KeyType>()(key1, key2));
    EXPECT_FALSE(std::less<KeyType>()(key1, key2));

    data = 15;
    *reinterpret_cast<CType *>(pr->AccessForceNotNull(0)) = data;
    key1.SetFromProjectedRow(*pr, metadata);

    // lhs: 15, rhs: NULL
    EXPECT_FALSE(std::equal_to<KeyType>()(key1, key2));
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
  KeySchema key_schema;
  key_schema.emplace_back(key_oid_t(0), type::TypeId::VARCHAR, true);

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

  // lhs: "john", rhs: "john" (same prefixes, same strings (both <= prefix))
  EXPECT_TRUE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_FALSE(std::less<GenericKey<64>>()(key1, key2));

  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnny), static_cast<uint32_t>(std::strlen(johnny)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: "john", rhs: "johnny" (same prefixes, different string (one <=prefix))
  EXPECT_FALSE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_TRUE(std::less<GenericKey<64>>()(key1, key2));

  key1.SetFromProjectedRow(*pr, metadata);
  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(john), static_cast<uint32_t>(std::strlen(john)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnny", rhs: "john" (same prefixes, different strings (one <=prefix))
  EXPECT_FALSE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_FALSE(std::less<GenericKey<64>>()(key1, key2));

  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnny), static_cast<uint32_t>(std::strlen(johnny)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnny", rhs: "johnny" (same prefixes, same strings (> prefix))
  EXPECT_TRUE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_FALSE(std::less<GenericKey<64>>()(key1, key2));

  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnathan), static_cast<uint32_t>(std::strlen(johnathan)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnathan", rhs: "johnny" (different prefixes, different strings (> prefix))
  EXPECT_FALSE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_TRUE(std::less<GenericKey<64>>()(key1, key2));

  key2.SetFromProjectedRow(*pr, metadata);
  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnny), static_cast<uint32_t>(std::strlen(johnny)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnny", rhs: "johnathan" (different prefixes, different strings (> prefix))
  EXPECT_FALSE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_FALSE(std::less<GenericKey<64>>()(key1, key2));

  pr->SetNull(0);
  key1.SetFromProjectedRow(*pr, metadata);

  // lhs: NULL, rhs: "johnathan"
  EXPECT_FALSE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_TRUE(std::less<GenericKey<64>>()(key1, key2));

  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: NULL, rhs: NULL
  EXPECT_TRUE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_FALSE(std::less<GenericKey<64>>()(key1, key2));

  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnathan), static_cast<uint32_t>(std::strlen(johnathan)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnathan", rhs: NULL
  EXPECT_FALSE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_FALSE(std::less<GenericKey<64>>()(key1, key2));

  delete[] pr_buffer;
}

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, GenericKeyNonInlineVarlenComparisons) {
  KeySchema key_schema;
  key_schema.emplace_back(key_oid_t(0), type::TypeId::VARCHAR, true);

  const IndexMetadata metadata(key_schema);
  const auto &initializer = metadata.GetProjectedRowInitializer();

  auto *const pr_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *const pr = initializer.InitializeRow(pr_buffer);

  char john[5] = "john";
  char johnny[7] = "johnny";
  char johnny_johnny[14] = "johnny_johnny";
  char johnathan_johnathan[20] = "johnathan_johnathan";

  VarlenEntry data = VarlenEntry::Create(reinterpret_cast<byte *>(johnathan_johnathan),
                                         static_cast<uint32_t>(std::strlen(johnathan_johnathan)), false);
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  GenericKey<64> key1, key2;
  key1.SetFromProjectedRow(*pr, metadata);
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnathan_johnathan", rhs: "johnathan_johnathan" (same prefixes, same strings (both non-inline))
  EXPECT_TRUE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_FALSE(std::less<GenericKey<64>>()(key1, key2));

  data = VarlenEntry::Create(reinterpret_cast<byte *>(johnny_johnny), static_cast<uint32_t>(std::strlen(johnny_johnny)),
                             false);
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnathan_johnathan", rhs: "johnny_johnny" (same prefixes, different strings (both non-inline))
  EXPECT_FALSE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_TRUE(std::less<GenericKey<64>>()(key1, key2));

  key1.SetFromProjectedRow(*pr, metadata);
  data = VarlenEntry::Create(reinterpret_cast<byte *>(johnathan_johnathan),
                             static_cast<uint32_t>(std::strlen(johnathan_johnathan)), false);
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnny_johnny", rhs: "johnathan_johnathan" (same prefixes, different strings (both non-inline))
  EXPECT_FALSE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_FALSE(std::less<GenericKey<64>>()(key1, key2));

  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(john), static_cast<uint32_t>(std::strlen(john)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnny_johnny", rhs: "john" (same prefixes, different strings (one <=prefix))
  EXPECT_FALSE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_FALSE(std::less<GenericKey<64>>()(key1, key2));

  key1.SetFromProjectedRow(*pr, metadata);
  data = VarlenEntry::Create(reinterpret_cast<byte *>(johnny_johnny), static_cast<uint32_t>(std::strlen(johnny_johnny)),
                             false);
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: "john", rhs: "johnny_johnny" (same prefixes, different strings (one <=prefix))
  EXPECT_FALSE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_TRUE(std::less<GenericKey<64>>()(key1, key2));

  data = VarlenEntry::CreateInline(reinterpret_cast<byte *>(johnny), static_cast<uint32_t>(std::strlen(johnny)));
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnny", rhs: "johnny_johnny" (same prefixes, different strings (one inline))
  EXPECT_FALSE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_TRUE(std::less<GenericKey<64>>()(key1, key2));

  key2.SetFromProjectedRow(*pr, metadata);
  data = VarlenEntry::Create(reinterpret_cast<byte *>(johnny_johnny), static_cast<uint32_t>(std::strlen(johnny_johnny)),
                             false);
  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key1.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnny_johnny", rhs: "johnny" (same prefixes, different strings (one inline))
  EXPECT_FALSE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_FALSE(std::less<GenericKey<64>>()(key1, key2));

  pr->SetNull(0);
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: "johnny_johnny", rhs: NULL
  EXPECT_FALSE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_FALSE(std::less<GenericKey<64>>()(key1, key2));

  key1.SetFromProjectedRow(*pr, metadata);

  // lhs: NULL, rhs: NULL
  EXPECT_TRUE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_FALSE(std::less<GenericKey<64>>()(key1, key2));

  *reinterpret_cast<VarlenEntry *>(pr->AccessForceNotNull(0)) = data;
  key2.SetFromProjectedRow(*pr, metadata);

  // lhs: NULL, rhs: "johnny_johnny"
  EXPECT_FALSE(std::equal_to<GenericKey<64>>()(key1, key2));
  EXPECT_TRUE(std::less<GenericKey<64>>()(key1, key2));

  delete[] pr_buffer;
}

}  // namespace terrier::storage::index
