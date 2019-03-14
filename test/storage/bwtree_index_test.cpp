#include <functional>
#include <limits>
#include <random>
#include <vector>
#include "storage/data_table.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/index_builder.h"
#include "storage/record_buffer.h"
#include "transaction/transaction_context.h"
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

template <uint8_t KeySize, typename AttrType, typename Random>
void CompactIntsKeyTest(const uint32_t num_iters, Random *generator) {
  const uint8_t num_cols = KeySize * sizeof(AttrType);
  TERRIER_ASSERT(num_cols <= 32, "You can't have more than 32 TINYINTs in a CompactIntsKey.");

  std::uniform_int_distribution<int8_t> val_dis(std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max());

  // Build two random keys and compare verify that equality and comparator helpers give correct results
  for (uint32_t i = 0; i < num_iters; i++) {
    uint8_t offset = 0;

    auto key1 = CompactIntsKey<KeySize>();
    auto key2 = CompactIntsKey<KeySize>();
    std::vector<int8_t> key1_ref(num_cols);
    std::vector<int8_t> key2_ref(num_cols);

    for (uint8_t j = 0; j < num_cols; j++) {
      const int8_t val1 = val_dis(*generator);
      const int8_t val2 = val_dis(*generator);
      key1.AddInteger(val1, offset);
      key2.AddInteger(val2, offset);
      key1_ref[j] = val1;
      key2_ref[j] = val2;
      offset += sizeof(val1);
    }

    EXPECT_EQ(std::equal_to<CompactIntsKey<KeySize>>()(key1, key2), key1_ref == key2_ref);
    EXPECT_EQ(std::less<CompactIntsKey<KeySize>>()(key1, key2), key1_ref < key2_ref);
  }
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
  // 5. exoect eq(key_A, key_B) == (ref_A == ref_B)
  // 6. expect cmp(key_A, key_B) == (ref_A < ref_B)

  for (uint32_t i = 0; i < num_iterations; i++) {
    // generate random key schema
    auto key_schema = RandomCompactIntsKeySchema(&generator_);
    IndexMetadata metadata(key_schema);
    auto initializer = metadata.GetProjectedRowInitializer();

    // create our projected row buffers
    auto *pr_buffer_A = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *pr_buffer_B = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *pr_A = initializer.InitializeRow(pr_buffer_A);
    auto *pr_B = initializer.InitializeRow(pr_buffer_B);

    // fill our buffers with random data
    const auto data_A = FillProjectedRowWithRandomCompactInts(metadata, pr_A, &generator_);
    const auto data_B = FillProjectedRowWithRandomCompactInts(metadata, pr_B, &generator_);

    // figure out which CompactIntsKey template was instantiated
    // this is unpleasant, but seems to be the cleanest way
    uint16_t key_size = 0;
    for (const auto key : key_schema) {
      key_size += type::TypeUtil::GetTypeSize(key.type_id);
    }
    uint8_t key_type = 0;
    for (uint8_t i = 1; i <= 4; i++) {
      if (key_size <= sizeof(uint64_t) * i) {
        key_type = i;
        break;
      }
    }
    TERRIER_ASSERT(1 <= key_type && key_type <= 4, "CompactIntsKey only has 4 possible KeySizes.");

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

    BasicOps(index, &generator_);

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

    //    BasicOps(index, &generator_);

    delete index;
  }
}
}  // namespace terrier::storage::index
