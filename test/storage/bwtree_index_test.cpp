#include <functional>
#include <limits>
#include <random>
#include <vector>
#include "storage/data_table.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/index_builder.h"
#include "storage/record_buffer.h"
#include "transaction/transaction_context.h"
#include "util/random_test_util.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"

namespace terrier {

struct BwTreeIndexTests : public ::terrier::TerrierTest {};

// compactintskey
// generickey
// two versions

template <typename Random>
storage::index::KeySchema RandomGenericKeySchema(const uint32_t num_cols, const std::vector<type::TypeId> &types,
                                                 Random *generator) {
  TERRIER_ASSERT(num_cols > 0, "Must have at least one column in your key schema.");

  std::vector<storage::index::key_oid_t> key_oids;
  key_oids.reserve(num_cols);

  for (auto i = 0; i < num_cols; i++) {
    key_oids.emplace_back(i);
  }

  std::shuffle(key_oids.begin(), key_oids.end(), *generator);

  storage::index::KeySchema key_schema;

  for (auto i = 0; i < num_cols; i++) {
    auto key_oid = key_oids[i];
    auto type = *RandomTestUtil::UniformRandomElement(types, generator);
    auto is_nullable = static_cast<bool>(std::uniform_int_distribution(0, 1)(*generator));
    key_schema.emplace_back(key_oid, type, is_nullable);
  }

  return key_schema;
}

template <typename Random>
storage::index::KeySchema RandomCompactIntsKeySchema(Random *generator) {
  const uint16_t max_bytes = sizeof(uint64_t) * INTSKEY_MAX_SLOTS;
  const auto key_size = std::uniform_int_distribution(static_cast<uint16_t>(1), max_bytes)(*generator);

  const std::vector<type::TypeId> types{type::TypeId::TINYINT, type::TypeId::SMALLINT, type::TypeId::INTEGER,
                                        type::TypeId::BIGINT};  // has to be sorted in ascending type size order

  const uint16_t max_cols = max_bytes;  // could have up to max_bytes TINYINTs
  std::vector<storage::index::key_oid_t> key_oids;
  key_oids.reserve(max_cols);

  for (auto i = 0; i < max_cols; i++) {
    key_oids.emplace_back(i);
  }

  std::shuffle(key_oids.begin(), key_oids.end(), *generator);

  storage::index::KeySchema key_schema;

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

template <uint8_t KeySize, typename AttrType, typename Random>
void CompactIntsKeyTest(const uint32_t num_iters, Random *generator) {

  const uint8_t num_cols = KeySize * sizeof(AttrType);
  TERRIER_ASSERT(num_cols <= 32, "You can't have more than 32 TINYINTs in a CompactIntsKey.");

  std::uniform_int_distribution<int8_t> val_dis(std::numeric_limits<int8_t>::min(),
                                                 std::numeric_limits<int8_t>::max());

  // Verify that we can instantiate all of the helper classes for this KeySize
  auto equality = storage::index::CompactIntsEqualityChecker<KeySize>();
  UNUSED_ATTRIBUTE auto hasher = storage::index::CompactIntsHasher<KeySize>();
  auto comparator = storage::index::CompactIntsComparator<KeySize>();

  // Build two random keys and compare verify that equality and comparator helpers give correct results
  for (uint32_t i = 0; i < num_iters; i++) {
    uint8_t offset = 0;

    auto key1 = storage::index::CompactIntsKey<KeySize>();
    auto key2 = storage::index::CompactIntsKey<KeySize>();
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

    EXPECT_EQ(equality(key1, key2), key1_ref == key2_ref);
    EXPECT_EQ(comparator(key1, key2), key1_ref < key2_ref);
  }
}

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, CompactIntsKeyBasicTest) {
  const uint32_t num_iters = 100000;
  std::default_random_engine generator;

  CompactIntsKeyTest<1, int8_t>(num_iters, &generator);
  CompactIntsKeyTest<1, int16_t>(num_iters, &generator);
  CompactIntsKeyTest<1, int32_t>(num_iters, &generator);
  CompactIntsKeyTest<1, int64_t>(num_iters, &generator);

  CompactIntsKeyTest<2, int8_t>(num_iters, &generator);
  CompactIntsKeyTest<2, int16_t>(num_iters, &generator);
  CompactIntsKeyTest<2, int32_t>(num_iters, &generator);
  CompactIntsKeyTest<2, int64_t>(num_iters, &generator);

  CompactIntsKeyTest<3, int8_t>(num_iters, &generator);
  CompactIntsKeyTest<3, int16_t>(num_iters, &generator);
  CompactIntsKeyTest<3, int32_t>(num_iters, &generator);
  CompactIntsKeyTest<3, int64_t>(num_iters, &generator);

  CompactIntsKeyTest<4, int8_t>(num_iters, &generator);  // test 32 int8_ts
  CompactIntsKeyTest<4, int16_t>(num_iters, &generator);  // test 16 int16_ts
  CompactIntsKeyTest<4, int32_t>(num_iters, &generator);  // test 8 int32_ts
  CompactIntsKeyTest<4, int64_t>(num_iters, &generator);  // test 4 int64_ts

}

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, BuilderTest) {
  const uint32_t num_iters = 100;
  std::default_random_engine generator;

  //  const std::vector<type::TypeId> generic_key_types{
  //      type::TypeId::BOOLEAN,   type::TypeId::TINYINT, type::TypeId::SMALLINT,
  //      type::TypeId::INTEGER,   type::TypeId::BIGINT,  type::TypeId::DECIMAL,
  //      type::TypeId::TIMESTAMP, type::TypeId::DATE,    type::TypeId::VARCHAR};

  for (uint32_t i = 0; i < num_iters; i++) {
    //    const UNUSED_ATTRIBUTE auto ks1 = RandomGenericKeySchema(10, generic_key_types, &generator);
    const auto key_schema = RandomCompactIntsKeySchema(&generator);

    storage::index::IndexBuilder builder;
    builder.SetConstraintType(storage::index::ConstraintType::DEFAULT)
        .SetKeySchema(key_schema)
        .SetOid(catalog::index_oid_t(i));
    auto *index = builder.Build();

    auto initializer = index->GetProjectedRowInitializer();

    auto *key_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());

    auto *key = initializer.InitializeRow(key_buffer);

    const auto &cmp_order = index->GetComparisonOrder();

    for (uint16_t j = 0; j < cmp_order.size(); j++) {
      EXPECT_EQ(cmp_order[j], !(key->ColumnIds()[j]));
      key->AccessForceNotNull(j);
    }

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

    delete index;
  }
}
}  // namespace terrier
