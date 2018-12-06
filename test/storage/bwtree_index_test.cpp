#include "storage/index/bwtree_index.h"
#include <functional>
#include <limits>
#include <random>
#include <vector>
#include "util/test_harness.h"

namespace terrier {

struct BwTreeIndexTests : public ::terrier::TerrierTest {};

template <uint8_t KeySize, typename Random>
void CompactIntsKeyTest(const uint32_t num_iters, Random *generator) {
  std::uniform_int_distribution<int64_t> val_dis(std::numeric_limits<int64_t>::min(),
                                                 std::numeric_limits<int64_t>::max());

  // Verify that we can instantiate all of the helper classes for this KeySize
  auto equality = storage::index::CompactIntsEqualityChecker<KeySize>();
  UNUSED_ATTRIBUTE auto hasher = storage::index::CompactIntsHasher<KeySize>();
  auto comparator = storage::index::CompactIntsComparator<KeySize>();

  // Build two random keys and compare verify that equality and comparator helpers give correct results
  for (uint32_t i = 0; i < num_iters; i++) {
    uint8_t offset = 0;

    auto key1 = storage::index::CompactIntsKey<KeySize>();
    auto key2 = storage::index::CompactIntsKey<KeySize>();
    std::vector<int64_t> key1_ref(KeySize);
    std::vector<int64_t> key2_ref(KeySize);

    for (uint8_t j = 0; j < KeySize; j++) {
      const int64_t val1 = val_dis(*generator);
      const int64_t val2 = val_dis(*generator);
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

  // Test all 4 KeySizes
  CompactIntsKeyTest<1>(num_iters, &generator);
  CompactIntsKeyTest<2>(num_iters, &generator);
  CompactIntsKeyTest<3>(num_iters, &generator);
  CompactIntsKeyTest<4>(num_iters, &generator);
}

TEST_F(BwTreeIndexTests, CompactIntsKeyBuilderTest) {}

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, BuilderTest) {
  storage::index::Builder builder;
  auto *bwtree = builder.Build();

  delete bwtree;
}
}  // namespace terrier
