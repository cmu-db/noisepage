#include "storage/index/bwtree_index.h"
#include <functional>
#include <limits>
#include <random>
#include <vector>
#include "util/test_harness.h"

namespace terrier {

struct BwTreeIndexTests : public ::terrier::TerrierTest {};

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, CompactIntsKeyTest) {
  uint32_t num_iters = 10000;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int64_t> val_dis(std::numeric_limits<int64_t>::min(),
                                                 std::numeric_limits<int64_t>::max());

  auto equality1 = storage::index::CompactIntsEqualityChecker<1>();
  UNUSED_ATTRIBUTE auto hasher1 = storage::index::CompactIntsHasher<1>();
  auto comparator1 = storage::index::CompactIntsComparator<1>();

  for (uint32_t i = 0; i < num_iters; i++) {
    constexpr uint8_t key_size = 1;
    uint8_t offset = 0;

    auto key1 = storage::index::CompactIntsKey<key_size>();
    auto key2 = storage::index::CompactIntsKey<key_size>();
    std::vector<int64_t> key1_ref(key_size);
    std::vector<int64_t> key2_ref(key_size);

    for (uint8_t j = 0; j < key_size; j++) {
      const int64_t val1 = val_dis(gen);
      const int64_t val2 = val_dis(gen);
      key1.AddInteger(val1, offset);
      key2.AddInteger(val2, offset);
      key1_ref[j] = val1;
      key2_ref[j] = val2;
      offset += sizeof(val1);
    }

    EXPECT_EQ(equality1(key1, key2), key1_ref == key2_ref);
    EXPECT_EQ(comparator1(key1, key2), key1_ref < key2_ref);
  }

  auto equality2 = storage::index::CompactIntsEqualityChecker<2>();
  UNUSED_ATTRIBUTE auto hasher2 = storage::index::CompactIntsHasher<2>();
  auto comparator2 = storage::index::CompactIntsComparator<2>();

  for (uint32_t i = 0; i < num_iters; i++) {
    constexpr uint8_t key_size = 2;
    uint8_t offset = 0;

    auto key1 = storage::index::CompactIntsKey<key_size>();
    auto key2 = storage::index::CompactIntsKey<key_size>();
    std::vector<int64_t> key1_ref(key_size);
    std::vector<int64_t> key2_ref(key_size);

    for (uint8_t j = 0; j < key_size; j++) {
      const int64_t val1 = val_dis(gen);
      const int64_t val2 = val_dis(gen);
      key1.AddInteger(val1, offset);
      key2.AddInteger(val2, offset);
      key1_ref[j] = val1;
      key2_ref[j] = val2;
      offset += sizeof(val1);
    }

    EXPECT_EQ(equality2(key1, key2), key1_ref == key2_ref);
    EXPECT_EQ(comparator2(key1, key2), key1_ref < key2_ref);
  }

  auto equality3 = storage::index::CompactIntsEqualityChecker<3>();
  UNUSED_ATTRIBUTE auto hasher3 = storage::index::CompactIntsHasher<3>();
  auto comparator3 = storage::index::CompactIntsComparator<3>();

  for (uint32_t i = 0; i < num_iters; i++) {
    constexpr uint8_t key_size = 3;
    uint8_t offset = 0;

    auto key1 = storage::index::CompactIntsKey<key_size>();
    auto key2 = storage::index::CompactIntsKey<key_size>();
    std::vector<int64_t> key1_ref(key_size);
    std::vector<int64_t> key2_ref(key_size);

    for (uint8_t j = 0; j < key_size; j++) {
      const int64_t val1 = val_dis(gen);
      const int64_t val2 = val_dis(gen);
      key1.AddInteger(val1, offset);
      key2.AddInteger(val2, offset);
      key1_ref[j] = val1;
      key2_ref[j] = val2;
      offset += sizeof(val1);
    }

    EXPECT_EQ(equality3(key1, key2), key1_ref == key2_ref);
    EXPECT_EQ(comparator3(key1, key2), key1_ref < key2_ref);
  }

  auto equality4 = storage::index::CompactIntsEqualityChecker<4>();
  UNUSED_ATTRIBUTE auto hasher4 = storage::index::CompactIntsHasher<4>();
  auto comparator4 = storage::index::CompactIntsComparator<4>();

  for (uint32_t i = 0; i < num_iters; i++) {
    constexpr uint8_t key_size = 4;
    uint8_t offset = 0;

    auto key1 = storage::index::CompactIntsKey<key_size>();
    auto key2 = storage::index::CompactIntsKey<key_size>();
    std::vector<int64_t> key1_ref(key_size);
    std::vector<int64_t> key2_ref(key_size);

    for (uint8_t j = 0; j < key_size; j++) {
      const int64_t val1 = val_dis(gen);
      const int64_t val2 = val_dis(gen);
      key1.AddInteger(val1, offset);
      key2.AddInteger(val2, offset);
      key1_ref[j] = val1;
      key2_ref[j] = val2;
      offset += sizeof(val1);
    }

    EXPECT_EQ(equality4(key1, key2), key1_ref == key2_ref);
    EXPECT_EQ(comparator4(key1, key2), key1_ref < key2_ref);
  }
}

// NOLINTNEXTLINE
TEST_F(BwTreeIndexTests, BuilderTest) {
  storage::index::BwTreeIndex<int64_t, std::less<>, std::equal_to<>, std::hash<int64_t>>::Builder builder;
  auto *bwtree = builder.Build();

  delete bwtree;
}
}  // namespace terrier
