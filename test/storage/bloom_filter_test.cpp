#include "bwtree/bloom_filter.h"
#include "util/test_harness.h"

namespace terrier {

struct BloomFilterTests : public TerrierTest {
  void SetUp() final {}

  void TearDown() final {
    for (const auto i : loose_pointers) {
      delete i;
    }
    loose_pointers.clear();
  }

  std::vector<int *> loose_pointers;
};

/**
 * Ported from https://github.com/wangziqi2013/BwTree/blob/master/stl_test/bloom_filter.cpp
 * Modified to have Inserts live on the heap, because that's required according to Ziqi
 */
// NOLINTNEXTLINE
TEST_F(BloomFilterTests, SimpleTest) {
  const int *buffer[256];

  third_party::bwtree::BloomFilter<int> bf{buffer};

  for (int i = 0; i < 256; i++) {
    loose_pointers.emplace_back(new int{i});
    EXPECT_FALSE(bf.Exists(i));
  }

  for (int i = 0; i < 256; i++) {
    bf.Insert(*loose_pointers[i]);
  }

  for (int i = 0; i < 256; i++) {
    EXPECT_TRUE(bf.Exists(i));
  }
}
}  // namespace terrier
