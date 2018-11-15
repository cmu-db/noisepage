#include "bwtree/bwtree.h"
#include <algorithm>
#include <random>
#include <vector>
#include "bwtree/bloom_filter.h"
#include "bwtree/sorted_small_set.h"
#include "util/test_harness.h"

namespace terrier {

struct BwTreeTests : public TerrierTest {};

/**
 * Adapted from https://github.com/wangziqi2013/BwTree/blob/master/stl_test/bloom_filter.cpp
 * Modified to have Inserts live on the heap, because that's required according to Ziqi
 */
// NOLINTNEXTLINE
TEST_F(BwTreeTests, BloomFilterTest) {
  std::vector<int *> loose_pointers;
  const int *buffer[256];

  third_party::bwtree::BloomFilter<int> bf{buffer};

  // Generate input and verify that it's not present in the BloomFilter
  for (int i = 0; i < 256; i++) {
    loose_pointers.emplace_back(new int{i});
    EXPECT_FALSE(bf.Exists(i));
  }

  // Insert into BloomFilter
  for (int i = 0; i < 256; i++) {
    bf.Insert(*loose_pointers[i]);
  }

  // Verify present in BloomFilter
  for (int i = 0; i < 256; i++) {
    EXPECT_TRUE(bf.Exists(i));
  }

  // Clean up
  for (const auto i : loose_pointers) {
    delete i;
  }
}

/**
 * Adapted from https://github.com/wangziqi2013/BwTree/blob/master/stl_test/sorted_small_set_test.cpp
 */
// NOLINTNEXTLINE
TEST_F(BwTreeTests, SortedSmallSetTest) {
  const uint32_t num_inserts = 100;
  int *buffer = new int[num_inserts];

  // Generate randomly permuted input
  std::vector<int> inserts;
  for (uint32_t i = 0; i < num_inserts; i++) {
    inserts.push_back(i);
  }
  std::random_device rd;
  std::shuffle(inserts.begin(), inserts.end(), rd);

  // Insert into a SortedSmallSet and verify that it's sorted at all times
  third_party::bwtree::SortedSmallSet<int> sss{buffer};
  for (const auto i : inserts) {
    sss.Insert(i);
    EXPECT_TRUE(std::is_sorted(sss.GetBegin(), sss.GetEnd()));
  }

  delete[] buffer;
}
}  // namespace terrier
