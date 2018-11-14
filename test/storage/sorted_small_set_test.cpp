#include "bwtree/sorted_small_set.h"
#include <algorithm>
#include <random>
#include <vector>
#include "util/test_harness.h"

namespace terrier {

struct SortedSmallSetTests : public TerrierTest {};

/**
 * Adapted from https://github.com/wangziqi2013/BwTree/blob/master/stl_test/sorted_small_set_test.cpp
 */
// NOLINTNEXTLINE
TEST_F(SortedSmallSetTests, SimpleTest) {
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
