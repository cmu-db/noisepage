#include <algorithm>
#include <random>
#include <vector>
#include "bwtree/bloom_filter.h"
#include "bwtree/sorted_small_set.h"
#include "util/bwtree_test_util.h"
#include "util/test_harness.h"

namespace terrier {

struct BwTreeTests : public TerrierTest {
  void SetUp() override {
    TerrierTest::SetUp();
    third_party::bwtree::print_flag = false;
  }

  void TearDown() override { TerrierTest::TearDown(); }

  /**
   * Adapted from https://github.com/wangziqi2013/BwTree/blob/master/test/test_suite.cpp
   */
  TreeType *GetEmptyTree() const {
    auto *t1 = new TreeType{true, BwTreeTestUtil::KeyComparator{1}, BwTreeTestUtil::KeyEqualityChecker{1}};

    // By default let is serve single thread (i.e. current one)
    // and assign gc_id = 0 to the current thread
    t1->UpdateThreadLocal(1);
    t1->AssignGCID(0);
    return t1;
  }
};

/**
 * Adapted from https://github.com/wangziqi2013/BwTree/blob/master/stl_test/bloom_filter.cpp
 * Modified to have Inserts live on the heap, because that's required according to Ziqi
 */
// NOLINTNEXTLINE
TEST_F(BwTreeTests, BloomFilter) {
  std::vector<uint32_t *> loose_pointers;
  const uint32_t *buffer[256];

  third_party::bwtree::BloomFilter<uint32_t> bf{buffer};

  // Generate input and verify that it's not present in the BloomFilter
  for (uint32_t i = 0; i < 256; i++) {
    loose_pointers.emplace_back(new uint32_t{i});
    EXPECT_FALSE(bf.Exists(i));
  }

  // Insert into BloomFilter
  for (uint32_t i = 0; i < 256; i++) {
    bf.Insert(*loose_pointers[i]);
  }

  // Verify present in BloomFilter
  for (uint32_t i = 0; i < 256; i++) {
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
TEST_F(BwTreeTests, SortedSmallSet) {
  const uint32_t num_inserts = 100;
  auto *buffer = new uint32_t[num_inserts];

  // Generate randomly permuted input
  std::vector<uint32_t> inserts;
  for (uint32_t i = 0; i < num_inserts; i++) {
    inserts.push_back(i);
  }
  std::random_device rd;
  std::shuffle(inserts.begin(), inserts.end(), rd);

  // Insert into a SortedSmallSet and verify that it's sorted at all times
  third_party::bwtree::SortedSmallSet<uint32_t> sss{buffer};
  for (const auto i : inserts) {
    sss.Insert(i);
    EXPECT_TRUE(std::is_sorted(sss.GetBegin(), sss.GetEnd()));
  }

  // Clean up
  delete[] buffer;
}

// NOLINTNEXTLINE
TEST_F(BwTreeTests, Basic) {
  auto *tree = GetEmptyTree();
  delete tree;
}

/**
 * Adapted from https://github.com/wangziqi2013/BwTree/blob/master/test/iterator_test.cpp
 */
// NOLINTNEXTLINE
TEST_F(BwTreeTests, ForwardIterator) {
  auto *tree = GetEmptyTree();
  const int key_num = 1024 * 1024;

  // First insert from 0 to 1 million
  for (int i = 0; i < key_num; i++) {
    tree->Insert(i, i);
  }

  auto it = tree->Begin();

  int64_t i = 0;
  while (!it.IsEnd()) {
    EXPECT_EQ(it->first, it->second);
    EXPECT_EQ(it->first, i);

    i++;
    it++;
  }

  EXPECT_EQ(i, key_num);

  auto it2 = tree->Begin(key_num - 1);
  auto it3 = it2;

  it2++;
  EXPECT_TRUE(it2.IsEnd());

  EXPECT_EQ(it3->first, key_num - 1);

  auto it4 = tree->Begin(key_num + 1);
  EXPECT_TRUE(it4.IsEnd());

  delete tree;
}

/**
 * Adapted from https://github.com/wangziqi2013/BwTree/blob/master/test/iterator_test.cpp
 */
// NOLINTNEXTLINE
TEST_F(BwTreeTests, ReverseIterator) {
  auto *tree = GetEmptyTree();
  const int key_num = 1024 * 1024;

  // First insert from 0 to 1 million
  for (int i = 0; i < key_num; i++) {
    tree->Insert(i, i);
  }

  auto it = tree->Begin(key_num - 1);

  EXPECT_TRUE(!it.IsEnd());
  EXPECT_TRUE(!it.IsBegin());

  // This does not test Begin()
  int64_t key = key_num - 1;
  while (!it.IsBegin()) {
    EXPECT_EQ(it->first, it->second);
    EXPECT_EQ(it->first, key);
    key--;
    it--;
  }

  // Test for Begin()
  EXPECT_EQ(it->first, it->second);
  EXPECT_EQ(it->first, key);
  EXPECT_EQ(key, 0);

  delete tree;
}

}  // namespace terrier
