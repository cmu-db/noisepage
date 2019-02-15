#include <algorithm>
#include <random>
#include <vector>
#include "bwtree/bloom_filter.h"
#include "bwtree/sorted_small_set.h"
#include "util/bwtree_test_util.h"
#include "util/multithread_test_util.h"
#include "util/test_harness.h"

namespace terrier {

/**
 * These tests are adapted from https://github.com/wangziqi2013/BwTree/tree/master/test
 * Please do not use these as a model for other tests within this repository.
 */
struct BwTreeTests : public TerrierTest {
  void SetUp() override { TerrierTest::SetUp(); }

  void TearDown() override { TerrierTest::TearDown(); }

  const uint32_t num_threads_ =
      MultiThreadTestUtil::HardwareConcurrency() + (MultiThreadTestUtil::HardwareConcurrency() % 2);
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

/**
 * Adapted from https://github.com/wangziqi2013/BwTree/blob/master/test/iterator_test.cpp
 */
// NOLINTNEXTLINE
TEST_F(BwTreeTests, ForwardIterator) {
  auto *const tree = BwTreeTestUtil::GetEmptyTree();
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
  auto *const tree = BwTreeTestUtil::GetEmptyTree();
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

/**
 * Adapted from https://github.com/wangziqi2013/BwTree/blob/master/test/random_pattern_test.cpp
 */
// NOLINTNEXTLINE
TEST_F(BwTreeTests, ConcurrentRandomInsert) {
  // This defines the key space (0 ~ (1M - 1))
  const uint32_t key_num = 1024 * 1024;
  std::atomic<size_t> insert_success_counter = 0;

  common::WorkerPool thread_pool(num_threads_, {});
  auto *const tree = BwTreeTestUtil::GetEmptyTree();

  // Inserts in a 1M key space randomly until all keys has been inserted
  auto workload = [&](uint32_t id) {
    const uint32_t gcid = id + 1;
    tree->AssignGCID(gcid);
    std::default_random_engine thread_generator(id);
    std::uniform_int_distribution<int> uniform_dist(0, key_num - 1);

    while (insert_success_counter.load() < key_num) {
      int key = uniform_dist(thread_generator);

      if (tree->Insert(key, key)) insert_success_counter.fetch_add(1);
    }
    tree->UnregisterThread(gcid);
  };

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
  tree->UpdateThreadLocal(1);

  // Verifies whether random insert is correct
  for (uint32_t i = 0; i < key_num; i++) {
    auto s = tree->GetValue(i);

    EXPECT_EQ(s.size(), 1);
    EXPECT_EQ(*s.begin(), i);
  }

  delete tree;
}

/**
 * Adapted from https://github.com/wangziqi2013/BwTree/blob/master/test/mixed_test.cpp
 *
 * Multithreaded insert-delete contention test
 *
 * This test focuses on corner cases where insert and delete happens
 * concurrently on the same key
 */
// NOLINTNEXTLINE
TEST_F(BwTreeTests, ConcurrentMixed) {
  TERRIER_ASSERT(num_threads_ % 2 == 0,
                 "This test requires an even number of threads. This should have been handled when it was assigned.");

  // This defines the key space (0 ~ (1M - 1))
  const uint32_t key_num = 1024 * 1024;

  common::WorkerPool thread_pool(num_threads_, {});
  auto *const tree = BwTreeTestUtil::GetEmptyTree();

  auto workload = [&](uint32_t id) {
    const uint32_t gcid = id + 1;
    tree->AssignGCID(gcid);
    if ((id % 2) == 0) {
      for (uint32_t i = 0; i < key_num; i++) {
        int key = num_threads_ * i + id;

        tree->Insert(key, key);
      }
    } else {
      for (uint32_t i = 0; i < key_num; i++) {
        int key = num_threads_ * i + id - 1;

        while (!tree->Delete(key, key)) {
        }
      }
    }
    tree->UnregisterThread(gcid);
  };

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
  tree->UpdateThreadLocal(1);

  // Verifies that all values are deleted after mixed test
  for (uint32_t i = 0; i < key_num * num_threads_; i++) {
    EXPECT_EQ(tree->GetValue(i).size(), 0);
  }

  delete tree;
}

/**
 * Adapted from https://github.com/wangziqi2013/BwTree/blob/master/test/basic_test.cpp and
 * https://github.com/wangziqi2013/BwTree/blob/master/test/main.cpp
 *
 * Test Basic Insert/Delete/GetValue with different patterns and multi thread
 */
// NOLINTNEXTLINE
TEST_F(BwTreeTests, Interleaved) {
  const uint32_t basic_test_key_num = 128 * 1024;

  common::WorkerPool thread_pool(num_threads_, {});
  auto *const tree = BwTreeTestUtil::GetEmptyTree();

  /*
   * InsertTest1() - Each threads inserts in its own consecutive key subspace
   *
   * The intervals of each thread does not intersect, therefore contention
   * is very small and this test is supposed to be very fast
   *
   * |---- thread 0 ----|---- thread 1----|----thread 2----| .... |---- thread n----|
   */
  auto InsertTest1 = [&](uint32_t id) {
    const uint32_t gcid = id + 1;
    tree->AssignGCID(gcid);
    for (uint32_t i = id * basic_test_key_num; i < static_cast<uint32_t>(id + 1) * basic_test_key_num; i++) {
      tree->Insert(i, i + 1);
      tree->Insert(i, i + 2);
      tree->Insert(i, i + 3);
      tree->Insert(i, i + 4);
    }
    tree->UnregisterThread(gcid);
  };

  /*
   * DeleteTest1() - Same pattern as InsertTest1()
   */
  auto DeleteTest1 = [&](uint32_t id) {
    const uint32_t gcid = id + 1;
    tree->AssignGCID(gcid);
    for (uint32_t i = id * basic_test_key_num; i < static_cast<uint32_t>(id + 1) * basic_test_key_num; i++) {
      tree->Delete(i, i + 1);
      tree->Delete(i, i + 2);
      tree->Delete(i, i + 3);
      tree->Delete(i, i + 4);
    }
    tree->UnregisterThread(gcid);
  };

  /*
   * InsertTest2() - All threads collectively insert on the key space
   *
   * | t0 t1 t2 t3 .. tn | t0 t1 t2 t3 .. tn | t0 t1 .. | .. |  ... tn |
   *
   * This test is supposed to be slower since the contention is very high
   * between different threads
   */
  auto InsertTest2 = [&](uint32_t id) {
    const uint32_t gcid = id + 1;
    tree->AssignGCID(gcid);
    for (uint32_t i = 0; i < basic_test_key_num; i++) {
      uint32_t key = num_threads_ * i + id;

      tree->Insert(key, key + 1);
      tree->Insert(key, key + 2);
      tree->Insert(key, key + 3);
      tree->Insert(key, key + 4);
    }
    tree->UnregisterThread(gcid);
  };

  /*
   * DeleteTest2() - The same pattern as InsertTest2()
   */
  auto DeleteTest2 = [&](uint32_t id) {
    const uint32_t gcid = id + 1;
    tree->AssignGCID(gcid);
    for (uint32_t i = 0; i < basic_test_key_num; i++) {
      uint32_t key = num_threads_ * i + id;

      tree->Delete(key, key + 1);
      tree->Delete(key, key + 2);
      tree->Delete(key, key + 3);
      tree->Delete(key, key + 4);
    }
    tree->UnregisterThread(gcid);
  };

  /*
   * DeleteGetValueTest() - Verifies all values have been deleted
   *
   * This function verifies on key_num * thread_num key space
   */
  auto DeleteGetValueTest = [&]() {
    for (uint32_t i = 0; i < basic_test_key_num * num_threads_; i++) {
      auto value_set = tree->GetValue(i);

      EXPECT_EQ(value_set.size(), 0);
    }
  };

  /*
   * InsertGetValueTest() - Verifies all values have been inserted
   */
  auto InsertGetValueTest = [&]() {
    for (uint32_t i = 0; i < basic_test_key_num * num_threads_; i++) {
      auto value_set = tree->GetValue(i);

      EXPECT_EQ(value_set.size(), 4);
    }
  };

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, InsertTest2);
  tree->UpdateThreadLocal(1);

  InsertGetValueTest();

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, DeleteTest1);
  tree->UpdateThreadLocal(1);

  DeleteGetValueTest();

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, InsertTest1);
  tree->UpdateThreadLocal(1);

  InsertGetValueTest();

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, DeleteTest2);
  tree->UpdateThreadLocal(1);

  DeleteGetValueTest();

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, InsertTest1);
  tree->UpdateThreadLocal(1);

  InsertGetValueTest();

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, DeleteTest1);
  tree->UpdateThreadLocal(1);

  DeleteGetValueTest();

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, InsertTest2);
  tree->UpdateThreadLocal(1);

  InsertGetValueTest();

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, DeleteTest2);
  tree->UpdateThreadLocal(1);

  DeleteGetValueTest();

  delete tree;
}

/**
 * Adapted from https://github.com/wangziqi2013/BwTree/blob/master/test/misc_test.cpp
 *
 * This function enters epoch and takes a random delay and exits epoch repeat until desired count has been reached
 */
// NOLINTNEXTLINE
TEST_F(BwTreeTests, EpochManager) {
  common::WorkerPool thread_pool(num_threads_, {});
  auto *const tree = BwTreeTestUtil::GetEmptyTree();

  auto workload = [&](uint32_t id) {
    const uint32_t gcid = id + 1;
    tree->AssignGCID(gcid);
    const uint32_t iterations = 100;
    for (uint32_t i = 0; i < iterations; i++) {
      auto node = tree->epoch_manager.JoinEpoch();

      std::default_random_engine thread_generator(id);
      std::uniform_int_distribution<int> uniform_dist(0, 100);
      std::this_thread::sleep_for(std::chrono::milliseconds{uniform_dist(thread_generator)});

      tree->epoch_manager.LeaveEpoch(node);
    }
    tree->UnregisterThread(gcid);
  };

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
  tree->UpdateThreadLocal(1);

  delete tree;
}

}  // namespace terrier
