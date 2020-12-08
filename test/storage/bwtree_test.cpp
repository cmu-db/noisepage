#include <algorithm>
#include <random>
#include <vector>

#include "bwtree/bloom_filter.h"
#include "bwtree/sorted_small_set.h"
#include "test_util/bwtree_test_util.h"
#include "test_util/multithread_test_util.h"
#include "test_util/test_harness.h"

namespace noisepage {

/**
 * These tests are adapted from https://github.com/wangziqi2013/BwTree/tree/master/test
 * Please do not use these as a model for other tests within this repository.
 *
 * Update 12/8/2020: They have been disabled because they take too long in CI. They've been running on essentially the
 * same data structure for about 2 years, and we know its issues.
 */
struct BwTreeTests : public TerrierTest {
  const uint32_t num_threads_ =
      MultiThreadTestUtil::HardwareConcurrency() + (MultiThreadTestUtil::HardwareConcurrency() % 2);
};

/**
 * mbutrovi: this test was added in order to reproduce an issue we saw while using the BwTree as the index for NewOrder
 * queries in TPC-C. Here is a description of the issue from Ziqi:
 *
 * We observed memory leaks while worker threads are inserting and scanning on the same leaf node. The leaked memory is
 * always reported as being allocated inside the constructor of ForwardIterator, in which we perform a down traversal of
 * the tree with the scan key. It seems that memory chunks allocated during this process is not properly added into the
 * garbage chain or not properly released by the GC.
 */
// NOLINTNEXTLINE
TEST_F(BwTreeTests, DISABLED_ReproduceNewOrderMemoryLeak) {
  NOISEPAGE_ASSERT(num_threads_ % 2 == 0,
                   "This test requires an even number of threads. This should have been handled when it was assigned.");

  // This defines the key space (0 ~ (1M - 1))
  const uint32_t key_num = 1024 * 1024 * num_threads_;
  common::WorkerPool thread_pool(num_threads_, {});
  thread_pool.Startup();
  auto *const tree = new third_party::bwtree::BwTree<int64_t, int64_t>;

  std::vector<int64_t> keys;
  keys.reserve(key_num);

  for (int64_t i = 0; i < key_num; i++) {
    keys.emplace_back(i);
  }
  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

  const uint32_t chunk_size = 1024 * 1024;

  auto workload = [&](uint32_t id) {
    const uint32_t chunk_offset = chunk_size * id;

    if ((id % 2) == 0) {
      // Insert random keys
      for (uint32_t i = 0; i < chunk_size && chunk_offset + i < key_num; i++) {
        const auto key = keys[chunk_offset + i];
        tree->Insert(key, key);
      }
    } else {
      std::vector<int64_t> scan_results;
      // Do ascending limit scans and delete the smallest element found in the range
      for (uint32_t i = 0; i < chunk_size - 1 && chunk_offset + i + 1 < key_num; i++) {
        const auto low_key = std::min(keys[chunk_offset + i], keys[chunk_offset + i + 1]);
        const auto high_key = std::max(keys[chunk_offset + i], keys[chunk_offset + i + 1]);

        const auto scan_limit = 1;
        scan_results.clear();

        for (auto scan_itr = tree->Begin(low_key); scan_results.size() < scan_limit && !scan_itr.IsEnd() &&
                                                   (tree->KeyCmpLessEqual(scan_itr->first, high_key));
             scan_itr++) {
          scan_results.emplace_back(scan_itr->second);
        }

        if (!scan_results.empty()) tree->Delete(scan_results[0], scan_results[0]);
      }
    }
  };

  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);

  delete tree;
}

/**
 * Adapted from https://github.com/wangziqi2013/BwTree/blob/master/stl_test/bloom_filter.cpp
 * Modified to have Inserts live on the heap, because that's required according to Ziqi
 */
// NOLINTNEXTLINE
TEST_F(BwTreeTests, DISABLED_BloomFilter) {
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
TEST_F(BwTreeTests, DISABLED_SortedSmallSet) {
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
TEST_F(BwTreeTests, DISABLED_ForwardIterator) {
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
TEST_F(BwTreeTests, DISABLED_ReverseIterator) {
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
TEST_F(BwTreeTests, DISABLED_ConcurrentRandomInsert) {
  // This defines the key space (0 ~ (1M - 1))
  const uint32_t key_num = 1024 * 1024;
  std::atomic<size_t> insert_success_counter = 0;

  common::WorkerPool thread_pool(num_threads_, {});
  thread_pool.Startup();
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
TEST_F(BwTreeTests, DISABLED_ConcurrentMixed) {
  NOISEPAGE_ASSERT(num_threads_ % 2 == 0,
                   "This test requires an even number of threads. This should have been handled when it was assigned.");

  // This defines the key space (0 ~ (1M - 1))
  const uint32_t key_num = 1024 * 1024;

  common::WorkerPool thread_pool(num_threads_, {});
  thread_pool.Startup();
  auto *const tree = BwTreeTestUtil::GetEmptyTree();

  auto workload = [&](uint32_t id) {
    const uint32_t gcid = id + 1;
    tree->AssignGCID(gcid);
    if ((id % 2) == 0) {
      for (uint32_t i = 0; i < key_num; i++) {
        int key = num_threads_ * i + id;  // NOLINT

        tree->Insert(key, key);
      }
    } else {
      for (uint32_t i = 0; i < key_num; i++) {
        int key = num_threads_ * i + id - 1;  // NOLINT

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
TEST_F(BwTreeTests, DISABLED_Interleaved) {
  const uint32_t basic_test_key_num = 128 * 1024;

  common::WorkerPool thread_pool(num_threads_, {});
  thread_pool.Startup();
  auto *const tree = BwTreeTestUtil::GetEmptyTree();

  /*
   * InsertTest1() - Each threads inserts in its own consecutive key subspace
   *
   * The intervals of each thread does not intersect, therefore contention
   * is very small and this test is supposed to be very fast
   *
   * |---- thread 0 ----|---- thread 1----|----thread 2----| .... |---- thread n----|
   */
  auto insert_test1 = [&](uint32_t id) {
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
  auto delete_test1 = [&](uint32_t id) {
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
  auto insert_test2 = [&](uint32_t id) {
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
  auto delete_test2 = [&](uint32_t id) {
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
  auto delete_get_value_test = [&]() {
    for (uint32_t i = 0; i < basic_test_key_num * num_threads_; i++) {
      auto value_set = tree->GetValue(i);

      EXPECT_EQ(value_set.size(), 0);
    }
  };

  /*
   * InsertGetValueTest() - Verifies all values have been inserted
   */
  auto insert_get_value_test = [&]() {
    for (uint32_t i = 0; i < basic_test_key_num * num_threads_; i++) {
      auto value_set = tree->GetValue(i);

      EXPECT_EQ(value_set.size(), 4);
    }
  };

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, insert_test2);
  tree->UpdateThreadLocal(1);

  insert_get_value_test();

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, delete_test1);
  tree->UpdateThreadLocal(1);

  delete_get_value_test();

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, insert_test1);
  tree->UpdateThreadLocal(1);

  insert_get_value_test();

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, delete_test2);
  tree->UpdateThreadLocal(1);

  delete_get_value_test();

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, insert_test1);
  tree->UpdateThreadLocal(1);

  insert_get_value_test();

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, delete_test1);
  tree->UpdateThreadLocal(1);

  delete_get_value_test();

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, insert_test2);
  tree->UpdateThreadLocal(1);

  insert_get_value_test();

  tree->UpdateThreadLocal(num_threads_ + 1);
  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, delete_test2);
  tree->UpdateThreadLocal(1);

  delete_get_value_test();

  delete tree;
}

/**
 * Adapted from https://github.com/wangziqi2013/BwTree/blob/master/test/misc_test.cpp
 *
 * This function enters epoch and takes a random delay and exits epoch repeat until desired count has been reached
 */
// NOLINTNEXTLINE
TEST_F(BwTreeTests, DISABLED_EpochManager) {
  common::WorkerPool thread_pool(num_threads_, {});
  thread_pool.Startup();
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

}  // namespace noisepage
