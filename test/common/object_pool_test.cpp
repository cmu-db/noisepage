#include <atomic>
#include <thread>  // NOLINT
#include <unordered_set>
#include <vector>

#include "common/object_pool.h"
#include "gtest/gtest.h"
#include "util/random_test_util.h"
#include "util/test_thread_pool.h"

namespace terrier {

// Rather minimalistic checks for whether we reuse memory
// NOLINTNEXTLINE
TEST(ObjectPoolTests, SimpleReuseTest) {
  const uint32_t repeat = 10;
  const uint64_t size_limit = 1;
  const uint64_t reuse_limit = 1;
  common::ObjectPool<uint32_t> tested(size_limit, reuse_limit);

  // Put a pointer on the the reuse queue
  // clang-tidy thinks gtest-printers will DefaultPrintTo the released pointer
  // NOLINTNEXTLINE
  uint32_t *reused_ptr = tested.Get();
  // clang-tidy thinks gtest-printers will DefaultPrintTo the released pointer
  // NOLINTNEXTLINE
  tested.Release(reused_ptr);

  // clang-tidy thinks gtest-printers will DefaultPrintTo the released pointer here too
  // NOLINTNEXTLINE
  for (uint32_t i = 0; i < repeat; i++) {
    EXPECT_EQ(tested.Get(), reused_ptr);
    tested.Release(reused_ptr);
  }
}

// Allocate more memory space than the object pool and expect exceptions
// NOLINTNEXTLINE
TEST(ObjectPoolTests, ExceedLimitTest) {
  const uint32_t repeat = 1;
  const uint64_t size_limit = 10;
  const uint64_t reuse_limit = size_limit;
  for (uint32_t iter = 0; iter < repeat; iter++) {
    common::ObjectPool<uint32_t> tested(size_limit, reuse_limit);

    // Get 11 objects
    std::vector<uint32_t *> objects;
    for (uint32_t i = 1; i <= size_limit + 1; i++) {
      uint32_t *cur_ptr = nullptr;
      try {
        cur_ptr = tested.Get();
        if (i == size_limit + 1) {
          // free memory before we fail
          tested.Release(cur_ptr);
          for (auto &ptr : objects) tested.Release(ptr);
          FAIL() << "Expect std::length_error when attempting to get object from pool with size limit " << size_limit
                 << " and " << i - 1 << " objects already allocated.";
          ;
        }
        objects.push_back(cur_ptr);
      } catch (const std::length_error &e) {
        // got length error
        if (i <= 10) {
          // free memory before we fail
          for (auto &ptr : objects) tested.Release(ptr);
          FAIL() << "Unexpected std::length_error; object pool has not allocated to its size limit yet.";
        }
      } catch (...) {
        // free memory before we fail
        for (auto &ptr : objects) tested.Release(ptr);
        FAIL() << "Unexpected exceptions";
      }
    }
    // Free Memory
    for (auto &ptr : objects) tested.Release(ptr);
  }
}

// Reset the size of the object pool
// NOLINTNEXTLINE
TEST(ObjectPoolTests, ResetLimitTest) {
  const uint32_t repeat = 10;
  const uint64_t size_limit = 10;
  for (uint32_t iteration = 0; iteration < repeat; ++iteration) {
    common::ObjectPool<uint32_t> tested(size_limit, size_limit);
    std::unordered_set<uint32_t *> used_ptrs;

    // The reuse_queue should have a size of size_limit
    for (uint32_t i = 0; i < size_limit; ++i) used_ptrs.insert(tested.Get());
    for (auto &it : used_ptrs) tested.Release(it);

    tested.SetReuseLimit(size_limit / 2);
    EXPECT_TRUE(tested.SetSizeLimit(size_limit / 2));

    std::vector<uint32_t *> ptrs;
    for (uint32_t i = 0; i < size_limit / 2; ++i) {
      // the first half should be reused pointers
      uint32_t *ptr = tested.Get();
      EXPECT_FALSE(used_ptrs.find(ptr) == used_ptrs.end());

      // store the pointer to free later
      ptrs.emplace_back(ptr);
    }

    // I should get an exception
    EXPECT_THROW(tested.Get(), std::length_error);

    // free memory
    for (auto &it : ptrs) tested.Release(it);
  }
}

class ObjectPoolTestType {
 public:
  ObjectPoolTestType *Use(uint32_t thread_id) {
    user_ = thread_id;
    return this;
  }

  ObjectPoolTestType *Release(uint32_t thread_id) {
    // Nobody used this
    EXPECT_EQ(thread_id, user_);
    return this;
  }

 private:
  std::atomic<uint32_t> user_;
};

// This test generates random workload and sees if the pool gives out
// the same pointer to two threads at the same time.
// NOLINTNEXTLINE
TEST(ObjectPoolTests, ConcurrentCorrectnessTest) {
  TestThreadPool thread_pool;

  const uint64_t size_limit = 100;
  const uint64_t reuse_limit = 100;
  common::ObjectPool<ObjectPoolTestType> tested(size_limit, reuse_limit);
  auto workload = [&](uint32_t tid) {
    std::uniform_int_distribution<uint64_t> size_dist_(1, reuse_limit);

    // Randomly generate a sequence of use-free
    std::default_random_engine generator;
    // Store the pointers we use.
    std::vector<ObjectPoolTestType *> ptrs;
    auto allocate = [&] {
      try {
        ptrs.push_back(tested.Get()->Use(tid));
      } catch (std::length_error) {
        // Since threads are alloc and free in random order, object pool could possibly have no object to hand out.
        // When this occurs, we just do nothing. The purpose of this test is to test object pool concurrently and
        // check correctness. We just skip and do nothing. The object pool will eventually have objects when other
        // threads release objects.
      }
    };
    auto free = [&] {
      if (!ptrs.empty()) {
        auto pos = RandomTestUtil::UniformRandomElement(&ptrs, &generator);
        tested.Release((*pos)->Release(tid));
        ptrs.erase(pos);
      }
    };
    auto set_reuse_limit = [&] { tested.SetReuseLimit(size_dist_(generator)); };

    auto set_size_limit = [&] { tested.SetSizeLimit(size_dist_(generator)); };

    RandomTestUtil::InvokeWorkloadWithDistribution({free, allocate, set_reuse_limit, set_size_limit},
                                                   {0.25, 0.25, 0.25, 0.25}, &generator, 1000);
    for (auto *ptr : ptrs) tested.Release(ptr->Release(tid));
  };
  thread_pool.RunThreadsUntilFinish(TestThreadPool::HardwareConcurrency(), workload, 100);
}
}  // namespace terrier
