#include <unordered_set>
#include <atomic>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"
#include "util/multi_threaded_test_util.h"
#include "common/object_pool.h"

namespace terrier {
// Rather minimalistic checks for whether we reuse memory
TEST(ObjectPoolTests, SimpleReuseTest) {
  const uint32_t repeat = 10;
  const uint64_t reuse_limit = 1;
  common::ObjectPool<uint32_t> tested(reuse_limit);

  // Put a pointer on the the reuse queue
  uint32_t *reused_ptr = tested.Get();
  tested.Release(reused_ptr);

  for (uint32_t i = 0; i < repeat; i++) {
    EXPECT_EQ(tested.Get(), reused_ptr);
    tested.Release(reused_ptr);
  }
}

class ObjectPoolTestType {
 public:
  ObjectPoolTestType *Use() {
    // We would expect that the memory we get back is not in-use;
    bool expected = false;
    // If this comes back false we are being given a reference that others hold,
    // which indicates a bug.
    EXPECT_TRUE(in_use_.compare_exchange_strong(expected, true));
    return this;
  }

  ObjectPoolTestType *Release() {
    in_use_.store(false);
    return this;
  }
 private:
  std::atomic<bool> in_use_;
};

// This test generates random workload and sees if the pool gives out
// the same pointer to two threads at the same time.
TEST(ObjectPoolTests, ConcurrentCorrectnessTest) {
  // This should have no bearing on the correctness of test
  const uint64_t reuse_limit = 100;
  common::ObjectPool<ObjectPoolTestType> tested(reuse_limit);
  auto workload = [&](uint32_t) {
    // Randomly generate a sequence of use-free
    std::default_random_engine generator;
    // Store the pointers we use.
    std::vector<ObjectPoolTestType *> ptrs;
    auto allocate = [&] {
      ptrs.push_back(tested.Get()->Use());
    };
    auto free = [&] {
      if (!ptrs.empty()) {
        auto pos = MultiThreadedTestUtil::UniformRandomElement(ptrs, generator);
        tested.Release((*pos)->Release());
        ptrs.erase(pos);
      }
    };
    MultiThreadedTestUtil::InvokeWorkloadWithDistribution({free, allocate},
                                             {0.5, 0.5},
                                             generator,
                                             100);
    for (auto *ptr : ptrs)
      tested.Release(ptr->Release());
  };

  MultiThreadedTestUtil::RunThreadsUntilFinish(8, workload, 100);
}
}  // namespace terrier
