#include <unordered_set>
#include <atomic>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"
#include "util/multi_threaded_test_util.h"
#include "common/object_pool.h"

namespace terrier {
// Rather minimalistic checks for whether we reuse memory
// NOLINTNEXTLINE
TEST(ObjectPoolTests, SimpleReuseTest) {
  const uint32_t repeat = 10;
  const uint64_t reuse_limit = 1;
  common::ObjectPool<uint32_t> tested(reuse_limit);

  // Put a pointer on the the reuse queue
  uint32_t *reused_ptr = tested.Get();
  // TODO(WAN): clang-tidy thinks gtest-printers will DefaultPrintTo the released pointer
  // NOLINTNEXTLINE
  tested.Release(reused_ptr);

  // TODO(WAN): clang-tidy thinks gtest-printers will DefaultPrintTo the released pointer here too
  // NOLINTNEXTLINE
  for (uint32_t i = 0; i < repeat; i++) {
    EXPECT_EQ(tested.Get(), reused_ptr);
    tested.Release(reused_ptr);
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
  MultiThreadedTestUtil mtt_util;
  // This should have no bearing on the correctness of test
  const uint64_t reuse_limit = 100;
  common::ObjectPool<ObjectPoolTestType> tested(reuse_limit);
  auto workload = [&](uint32_t tid) {
    // Randomly generate a sequence of use-free
    std::default_random_engine generator;
    // Store the pointers we use.
    std::vector<ObjectPoolTestType *> ptrs;
    auto allocate = [&] {
      ptrs.push_back(tested.Get()->Use(tid));
    };
    auto free = [&] {
      if (!ptrs.empty()) {
        auto pos = MultiThreadedTestUtil::UniformRandomElement(&ptrs, &generator);
        tested.Release((*pos)->Release(tid));
        ptrs.erase(pos);
      }
    };
    MultiThreadedTestUtil::InvokeWorkloadWithDistribution({free, allocate},
                                             {0.5, 0.5},
                                             &generator,
                                             100);
    for (auto *ptr : ptrs)
      tested.Release(ptr->Release(tid));
  };

  mtt_util.RunThreadsUntilFinish(8, workload, 100);
}
}  // namespace terrier
