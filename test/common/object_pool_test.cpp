#include <unordered_set>
#include <atomic>
#include <random>
#include <thread>
#include "gtest/gtest.h"
#include "common/object_pool.h"

namespace terrier {
// TODO(Tianyu): This should eventually extend harness we define.
class ObjectPoolTests : public ::testing::Test {};

TEST_F(ObjectPoolTests, SimpleReuseTest) {
  const uint32_t repeat = 10;
  const uint64_t reuse_limit = 1;
  ObjectPool<uint32_t> tested(reuse_limit);

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
TEST_F(ObjectPoolTests, ConcurrentCorrectnessTest) {
  // This should have no bearing on the correctness of test
  const uint64_t reuse_limit = 100;
  ObjectPool<ObjectPoolTestType> tested(reuse_limit);
  auto workload = [&] {
    const double get_new_ratio = 0.5;

    // Randomly generate a sequence of use-free
    std::default_random_engine generator;
    std::bernoulli_distribution op_distribution(get_new_ratio);
    // Store the pointers we use.
    std::vector<ObjectPoolTestType *> ptrs;

    const uint32_t trial = 100;
    for (uint32_t i = 0; i < trial; i++) {
      bool allocate = op_distribution(generator);
      if (allocate)
        ptrs.push_back(tested.Get()->Use());
      else if (!ptrs.empty()) {
        // Randomly pick a pointer to release
        auto pos = std::uniform_int_distribution(0, (int) ptrs.size() - 1)(generator);
        tested.Release(ptrs[pos]->Release());
        ptrs.erase(ptrs.begin() + pos);
      }
    }
    for (auto *ptr : ptrs)
      tested.Release(ptr->Release());
  };

  const uint32_t repeat = 100;
  for (uint32_t i = 0; i < repeat; i++) {
    std::vector<std::thread> threads;
    const uint32_t num_threads = 8;
    for (uint32_t j = 0; j < num_threads; j++)
      threads.emplace_back(workload);
    for (auto &thread : threads)
      thread.join();
  }
}
}

