#include <limits.h>
#include <pthread.h>

#include "storage/varlen_pool.h"
#include "common/test_util.h"
#include "gtest/gtest.h"

namespace terrier {
class VarlenPoolTests : public ::testing::Test {};

// Allocate and free once
TEST_F(VarlenPoolTests, AllocateOnceTest) {
  VarlenPool pool;
  void *p = nullptr;
  const uint64_t size = 40;

  p = pool.Allocate(size);
  EXPECT_TRUE(p != nullptr);

  pool.Free(p);
}

// This test generates random workload and sees if the pool gives out
// the same pointer to two threads at the same time.
TEST_F(VarlenPoolTests, ConcurrentCorrectnessTest) {
  // This should have no bearing on the correctness of test
  VarlenPool pool;
  const uint64_t size = 40;
  auto workload = [&](uint32_t) {
  // Randomly generate a sequence of use-free
  std::default_random_engine generator;
  // Store the pointers we use.
  std::vector<void *> ptrs;
  auto allocate = [&] {
    ptrs.push_back(pool.Allocate(size));
  };
  auto free = [&] {
    if (!ptrs.empty()) {
      auto pos = testutil::UniformRandomElement(ptrs, generator);
      pool.Free((*pos));
      ptrs.erase(pos);
    }
  };
  testutil::InvokeWorkloadWithDistribution({free, allocate},
                                           {0.5, 0.5},
                                           generator,
                                           100);
  for (auto ptr : ptrs)
    pool.Free(ptr);
  };

  testutil::RunThreadsUntilFinish(8, workload, 100);
}

}  // namespace terrier
