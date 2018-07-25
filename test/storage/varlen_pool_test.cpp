#include <limits.h>
#include <pthread.h>

#include "storage/varlen_pool.h"
#include "gtest/gtest.h"

namespace terrier {
class VarlenPoolTests : public ::testing::Test {};

// Allocate and free once
TEST_F(VarlenPoolTests, AllocateOnceTest) {
  auto pool = new VarlenPool();
  void *p = nullptr;
  const uint64_t size = 40;

  p = pool->Allocate(size);
  EXPECT_TRUE(p != nullptr);

  pool->Free(p);
}

}  // namespace terrier
