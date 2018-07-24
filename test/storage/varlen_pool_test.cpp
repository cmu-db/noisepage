//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// varlen_pool_test.cpp
//
// Identification: test/storage/varlen_pool_test.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <limits.h>
#include <pthread.h>

#include "storage/varlen_pool.h"
#include "gtest/gtest.h"

namespace terrier {
class VarlenPoolTests : public ::testing::Test {};

#define M 1000

// disable unused const variable warning for clang
#ifdef __APPLE__
    #pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-const-variable"
#endif
    const size_t str_len = 1000; // test string length
#ifdef __APPLE__
#pragma clang diagnostic pop
#endif

// Allocate and free once
TEST_F(VarlenPoolTests, AllocateOnceTest) {
  std::unique_ptr<VarlenPool> pool(new VarlenPool());
  void *p = nullptr;
  size_t size;
  size = 40;

  p = pool->Allocate(size);
  EXPECT_TRUE(p != nullptr);

  pool->Free(p);
}

}  // namespace terrier
