
#include "gtest/gtest.h"
#include "storage/storage_defs.h"
#include "common/object_pool.h"

namespace terrier {

//===--------------------------------------------------------------------===//
// Block store performance counter Tests
//===--------------------------------------------------------------------===//

TEST(BlockStorePCTests, BlockCountTest) {
  const uint64_t reuse_limit = 1;
  common::PerformanceCounters pc;
  storage::BlockStore tested(reuse_limit, &pc);

  // Create RawBlock
  storage::RawBlock *reused_ptr1 = tested.Get();
  storage::RawBlock *reused_ptr2 = tested.Get();
  pc.PrintPerformanceCounters();

  // Release RawBlock
  tested.Release(reused_ptr1);
  tested.Release(reused_ptr2);

  pc.PrintPerformanceCounters();

  // Create again
  reused_ptr1 = tested.Get();
  pc.PrintPerformanceCounters();

  // Release RawBlock
  tested.Release(reused_ptr1);

  pc.PrintPerformanceCounters();
}

}
