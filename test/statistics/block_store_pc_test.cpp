#include "common/harness.h"
#include "storage/storage_defs.h"

namespace terrier {

//===--------------------------------------------------------------------===//
// Block store performance counter Tests
//===--------------------------------------------------------------------===//

TEST(BlockStorePCTests, BlockCountTest) {
  const uint64_t reuse_limit = 1;
  storage::BlockStore tested(reuse_limit);

  // Create RawBlock
  storage::RawBlock *reused_ptr1 = tested.Get();
  storage::RawBlock *reused_ptr2 = tested.Get();
  tested.PrintPerformanceCounters();

  // Release RawBlock
  tested.Release(reused_ptr1);
  tested.Release(reused_ptr2);

  tested.PrintPerformanceCounters();

  // Create again
  reused_ptr1 = tested.Get();
  tested.PrintPerformanceCounters();

  // Release RawBlock
  tested.Release(reused_ptr1);

  tested.PrintPerformanceCounters();

}

}
