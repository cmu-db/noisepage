
#include <memory>
#include "gtest/gtest.h"
#include "storage/storage_defs.h"
#include "common/object_pool.h"

namespace terrier {

/**
 * Stats Tests
 */

// Stats test for object pool stats
// NOLINTNEXTLINE
TEST(StatsTests, ObjectPoolStatsTest) {
  const uint64_t reuse_limit = 10;
  common::StatsCollector stats_collector;
  std::unique_ptr<storage::BlockStore> tested(new storage::BlockStore(reuse_limit, &stats_collector));

  // Create RawBlock
  storage::RawBlock *block_ptr1 = tested->Get();
  storage::RawBlock *block_ptr2 = tested->Get();
  EXPECT_EQ(0, stats_collector.GetCounter("create block"));
  EXPECT_EQ(0, stats_collector.GetCounter("reuse block"));
  stats_collector.CollectCounters();
  stats_collector.PrintStats();
  EXPECT_EQ(2, stats_collector.GetCounter("create block"));
  EXPECT_EQ(0, stats_collector.GetCounter("reuse block"));

  // Release RawBlock
  // NOLINTNEXTLINE
  tested->Release(block_ptr1);
  // NOLINTNEXTLINE
  tested->Release(block_ptr2);

  // Create again
  storage::RawBlock *block_ptr3 = tested->Get();
  storage::RawBlock *block_ptr4 = tested->Get();
  storage::RawBlock *block_ptr5 = tested->Get();
  EXPECT_EQ(2, stats_collector.GetCounter("create block"));
  EXPECT_EQ(0, stats_collector.GetCounter("reuse block"));
  stats_collector.CollectCounters();
  stats_collector.PrintStats();
  EXPECT_EQ(3, stats_collector.GetCounter("create block"));
  EXPECT_EQ(2, stats_collector.GetCounter("reuse block"));

  // Release RawBlock
  // NOLINTNEXTLINE
  tested->Release(block_ptr3);
  // NOLINTNEXTLINE
  tested->Release(block_ptr4);
  // NOLINTNEXTLINE
  tested->Release(block_ptr5);

  // Destructor test
  storage::RawBlock *block_ptr6 = tested->Get();
  // NOLINTNEXTLINE
  tested->Release(block_ptr6);
  tested.reset();
  stats_collector.PrintStats();
  EXPECT_EQ(3, stats_collector.GetCounter("create block"));
  EXPECT_EQ(3, stats_collector.GetCounter("reuse block"));
}

}  // namespace terrier
