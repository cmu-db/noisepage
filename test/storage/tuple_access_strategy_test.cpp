//
//#include <storage/tuple_access_strategy.h>
//#include "gtest/gtest.h"
//#include "storage/block_store.h"
//#include "storage/tuple_access_strategy.h"
//
//namespace terrier {
//struct TupleAccessStrategyTests : public ::testing::Test {
//  RawBlock *raw_block_;
//  ObjectPool<RawBlock> pool_{1};
// protected:
//  void SetUp() override {
//    ObjectPool<RawBlock> pool_(1);
//
//  }
//  void TearDown() override {
//  }
//};
//
//TEST_F(TupleAccessStrategyTests, BlockLayoutTest) {
//  // System Initialization time
//  ObjectPool<RawBlock> pool_(1);
//  storage::BlockStore block_store(pool_);
//
//  // TODO(Tianyu): Maybe a more complicated layout?
//  storage::BlockLayout layout(2, {4, 8});
//  // TODO(Tianyu): This test will break completely if we change the layout
//  EXPECT_EQ(12, layout.tuple_size_);
//  EXPECT_EQ(24, layout.header_size_);
//
//
//}
//
//TEST_F(TupleAccessStrategyTests, SimpleCorrectnessTest) {
//  // System Initialization time
//  ObjectPool<RawBlock> pool_(1);
//  storage::BlockStore block_store(pool_);
//
//  // Table Creation Time
//  storage::BlockLayout layout(2, {4, 8});
//  storage::TupleAccessStrategy tested(layout);
//
//  // Table Operation Runtime
//  auto block_pair = block_store.NewBlock();
//  block_id_t id = block_pair.first;
//  RawBlock *block = block_pair.second;
//  storage::InitializeRawBlock(block, layout, id);
//
//  // Test that everything is free at this point
//  for (uint32_t i; i < layout.num_slots_; i++) {
//
//  }
//}
//}