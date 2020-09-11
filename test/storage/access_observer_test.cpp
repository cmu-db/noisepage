#include "storage/access_observer.h"

#include <random>
#include <thread>  // NOLINT

#include "storage/block_compactor.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
namespace terrier {
class MockBlockCompactor : public storage::BlockCompactor {
 public:
  // NOLINTNEXTLINE
  MOCK_METHOD1(PutInQueue, void(storage::RawBlock *));
};

// Tests that the observer only enqueues blocks that are full and has not been accessed for a while
// NOLINTNEXTLINE
TEST(AccessObserverTest, EmptyBlocksNotObserved) {
  // Obtain a fake block
  std::default_random_engine generator;
  // Varlens within the layout has no impact on the observer
  storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(100, &generator);
  storage::TupleAccessStrategy accessor(layout);
  storage::DataTable table(nullptr, layout, storage::layout_version_t(0));
  auto *fake_block = new storage::RawBlock;
  accessor.InitializeRawBlock(&table, fake_block, storage::layout_version_t(0));

  MockBlockCompactor mock_compactor;
  EXPECT_CALL(mock_compactor, PutInQueue(::testing::_)).Times(0);
  storage::AccessObserver tested(&mock_compactor);

  // Test that empty blocks are never observed
  tested.ObserveWrite(fake_block);
  for (uint32_t i = 0; i < COLD_DATA_EPOCH_THRESHOLD; i++) tested.ObserveGCInvocation();
  // Should not be called
  delete fake_block;
}

// NOLINTNEXTLINE
TEST(AccessObserverTest, FilledBlocksObserved) {
  // Obtain a fake block
  std::default_random_engine generator;
  // Varlens within the layout has no impact on the observer
  storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(100, &generator);
  storage::TupleAccessStrategy accessor(layout);
  storage::DataTable table(nullptr, layout, storage::layout_version_t(0));
  auto *fake_block = new storage::RawBlock;
  accessor.InitializeRawBlock(&table, fake_block, storage::layout_version_t(0));

  MockBlockCompactor mock_compactor;
  // NOLINTNEXTLINE
  EXPECT_CALL(mock_compactor, PutInQueue(::testing::_)).Times(1);
  storage::AccessObserver tested(&mock_compactor);

  // Manually set block to be filled
  fake_block->insert_head_ = layout.NumSlots();
  // Now it should be called
  tested.ObserveWrite(fake_block);
  for (uint32_t i = 0; i <= COLD_DATA_EPOCH_THRESHOLD; i++) tested.ObserveGCInvocation();
  delete fake_block;
}
}  // namespace terrier

int main(int argc, char **argv) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}
