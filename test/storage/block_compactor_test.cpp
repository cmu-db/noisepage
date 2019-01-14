#include "storage/block_compactor.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
namespace terrier {
// NOLINTNEXTLINE
TEST(BlockCompactorTest, SimpleTest) {
  storage::BlockStore block_store{1, 1};
  storage::RawBlock *block = block_store.Get();
  storage::BlockLayout layout({8, 8, VARLEN_COLUMN});
  storage::TupleAccessStrategy accessor(layout);
  accessor.InitializeRawBlock(block, storage::layout_version_t(0));

  storage::DataTable table(&block_store, layout, storage::layout_version_t(0));
  storage::RecordBufferSegmentPool buffer_pool{10000, 10000};
  transaction::TransactionManager txn_manager(&buffer_pool, false, LOGGING_DISABLED);

  storage::ProjectedRowInitializer initializer(layout, StorageTestUtil::ProjectionListAllColumns(layout));
  byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *row = initializer.InitializeRow(buffer);

  for (uint32_t i = 0; i < 20; i++) {
    storage::TupleSlot slot;
    accessor.Allocate(block, &slot);
    if (slot.GetOffset() % 5 == 0) {
      *reinterpret_cast<byte **>(accessor.AccessForceNotNull(slot, storage::col_id_t(0))) = nullptr;
      auto *foo = new char[4];
      *reinterpret_cast<storage::VarlenEntry *>(accessor.AccessForceNotNull(slot, storage::col_id_t(1))) = {
          reinterpret_cast<byte *>(foo), 4, false};
      *reinterpret_cast<uint64_t *>(accessor.AccessForceNotNull(slot, storage::col_id_t(2))) = slot.GetOffset() / 5;
    } else {
      accessor.Deallocate(slot);
    }
  }
  block->insert_head_ = layout.NumSlots();

  storage::BlockCompactor compactor;
  compactor.PutInQueue({block, &table});
  compactor.ProcessCompactionQueue(&txn_manager);

  transaction::TransactionContext *txn = txn_manager.BeginTransaction();

  for (uint32_t i = 0; i < 4; i++) {
    table.Select(txn, {block, i}, row);
    StorageTestUtil::PrintRow(*row, layout);
  }
  for (uint32_t i = 4; i < 20; i++) {
    EXPECT_FALSE(table.Select(txn, {block, i}, row));
  }
}
}  // namespace terrier
