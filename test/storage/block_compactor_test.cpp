#include "storage/block_compactor.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
#include "arrow/api.h"

namespace terrier {
// NOLINTNEXTLINE
TEST(BlockCompactorTest, SingleBlockTest) {
  std::default_random_engine generator;
  storage::BlockStore block_store{1, 1};
  storage::RawBlock *block = block_store.Get();
  storage::BlockLayout layout({8, 8, VARLEN_COLUMN});
  storage::TupleAccessStrategy accessor(layout);
  accessor.InitializeRawBlock(block, storage::layout_version_t(0));

  // Technically, the block above is not "in" the table, but since we don't sequential scan that does not matter
  storage::DataTable table(&block_store, layout, storage::layout_version_t(0));
  storage::RecordBufferSegmentPool buffer_pool{10000, 10000};
  transaction::TransactionManager txn_manager(&buffer_pool, false, LOGGING_DISABLED);

  auto tuples = StorageTestUtil::PopulateBlockRandomly(layout, block, 0.1, &generator);

  storage::BlockCompactor compactor;
  compactor.PutInQueue({block, &table});
  compactor.ProcessCompactionQueue(&txn_manager); // should always succeed with no other threads

  storage::ProjectedRowInitializer initializer(layout, StorageTestUtil::ProjectionListAllColumns(layout));
  byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *read_row = initializer.InitializeRow(buffer);
  std::vector<storage::ProjectedRow *> moved_rows;
  // This transaction is guaranteed to start after the compacting one commits
  transaction::TransactionContext *txn = txn_manager.BeginTransaction();
  const uint32_t num_tuples = tuples.size();
  for (uint32_t i = 0; i < layout.NumSlots(); i++) {
    storage::TupleSlot slot(block, i);
    bool visible = table.Select(txn, slot, read_row);
    if (i >= num_tuples) {
      EXPECT_FALSE(visible); // Should be deleted after compaction
    } else {
      EXPECT_TRUE(visible); // Should be filled after compaction
      auto it = tuples.find(slot);
      if (it != tuples.end()) {
        // Here we can assume that the row is not moved. Check that everything is still equal
        if (!StorageTestUtil::ProjectionListEqualShallow(layout, it->second, read_row))
          throw std::runtime_error("");
        delete[] reinterpret_cast<byte *>(tuples[slot]);
        tuples.erase(slot);
      } else {
        // Need to copy and do quadratic comparison later.
        byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
        std::memcpy(buffer, read_row, initializer.ProjectedRowSize());
        moved_rows.push_back(reinterpret_cast<storage::ProjectedRow *>(buffer));
      }
    }
  }
  delete[] buffer;

  for (auto *moved_row : moved_rows) {
    bool match_found = false;
    for (auto &entry : tuples) {
      if (StorageTestUtil::ProjectionListEqualShallow(layout, entry.second, moved_row)) {
        // Here we can assume that the row is not moved. All good.
        delete[] reinterpret_cast<byte *>(entry.second);
        tuples.erase(entry.first);
        match_found = true;
        break;
      }
    }
    // the read tuple should be one of the original tuples that are moved.
    EXPECT_TRUE(match_found);
    delete[] reinterpret_cast<byte *>(moved_row);
  }
  // All tuples from the original block should have been accounted for.
  EXPECT_TRUE(tuples.empty());
}

}  // namespace terrier
