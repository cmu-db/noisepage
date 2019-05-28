#include "storage/block_compactor.h"
#include <vector>
#include "arrow/api.h"
#include "storage/block_access_controller.h"
#include "storage/garbage_collector.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"

namespace terrier {
struct BlockCompactorTest : public ::terrier::TerrierTest {
  storage::BlockStore block_store_{5000, 5000};
  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool buffer_pool_{100000, 100000};
  storage::BlockLayout layout_{{8, 8, VARLEN_COLUMN}};
  storage::TupleAccessStrategy accessor_{layout_};

  storage::DataTable table_{&block_store_, layout_, storage::layout_version_t(0)};
  transaction::TransactionManager txn_manager_{&buffer_pool_, true, LOGGING_DISABLED};
  storage::GarbageCollector gc_{&txn_manager_};
  storage::BlockCompactor compactor_;

  std::default_random_engine generator;

  uint32_t num_blocks_ = 500;
  double percent_empty_ = 0.01;
};

TEST_F(BlockCompactorTest, CompactionTest) {}

// NOLINTNEXTLINE
TEST_F(BlockCompactorTest, SingleBlockDictionaryTest) {
  std::default_random_engine generator;
  storage::BlockStore block_store{1, 1};
  storage::BlockLayout layout({8, 8, VARLEN_COLUMN});
  // Technically, the block above is not "in" the table, but since we don't sequential scan that does not matter
  storage::DataTable table(&block_store, layout, storage::layout_version_t(0));

  storage::RawBlock *block = block_store.Get();
  storage::TupleAccessStrategy accessor(layout);
  accessor.InitializeRawBlock(block, storage::layout_version_t(0));

  storage::RecordBufferSegmentPool buffer_pool{10000, 10000};
  // Enable GC to cleanup transactions started by the block compactor
  transaction::TransactionManager txn_manager(&buffer_pool, true, LOGGING_DISABLED);
  storage::GarbageCollector gc(&txn_manager);

  auto tuples = StorageTestUtil::PopulateBlockRandomly(layout, block, 0.1, &generator);
  auto &arrow_metadata = accessor.GetArrowBlockMetadata(block);
  for (storage::col_id_t col_id : layout.AllColumns()) {
    if (layout.IsVarlen(col_id)) {
      arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::GATHERED_VARLEN;
    } else {
      arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
    }
  }

  storage::BlockCompactor compactor;
  compactor.PutInQueue(block);
  compactor.ProcessCompactionQueue(&txn_manager);  // should always succeed with no other threads

  auto initializer =
      storage::ProjectedRowInitializer::Create(layout, StorageTestUtil::ProjectionListAllColumns(layout));
  byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *read_row = initializer.InitializeRow(buffer);
  std::vector<storage::ProjectedRow *> moved_rows;
  // This transaction is guaranteed to start after the compacting one commits
  transaction::TransactionContext *txn = txn_manager.BeginTransaction();
  auto num_tuples = tuples.size();
  for (uint32_t i = 0; i < layout.NumSlots(); i++) {
    storage::TupleSlot slot(block, i);
    bool visible = table.Select(txn, slot, read_row);
    if (i >= num_tuples) {
      EXPECT_FALSE(visible);  // Should be deleted after compaction
    } else {
      EXPECT_TRUE(visible);  // Should be filled after compaction
      auto it = tuples.find(slot);
      if (it != tuples.end()) {
        // Here we can assume that the row is not moved. Check that everything is still equal. Has to be deep
        // equality because varlens are moved.
        EXPECT_TRUE(StorageTestUtil::ProjectionListEqualDeep(layout, it->second, read_row));
        delete[] reinterpret_cast<byte *>(tuples[slot]);
        tuples.erase(slot);
      } else {
        // Need to copy and do quadratic comparison later.
        byte *local_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
        std::memcpy(local_buffer, read_row, initializer.ProjectedRowSize());
        moved_rows.push_back(reinterpret_cast<storage::ProjectedRow *>(local_buffer));
      }
    }
  }
  txn_manager.Commit(txn, [](void *) -> void {}, nullptr);  // Commit: will be cleaned up by GC
  delete[] buffer;

  for (auto *moved_row : moved_rows) {
    bool match_found = false;
    for (auto &entry : tuples) {
      // This comparison needs to be deep because varlens are moved.
      if (StorageTestUtil::ProjectionListEqualDeep(layout, entry.second, moved_row)) {
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
  gc.PerformGarbageCollection();
  gc.PerformGarbageCollection();  // Second call to deallocate.
  // Deallocated arrow buffers
  for (const auto &col_id : layout.AllColumns()) {
    arrow_metadata.GetColumnInfo(layout, col_id).Deallocate();
  }
  delete block;
}

}  // namespace terrier
