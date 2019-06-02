#include "storage/block_compactor.h"
#include <vector>
#include "arrow/api.h"
#include "storage/block_access_controller.h"
#include "storage/garbage_collector.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
#include "common/hash_util.h"

namespace terrier {

class ProjectedRowDeepEqual {
 public:
  explicit ProjectedRowDeepEqual(const storage::BlockLayout &layout) : layout_(layout) {}

  bool operator()(storage::ProjectedRow *tuple1, storage::ProjectedRow *tuple2) const {
    return StorageTestUtil::ProjectionListEqualDeep(layout_, tuple1, tuple2);
  }

 private:
  const storage::BlockLayout &layout_;
};

class ProjectedRowDeepEqualHash {
 public:
  explicit ProjectedRowDeepEqualHash(const storage::BlockLayout &layout) : layout_(layout) {}

  size_t operator()(storage::ProjectedRow *tuple) const {
    size_t result = 0;
    for (uint16_t i = 0; i < tuple->NumColumns(); i++) {
      byte *field = tuple->AccessWithNullCheck(i);
      storage::col_id_t id = tuple->ColumnIds()[i];
      if (field == nullptr) continue;
      size_t field_hash = layout_.IsVarlen(id)
                          ? storage::VarlenContentHasher()(*reinterpret_cast<storage::VarlenEntry *>(field))
                          : common::HashUtil::HashBytes(field, layout_.AttrSize(id));
      result = common::HashUtil::CombineHashes(result, field_hash);
    }
    return result;
  }

 private:
  const storage::BlockLayout &layout_;
};

using TupleMultiSet = std::unordered_map<storage::ProjectedRow *,
                                         uint32_t,
                                         ProjectedRowDeepEqualHash,
                                         ProjectedRowDeepEqual>;

struct BlockCompactorTest : public ::terrier::TerrierTest {
  storage::BlockStore block_store_{5000, 5000};
  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool buffer_pool_{100000, 100000};
  uint32_t num_blocks_ = 500;
  double percent_empty_ = 0.01;

  TupleMultiSet GetTupleSet(const storage::BlockLayout &layout,
                            std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> tuples) {
    // The fact that I will need to fill in a bucket size is retarded...
    TupleMultiSet result(10, ProjectedRowDeepEqualHash(layout), ProjectedRowDeepEqual(layout));
    for (auto &entry : tuples) result[entry.second]++;
    return result;
  }
};

// This tests generates random single blocks and compacts them. It then verifies that the tuples are reshuffled to be
// compact and its contents unmodified.
// NOLINTNEXTLINE
TEST_F(BlockCompactorTest, SingleBlockCompactionTest) {
  storage::BlockLayout layout({8, 8, VARLEN_COLUMN});
  storage::TupleAccessStrategy accessor(layout);
  // Technically, the block above is not "in" the table, but since we don't sequential scan that does not matter
  storage::DataTable table(&block_store_, layout, storage::layout_version_t(0));

  uint32_t repeat = 100;
  for (uint32_t iteration = 0; iteration < repeat; iteration++) {
    storage::RawBlock *block = block_store_.Get();
    accessor.InitializeRawBlock(&table, block, storage::layout_version_t(0));

    // Enable GC to cleanup transactions started by the block compactor
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    storage::GarbageCollector gc(&txn_manager);

    auto tuples = StorageTestUtil::PopulateBlockRandomly(&table, block, percent_empty_, &generator_);
    auto num_tuples = tuples.size();
    auto tuple_set = GetTupleSet(layout, tuples);
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

    // Read out the rows one-by-one. Check that the tuples are laid out contiguously. If a tuple is not
    // equal to its original value, we store it for later checks.
    auto initializer =
        storage::ProjectedRowInitializer::Create(layout, StorageTestUtil::ProjectionListAllColumns(layout));
    byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *read_row = initializer.InitializeRow(buffer);
    // This transaction is guaranteed to start after the compacting one commits
    transaction::TransactionContext *txn = txn_manager.BeginTransaction();

    for (uint32_t i = 0; i < layout.NumSlots(); i++) {
      storage::TupleSlot slot(block, i);
      bool visible = table.Select(txn, slot, read_row);
      if (i >= num_tuples) {
        EXPECT_FALSE(visible);  // Should be deleted after compaction
      } else {
        EXPECT_TRUE(visible);  // Should be filled after compaction
        auto entry = tuple_set.find(read_row);
        EXPECT_NE(entry, tuple_set.end()); // Should be present in the original
        if (entry != tuple_set.end()) {
          EXPECT_GT(entry->second, 0);
          entry->second--;
        }
      }
    }
    txn_manager.Commit(txn, [](void *) -> void {}, nullptr);  // Commit: will be cleaned up by GC
    delete[] buffer;

    for (auto &entry : tuple_set) {
      EXPECT_EQ(entry.second, 0); // All tuples from the original block should have been accounted for.
    }

    for (auto &entry : tuples)
      delete[] reinterpret_cast<byte *>(entry.second); // reclaim memory used for bookkeeping

    gc.PerformGarbageCollection();
    gc.PerformGarbageCollection();  // Second call to deallocate.
    block_store_.Release(block);
  }
}

}  // namespace terrier
