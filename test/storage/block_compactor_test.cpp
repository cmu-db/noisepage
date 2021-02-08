#include "storage/block_compactor.h"

#include <unordered_map>
#include <vector>

#include "common/hash_util.h"
#include "storage/block_access_controller.h"
#include "storage/garbage_collector.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/deferred_action_manager.h"

namespace noisepage {

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

// Nothing says two randomly generated rows cannot be equal to each other. We thus have to account for this
// using a frequency count instead of just a set.
using TupleMultiSet =
    std::unordered_map<storage::ProjectedRow *, uint32_t, ProjectedRowDeepEqualHash, ProjectedRowDeepEqual>;

struct BlockCompactorTest : public ::noisepage::TerrierTest {
  storage::BlockStore block_store_{5000, 5000};
  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool buffer_pool_{100000, 100000};
  uint32_t num_blocks_ = 500;
  double percent_empty_ = 0.01;

  TupleMultiSet GetTupleSet(const storage::BlockLayout &layout,
                            const std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> &tuples) {
    // The fact that I will need to fill in a bucket size is retarded...
    TupleMultiSet result(10, ProjectedRowDeepEqualHash(layout), ProjectedRowDeepEqual(layout));
    for (auto &entry : tuples) result[entry.second]++;
    return result;
  }
};

// This tests generates random single blocks and compacts them. It then verifies that the tuples are reshuffled to be
// compact and its contents unmodified.
// NOLINTNEXTLINE
TEST_F(BlockCompactorTest, CompactionTest) {
  // TODO(Tianyu): This test currently still only tests one block at a time. We do this because the implementation
  // only compacts block-at-a-time, although the logic handles more blocks. When we change that to have a more
  // intelligent policy, we need to rewrite this test as well.
  uint32_t repeat = 10;
  for (uint32_t iteration = 0; iteration < repeat; iteration++) {
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutWithVarlens(100, &generator_);
    storage::TupleAccessStrategy accessor(layout);
    // Technically, the block above is not "in" the table, but since we don't sequential scan that does not matter
    storage::DataTable table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout,
                             storage::layout_version_t(0));
    storage::RawBlock *block = block_store_.Get();
    accessor.InitializeRawBlock(&table, block, storage::layout_version_t(0));

    // Enable GC to cleanup transactions started by the block compactor
    transaction::TimestampManager timestamp_manager;
    transaction::DeferredActionManager deferred_action_manager{common::ManagedPointer(&timestamp_manager)};
    transaction::TransactionManager txn_manager{common::ManagedPointer(&timestamp_manager),
                                                common::ManagedPointer(&deferred_action_manager),
                                                common::ManagedPointer(&buffer_pool_),
                                                true,
                                                false,
                                                DISABLED};
    storage::GarbageCollector gc{common::ManagedPointer(&timestamp_manager),
                                 common::ManagedPointer(&deferred_action_manager), common::ManagedPointer(&txn_manager),
                                 DISABLED};

    auto tuples = StorageTestUtil::PopulateBlockRandomly(&table, block, percent_empty_, &generator_);
    auto num_tuples = tuples.size();
    auto tuple_set = GetTupleSet(layout, tuples);
    // Manually populate the block header's arrow metadata for test initialization
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
    compactor.ProcessCompactionQueue(&deferred_action_manager,
                                     &txn_manager);  // should always succeed with no other threads

    // Read out the rows one-by-one. Check that the tuples are laid out contiguously, and that the
    // logical contents of the table did not change
    auto initializer =
        storage::ProjectedRowInitializer::Create(layout, StorageTestUtil::ProjectionListAllColumns(layout));
    byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *read_row = initializer.InitializeRow(buffer);
    // This transaction is guaranteed to start after the compacting one commits
    transaction::TransactionContext *txn = txn_manager.BeginTransaction();

    for (uint32_t i = 0; i < layout.NumSlots(); i++) {
      storage::TupleSlot slot(block, i);
      bool visible = table.Select(common::ManagedPointer(txn), slot, read_row);
      if (i >= num_tuples) {
        EXPECT_FALSE(visible);  // Should be deleted after compaction
      } else {
        EXPECT_TRUE(visible);  // Should be filled after compaction
        auto entry = tuple_set.find(read_row);
        EXPECT_NE(entry, tuple_set.end());  // Should be present in the original
        if (entry != tuple_set.end()) {
          EXPECT_GT(entry->second, 0);
          entry->second--;
        }
      }
    }
    txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);  // Commit: will be cleaned up by GC
    delete[] buffer;

    for (auto &entry : tuple_set) {
      EXPECT_EQ(entry.second, 0);  // All tuples from the original block should have been accounted for.
    }

    for (auto &entry : tuples) delete[] reinterpret_cast<byte *>(entry.second);  // reclaim memory used for bookkeeping

    gc.PerformGarbageCollection();
    gc.PerformGarbageCollection();  // Second call to deallocate.
    // Deallocate all the leftover versions
    storage::StorageUtil::DeallocateVarlens(block, accessor);
    block_store_.Release(block);
  }
}

// This tests generates random single blocks and compacts them. It then verifies that the logical content of the table
// does not change and that the varlens are contiguous in Arrow storage. We only test single blocks because gathering
// happens block at a time.
// NOLINTNEXTLINE
TEST_F(BlockCompactorTest, GatherTest) {
  uint32_t repeat = 10;
  for (uint32_t iteration = 0; iteration < repeat; iteration++) {
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutWithVarlens(100, &generator_);
    storage::TupleAccessStrategy accessor(layout);
    // Technically, the block above is not "in" the table, but since we don't sequential scan that does not matter
    storage::DataTable table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout,
                             storage::layout_version_t(0));
    storage::RawBlock *block = block_store_.Get();
    accessor.InitializeRawBlock(&table, block, storage::layout_version_t(0));

    // Enable GC to cleanup transactions started by the block compactor
    transaction::TimestampManager timestamp_manager;
    transaction::DeferredActionManager deferred_action_manager{common::ManagedPointer(&timestamp_manager)};
    transaction::TransactionManager txn_manager{common::ManagedPointer(&timestamp_manager),
                                                common::ManagedPointer(&deferred_action_manager),
                                                common::ManagedPointer(&buffer_pool_),
                                                true,
                                                false,
                                                DISABLED};
    storage::GarbageCollector gc{common::ManagedPointer(&timestamp_manager),
                                 common::ManagedPointer(&deferred_action_manager), common::ManagedPointer(&txn_manager),
                                 DISABLED};

    auto tuples = StorageTestUtil::PopulateBlockRandomly(&table, block, percent_empty_, &generator_);
    auto num_tuples = tuples.size();
    auto tuple_set = GetTupleSet(layout, tuples);

    // Manually populate the block header's arrow metadata for test initialization
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
    compactor.ProcessCompactionQueue(&deferred_action_manager, &txn_manager);  // compaction pass

    // Need to prune the version chain in order to make sure that the second pass succeeds
    gc.PerformGarbageCollection();
    compactor.PutInQueue(block);
    compactor.ProcessCompactionQueue(&deferred_action_manager, &txn_manager);  // gathering pass

    // Read out the rows one-by-one. Check that the varlens are laid out contiguously, and that the
    // logical contents of the table did not change
    auto initializer =
        storage::ProjectedRowInitializer::Create(layout, StorageTestUtil::ProjectionListAllColumns(layout));
    byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *read_row = initializer.InitializeRow(buffer);
    // This transaction is guaranteed to start after the compacting one commits
    transaction::TransactionContext *txn = txn_manager.BeginTransaction();
    for (uint32_t i = 0; i < num_tuples; i++) {
      storage::TupleSlot slot(block, i);
      bool visible = table.Select(common::ManagedPointer(txn), slot, read_row);
      EXPECT_TRUE(visible);  // Should be filled after compaction
      auto entry = tuple_set.find(read_row);
      EXPECT_NE(entry, tuple_set.end());  // Should be present in the original
      if (entry != tuple_set.end()) {
        EXPECT_GT(entry->second, 0);
        entry->second--;
      }

      // Now, check that all the varlen values point to the correct arrow storage locations
      for (uint16_t offset = 0; offset < read_row->NumColumns(); offset++) {
        storage::col_id_t id = read_row->ColumnIds()[offset];
        if (!layout.IsVarlen(id)) continue;
        auto *varlen = reinterpret_cast<storage::VarlenEntry *>(read_row->AccessWithNullCheck(offset));
        storage::ArrowVarlenColumn &arrow_column = arrow_metadata.GetColumnInfo(layout, id).VarlenColumn();
        // No need to check the null case, because the arrow data structure shares the null bitmap with
        // our table, and thus will always be equal
        if (varlen == nullptr) continue;
        auto size UNUSED_ATTRIBUTE = varlen->Size();
        // Safe to do plus 1, because length array will always have one more element
        EXPECT_EQ(arrow_column.Offsets()[i + 1] - arrow_column.Offsets()[i], varlen->Size());
        if (!varlen->IsInlined()) {
          EXPECT_EQ(arrow_column.Values() + arrow_column.Offsets()[i], varlen->Content());
        } else {
          EXPECT_EQ(std::memcmp(varlen->Content(), arrow_column.Values() + arrow_column.Offsets()[i], varlen->Size()),
                    0);
        }
      }
    }

    txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);  // Commit: will be cleaned up by GC
    delete[] buffer;

    for (auto &entry : tuple_set) {
      EXPECT_EQ(entry.second, 0);  // All tuples from the original block should have been accounted for.
    }

    for (auto &entry : tuples) delete[] reinterpret_cast<byte *>(entry.second);  // reclaim memory used for bookkeeping

    gc.PerformGarbageCollection();
    gc.PerformGarbageCollection();  // Second call to deallocate.
    // Deallocate all the leftover gathered varlens
    // No need to gather the ones still in the block because they are presumably all gathered
    for (storage::col_id_t col_id : layout.AllColumns())
      if (layout.IsVarlen(col_id)) arrow_metadata.GetColumnInfo(layout, col_id).Deallocate();
    block_store_.Release(block);
  }
}

// This tests generates random single blocks and dictionary compresses them. It then verifies that the logical contents
// of the table does not change, and the varlens are properly compressed. We only test single blocks because gathering
// happens block at a time.
// NOLINTNEXTLINE
TEST_F(BlockCompactorTest, DictionaryCompressionTest) {
  uint32_t repeat = 10;
  for (uint32_t iteration = 0; iteration < repeat; iteration++) {
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutWithVarlens(100, &generator_);
    storage::TupleAccessStrategy accessor(layout);
    // Technically, the block above is not "in" the table, but since we don't sequential scan that does not matter
    storage::DataTable table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout,
                             storage::layout_version_t(0));
    storage::RawBlock *block = block_store_.Get();
    accessor.InitializeRawBlock(&table, block, storage::layout_version_t(0));

    // Enable GC to cleanup transactions started by the block compactor
    transaction::TimestampManager timestamp_manager;
    transaction::DeferredActionManager deferred_action_manager{common::ManagedPointer(&timestamp_manager)};
    transaction::TransactionManager txn_manager{common::ManagedPointer(&timestamp_manager),
                                                common::ManagedPointer(&deferred_action_manager),
                                                common::ManagedPointer(&buffer_pool_),
                                                true,
                                                false,
                                                DISABLED};
    storage::GarbageCollector gc{common::ManagedPointer(&timestamp_manager),
                                 common::ManagedPointer(&deferred_action_manager), common::ManagedPointer(&txn_manager),
                                 DISABLED};

    auto tuples = StorageTestUtil::PopulateBlockRandomly(&table, block, percent_empty_, &generator_);
    auto num_tuples = tuples.size();
    auto tuple_set = GetTupleSet(layout, tuples);

    // Manually populate the block header's arrow metadata for test initialization
    auto &arrow_metadata = accessor.GetArrowBlockMetadata(block);
    for (storage::col_id_t col_id : layout.AllColumns()) {
      if (layout.IsVarlen(col_id)) {
        arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::DICTIONARY_COMPRESSED;
      } else {
        arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
      }
    }

    storage::BlockCompactor compactor;
    compactor.PutInQueue(block);
    compactor.ProcessCompactionQueue(&deferred_action_manager, &txn_manager);  // compaction pass

    // Need to prune the version chain in order to make sure that the second pass succeeds
    gc.PerformGarbageCollection();
    compactor.PutInQueue(block);
    compactor.ProcessCompactionQueue(&deferred_action_manager, &txn_manager);  // gathering pass

    // Read out the rows one-by-one. Check that the varlens are laid out contiguously, and that the
    // logical contents of the table did not change
    auto initializer =
        storage::ProjectedRowInitializer::Create(layout, StorageTestUtil::ProjectionListAllColumns(layout));
    byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *read_row = initializer.InitializeRow(buffer);

    // Test that all entries are unique and sorted within the dictionary
    for (storage::col_id_t varlen_col : layout.Varlens()) {
      storage::ArrowVarlenColumn &arrow_column = arrow_metadata.GetColumnInfo(layout, varlen_col).VarlenColumn();
      // It suffices to check that every dictionary entry is strictly larger than the previous one for this.
      for (uint32_t code = 1; code < arrow_column.OffsetsLength() - 1; code++) {
        std::string_view prev(reinterpret_cast<char *>(arrow_column.Values() + arrow_column.Offsets()[code - 1]),
                              arrow_column.Offsets()[code] - arrow_column.Offsets()[code - 1]);
        std::string_view curr(reinterpret_cast<char *>(arrow_column.Values() + arrow_column.Offsets()[code]),
                              arrow_column.Offsets()[code + 1] - arrow_column.Offsets()[code]);
        EXPECT_TRUE(prev < curr);
      }
    }

    // This transaction is guaranteed to start after the compacting one commits
    transaction::TransactionContext *txn = txn_manager.BeginTransaction();
    for (uint32_t i = 0; i < num_tuples; i++) {
      storage::TupleSlot slot(block, i);
      bool visible = table.Select(common::ManagedPointer(txn), slot, read_row);
      EXPECT_TRUE(visible);  // Should be filled after compaction
      auto entry = tuple_set.find(read_row);
      EXPECT_NE(entry, tuple_set.end());  // Should be present in the original
      if (entry != tuple_set.end()) {
        EXPECT_GT(entry->second, 0);
        entry->second--;
      }

      // Now, check that all the varlen values point to the correct arrow storage locations
      for (uint16_t offset = 0; offset < read_row->NumColumns(); offset++) {
        storage::col_id_t id = read_row->ColumnIds()[offset];
        if (!layout.IsVarlen(id)) continue;
        auto *varlen = reinterpret_cast<storage::VarlenEntry *>(read_row->AccessWithNullCheck(offset));
        storage::ArrowVarlenColumn &arrow_column = arrow_metadata.GetColumnInfo(layout, id).VarlenColumn();
        // No need to check the null case, because the arrow data structure shares the null bitmap with
        // our table, and thus will always be equal
        if (varlen == nullptr) continue;
        auto size UNUSED_ATTRIBUTE = varlen->Size();
        auto dict_code = arrow_metadata.GetColumnInfo(layout, id).Indices()[i];
        // Safe to do plus 1, because length array will always have one more element
        EXPECT_EQ(arrow_column.Offsets()[dict_code + 1] - arrow_column.Offsets()[dict_code], varlen->Size());
        if (!varlen->IsInlined()) {
          EXPECT_EQ(arrow_column.Values() + arrow_column.Offsets()[dict_code], varlen->Content());
        } else {
          EXPECT_EQ(
              std::memcmp(varlen->Content(), arrow_column.Values() + arrow_column.Offsets()[dict_code], varlen->Size()),
              0);
        }
      }
    }

    txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);  // Commit: will be cleaned up by GC
    delete[] buffer;

    for (auto &entry : tuple_set) {
      EXPECT_EQ(entry.second, 0);  // All tuples from the original block should have been accounted for.
    }

    for (auto &entry : tuples) delete[] reinterpret_cast<byte *>(entry.second);  // reclaim memory used for bookkeeping

    gc.PerformGarbageCollection();
    gc.PerformGarbageCollection();  // Second call to deallocate.
    // Deallocate all the leftover gathered varlens
    // No need to gather the ones still in the block because they are presumably all gathered
    for (storage::col_id_t col_id : layout.AllColumns())
      if (layout.IsVarlen(col_id)) arrow_metadata.GetColumnInfo(layout, col_id).Deallocate();
    block_store_.Release(block);
  }
}

}  // namespace noisepage
