#pragma once
#include <unordered_map>
#include <utility>
#include <vector>
#include "storage/arrow_block_metadata.h"
#include "storage/data_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage {

/**
 * Typedef for a standard hash map with varlen entry as the key. The map uses deep equality checks (whether
 * the stored underlying byte string is the same) for key comparison.
 */
template <class T>
using VarlenEntryMap = std::unordered_map<VarlenEntry, T, VarlenContentHasher, VarlenContentDeepEqual>;

/**
 * The block compactor is responsible for taking hot data blocks that are considered to be cold, and make them
 * arrow-compatible. In the process, any gaps resulting from deletes or aborted transactions are also eliminated.
 * If the compaction is successful, the block is considered to be fully cold and will be accessed mostly as read-only
 * data.
 */
class BlockCompactor {
 private:
  // We have to write down a variety of information and pass them around between stages of compaction.
  // This struct holds some metadata relevant for the compaction process as well as the metadata to be stored
  // in the block itself if compaction to arrow itself is successful.
  struct BlockCompactionTask {
    explicit BlockCompactionTask(const BlockLayout &layout, const ArrowBlockMetadata &arrow_metadata) {
      for (col_id_t col : layout.Varlens()) total_varlen_sizes_[col] = 0;
      new_block_metadata_ = reinterpret_cast<ArrowBlockMetadata *>(
          common::AllocationUtil::AllocateAligned(ArrowBlockMetadata::Size(layout.NumColumns())));
      new_block_metadata_->Initialize(layout.NumColumns());
      for (const auto &col_id : layout.AllColumns()) {
        new_block_metadata_->GetColumnInfo(layout, col_id).type_ = arrow_metadata.GetColumnInfo(layout, col_id).type_;
      }
    }

    std::vector<TupleSlot> filled_, empty_;
    // This map is used to keep track of the length of the varlen buffer we need to allocate. Its meaning changes
    // based on whether we are merely gathering or compressing. When compressing, we only count distinct varlen lengths.
    std::unordered_map<col_id_t, uint32_t> total_varlen_sizes_;
    // This is unused unless we are dictionary encoding
    std::unordered_map<col_id_t, VarlenEntryMap<uint32_t>> dictionary_corpus_;
    ArrowBlockMetadata *new_block_metadata_;
  };

  // A Compaction group is a series of blocks all belonging to the same data table. We compact them together
  // so slots can be freed up. If we only eliminate gaps, deleted slots will never be reclaimed.
  struct CompactionGroup {
    CompactionGroup(transaction::TransactionContext *txn, DataTable *table)
        : txn_(txn),
          table_(table),
          all_cols_initializer_(table_->accessor_.GetBlockLayout(), table_->accessor_.GetBlockLayout().AllColumns()),
          read_buffer_(all_cols_initializer_.InitializeRow(
              common::AllocationUtil::AllocateAligned(all_cols_initializer_.ProjectedRowSize()))) {}

    ~CompactionGroup() {
      // Deleting nullptr is just a noop
      delete[] reinterpret_cast<byte *>(read_buffer_);
    }

    void AddBlock(RawBlock *block) {
      blocks_to_compact_.emplace(
          std::piecewise_construct, std::forward_as_tuple(block),
          std::forward_as_tuple(table_->accessor_.GetBlockLayout(), table_->accessor_.GetArrowBlockMetadata(block)));
    }

    // A single compaction task is done within a single transaction
    transaction::TransactionContext *txn_;
    DataTable *table_;
    std::unordered_map<RawBlock *, BlockCompactionTask> blocks_to_compact_;
    ProjectedRowInitializer all_cols_initializer_;
    ProjectedRow *read_buffer_;
  };

 public:
  /**
   * Processes the compaction queue and mark processed blocks as cold if successful. The compaction can fail due
   * to live versions or contention. There will be a brief window where user transactions writing to the block
   * can be aborted, but no readers would be blocked.
   *
   */
  void ProcessCompactionQueue(transaction::TransactionManager *txn_manager);

  // TODO(Tianyu): Should a block know about its own data table? We seem to need this back pointer awfully often.
  /**
   * Adds a block associated with a data table to the compaction to be processed in the future.
   * @param entry the block (and its parent data table) that needs to be processed by the compactor
   */
  void PutInQueue(const std::pair<RawBlock *, DataTable *> &entry) { compaction_queue_.push_front(entry); }

 private:
  // This will identify all the present and deleted tuples in a first pass as well as check for active versions.
  // If there are active versions then the function returns false to signal that the compaction should not proceed.
  // The compaction group passed in should only contain the blocks and data table we are interested in compacting,
  // not any of the calculated metadata.
  bool CheckForActiveVersionsAndGaps(CompactionGroup *cg);

  // When a tuple is present, read the tuple and update associated metadata we need for later stages of the
  // transformation
  void InspectTuple(CompactionGroup *cg, BlockCompactionTask *bct, TupleSlot slot);

  // Given the identified deleted tuples and assuming that no conflict was detected in the previous scan,
  // move around tuples to make all the gaps disappear. The transaction could get aborted and end the compaction
  // process prematurely. Metadata on varlen columns is also updated to help with the following step.
  bool EliminateGaps(CompactionGroup *cg);

  // Move a tuple and updated associated information in their respective blocks
  bool MoveTuple(CompactionGroup *cg, BlockCompactionTask *giver, BlockCompactionTask *taker, TupleSlot from,
                 TupleSlot to);

  // After all the tuples are logically contiguous within a group, we can scan through the blocks individually
  // and update all the varlens to point to an offset within our new, arrow-compatible varlen buffer. This process
  // also serves as some kind of lock for the blocks as we are updating every single tuple within the block. This
  // verifies that there are no other versions within the block and we are safe to mark the block cold after the
  // compacting transaction commits. For this reason, we will need to install dummy updates (locks) even if a block
  // has no varlen column.
  // TODO(Tianyu): If we ever write bulk-updates, this will benefit from that greatly
  bool GatherVarlens(CompactionGroup *cg);

  // Initialize each block's buffer for storing varlens as well as metadata associated with the gathering phase
  void InitializeGatherWorkspace(BlockCompactionTask *bct, const BlockLayout &layout,
                                 std::unordered_map<col_id_t, uint32_t> *acc,
                                 std::unordered_map<col_id_t, VarlenEntryMap<uint32_t>> *index_map);

  // Update varlen columns in the block with updated locations in Arrow buffers
  bool UpdateVarlensForBlock(RawBlock *block, const BlockLayout &layout, BlockCompactionTask *bct, CompactionGroup *cg,
                             std::unordered_map<col_id_t, uint32_t> *acc,
                             std::unordered_map<col_id_t, VarlenEntryMap<uint32_t>> *index_map);

  // When the compaction process is done (i.e. all transactional operations are done, which guaratees successful
  // commit under SI), we need to cleanup some metadata and free up some memory
  void Cleanup(CompactionGroup *cg, bool successful);

  std::forward_list<std::pair<RawBlock *, DataTable *>> compaction_queue_;
};
}  // namespace terrier::storage
