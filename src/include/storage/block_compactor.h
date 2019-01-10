#pragma once
#include <forward_list>
#include <unordered_map>
#include <utility>
#include <vector>
#include "storage/data_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage {

/**
 * The block compactor is responsible for taking hot data blocks that are considered to be cold, and make them
 * arrow-compatible. In the process, any gaps resulting from deletes or aborted transactions are also eliminated.
 * If the compaction is successful, the block is considered to be fully cold and will be accessed mostly as read-only
 * data.
 */
class BlockCompactor {
 private:
  static void NoOp(void * /*unused*/) {}
  // This structs has metadata relevant to a single varlen column within a single block that is relevant to
  // the compaction process.
  struct VarlenColumnMetadata {
    // number of varlen bytes we need in total in this varlen column
    uint32_t total_size_;
    // The contiguous buffer we are writing to
    byte *buffer_;
    // the offset within the buffer to copy the next varlen entry into
    uint32_t write_head_;
  };

  // We have to write down a variety of information and pass them around between stages of compaction.
  // This struct writes down metadata relevant for the compaction process witin a single block. Mostly
  // we are interested in compaction within
  struct BlockCompactionTask {
    explicit BlockCompactionTask(const BlockLayout &layout) {
      // Initialize the map of varlen lengths to correspond to the block layout
      for (col_id_t id : layout.Varlens())
        varlen_columns_.emplace(std::piecewise_construct,
                                std::forward_as_tuple(id),
                                std::forward_as_tuple());
    }

    std::vector<TupleSlot> filled_, empty_;
    std::unordered_map<col_id_t, VarlenColumnMetadata> varlen_columns_;
  };

  // A Compaction group is a series of blocks all belonging to the same data table. We compact them together
  // so slots can be freed up. If we only eliminate gaps, deleted slots will never be reclaimed.
  struct CompactionGroup {
    CompactionGroup(transaction::TransactionManager *txn_manager, DataTable *table)
        : txn_(txn_manager->BeginTransaction()), table_(table) {}

    void AddBlock(RawBlock *block) {
      blocks_to_compact_.emplace(std::piecewise_construct,
                                 std::forward_as_tuple(block),
                                 std::forward_as_tuple(table_->accessor_.GetBlockLayout()));
    }

    // A single compaction task is done within a single transaction
    transaction::TransactionContext *txn_;
    DataTable *table_;
    std::unordered_map<RawBlock *, BlockCompactionTask> blocks_to_compact_;
  };

 public:
  /**
   * Processes the compaction queue and mark processed blocks as cold if successful. The compaction can fail due
   * to live versions or contention. There will be a brief window where user transactions writing to the block
   * can be aborted, but no readers would be blocked.
   *
   */
  void ProcessCompactionQueue() {
    std::forward_list<std::pair<RawBlock *, DataTable *>> to_process = std::move(compaction_queue_);

    for (auto &entry : to_process) {
      if (entry.first->insert_head_ != entry.second->accessor_.GetBlockLayout().NumSlots()) continue;
      BlockAccessController &controller = entry.first->controller_;
      // Then the last transactional access must be the compacting transaction, which we can safely ignore.
      if (controller.IsFrozen()) continue;
      // TODO(Tianyu): This is probably fine for now, but we will want to not only compact within a block
      // but also across blocks to eventually free up slots
      CompactionGroup cg(&txn_manager_, entry.second);
      cg.AddBlock(entry.first);
      if (CheckForActiveVersionsAndGaps(&cg)) {
        if (EliminateGaps(&cg)) {
          if (Gather(compacting_txn, entry, varlen_length_sums)) {
            // TODO(Tianyu): I think this is safe if in update, we mark hot immediately before compare and swap.
            // It should be correct, but double check
            controller.MarkFrozen();
            // We should not need to wait for logs to finish as this is purely internal
            txn_manager_.Commit(compacting_txn, NoOp, nullptr);
            continue;
          }
        }
      }
      // One of the steps have failed, we should just abort
      txn_manager_.Abort(compacting_txn);
    }
  }

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
  bool CheckForActiveVersionsAndGaps(CompactionGroup *cg) {
    const TupleAccessStrategy &accessor = cg->table_->accessor_;
    const BlockLayout &layout = accessor.GetBlockLayout();
    for (auto &entry : cg->blocks_to_compact_) {
      RawBlock *block = entry.first;
      BlockCompactionTask &task = entry.second;
      TERRIER_ASSERT(block->insert_head_ == layout.NumSlots(),
                     "The block should be full to stop inserts from coming in");
      // We will loop through each block and figure out if we are safe to proceed with compaction and identify
      // any gaps
      for (uint32_t offset = 0; offset < layout.NumSlots(); offset++) {
        TupleSlot slot(block, offset);
        // TODO(Tianyu): Should we abort right away in the case of a conflict, or simply throw the offending block
        // out of the compaction group and proceed with the rest? Will not matter until we actually start making groups
        // larger than 1.
        // If there is a version pointer in the table, maybe it is not fully cold yet, so hands off
        if (cg->table_->AtomicallyReadVersionPtr(slot, accessor) != nullptr) return false;

        bool allocated = accessor.Allocated(slot);
        // A logically deleted column implies that some changes are happening since the GC put this block into the
        // compaction queue. We should not do anything to this block further.
        if (allocated && accessor.IsNull(slot, VERSION_POINTER_COLUMN_ID)) return false;

        // Push this slots to be either in the list of empty slots of filled slots
        if (!allocated) {
          task.empty_.push_back(slot);
        } else {
          task.filled_.push_back(slot);
          // If the tuple is present, we also need to count the size of all the varlens. This count needs to be adjusted
          // as we shuffle tuples around blocks to fill gaps and will not be final count.
          for (auto &column_entry : task.varlen_columns_) {
            col_id_t col_id = column_entry.first;
            VarlenColumnMetadata &col_metadata = column_entry.second;
            // If there is a race this should result in failure in later steps, and there probably does not matter
            // TODO(Tianyu): It is probably okay to transactionally read this as well if there are edge cases
            auto *varlen_entry = reinterpret_cast<VarlenEntry *>(accessor.AccessWithNullCheck(slot, col_id));
            col_metadata.total_size_ += varlen_entry == nullptr ? 0 : varlen_entry->Size();
          }
        }
      }
    }
  }

  // Given the identified deleted tuples and assuming that no conflict was detected in the previous scan,
  // move around tuples to make all the gaps disappear. The transaction could get aborted and end the compaction
  // process prematurely. Metadata on varlen columns is also updated to help with the following step.
  bool EliminateGaps(CompactionGroup *cg) {
    const TupleAccessStrategy &accessor = cg->table_->accessor_;
    const BlockLayout &layout = accessor.GetBlockLayout();

    // This initializes an update that will copy over all the relevant columns in this block
    // TODO(Tianyu): Fish out the thing from tests and use that
    std::vector<col_id_t> all_cols;
    for (uint16_t i = 1; i < layout.NumColumns(); i++) all_cols.emplace_back(i);
    ProjectedRowInitializer all_cols_initializer(layout, all_cols);

    // TODO(Tianyu): This process can probably be optimized further for the least amount of movements of tuples. But we
    // are probably close enough to optimal that it does not matter that much
    // Within a group, we can calculate the number of blocks exactly we need to store all the filled tuples (we may
    // or may not need to throw away extra blocks when we are done compacting). Then, the algorithm involves selecting
    // the blocks with the least number of empty slots as blocks to "fill into", and the rest as blocks to "take away
    // from". These are not two disjoint sets as we will probably need to shuffle tuples within one block to have
    // perfectly compact groups (but only one block within a group needs this)
    std::vector<RawBlock *> all_blocks;
    uint32_t num_filled = 0;
    for (auto &entry : cg->blocks_to_compact_) {
      all_blocks.push_back(entry.first);
      num_filled += entry.second.filled_.size();
    }
    // Sort all the blocks within a group based on the number of filled slots, indescending order.
    std::sort(all_blocks.begin(), all_blocks.end(),
        [&](std::vector<RawBlock *>::iterator a, std::vector<RawBlock *>::iterator b)  {
      return cg->blocks_to_compact_[*a].filled_.size() > cg->blocks_to_compact_[*b].filled_.size();
    });

    bool has_partially_filled_block = (num_filled % layout.NumSlots() == 0);
    uint32_t num_blocks_required = num_filled / layout.NumSlots() + (has_partially_filled_block ? 1 : 0);

    for (auto taker = all_blocks.begin(), giver = all_blocks.end() - 1; taker <= giver; taker++) {
      BlockCompactionTask &taker_bct = cg->blocks_to_compact_[*taker];
      BlockCompactionTask &giver_bct = cg->blocks_to_compact_[*giver];
      for (TupleSlot empty_slot : taker_bct.empty_) {
        // fill the first empty slot with the last filled slot, essentially
        TupleSlot filled_slot = giver_bct.filled_.back();
        if (taker == giver && filled_slot.GetOffset() < empty_slot.GetOffset())
        RedoRecord *record = cg->txn_->StageWrite(cg->table_, empty_slot, all_cols_initializer);
        cg->table_->Select(cg->txn_, filled_slot, record->Delta());
        cg->table_->InsertInto(cg->txn_, *record->Delta(), empty_slot);
        cg->table_->Delete(cg->txn_, filled_slot);
        giver_bct.filled_.pop_back();
        if (giver_bct.filled_.empty()) giver--;
      }
    }

    for (auto &empty_slot : cg->empty_) {
      // Because we constructed the two lists from sequential scan, slots will always appear in order. We
      // essentially will fill gaps in order, by using the real tuples in reverse order. (Take the last tuple to
      // fill the first empty slot)

      // As an invariant, in the processing of elimnating gaps, any slot that comes logically before the current
      // empty slot must be filled. Thus, if we see an empty slot that is logically the nth tuple slot in the compaction
      // group, where n = number of filled slots, we know we are done.


      // otherwise, insert the last filled slot into the empty slot and delete the filled one

    }
    return true;
  }

  bool Gather(transaction::TransactionContext *txn, const std::pair<RawBlock *, DataTable *> &entry,
              const std::unordered_map<col_id_t, uint32_t> &varlen_length_sums) {
    RawBlock *block = entry.first;
    DataTable *table = entry.second;
    const TupleAccessStrategy &accessor = table->accessor_;
    const BlockLayout &layout = accessor.GetBlockLayout();

    // Initialize all the varlen buffers to use in the block
    std::unordered_map<col_id_t, std::pair<byte *, uint32_t>> varlen_buffers;
    for (auto &entry : varlen_length_sums)
      varlen_buffers.emplace(std::piecewise_construct,
                             std::forward_as_tuple(entry.first),
                             std::forward_as_tuple(common::AllocationUtil::AllocateAligned(entry.second), 0));

    for (uint32_t offset = 0; offset < layout.NumSlots(); offset++) {
      TupleSlot slot(block, offset);
    }
    return true;
  }

  std::forward_list<std::pair<RawBlock *, DataTable *>> compaction_queue_;
  transaction::TransactionManager txn_manager_;
};
}  // namespace terrier::storage
