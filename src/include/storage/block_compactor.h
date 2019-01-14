#pragma once
#include<algorithm>
#include <forward_list>
#include <unordered_map>
#include <utility>
#include <vector>
#include "storage/arrow_block_metadata.h"
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

  // We have to write down a variety of information and pass them around between stages of compaction.
  // This struct holds some metadata relevant for the compaction process as well as the metadata to be stored
  // in the block itself if compaction to arrow itself is successful.
  struct BlockCompactionTask {
    explicit BlockCompactionTask(const BlockLayout &layout) {
      for (col_id_t col : layout.Varlens()) total_varlen_sizes_[col] = 0;
      new_block_metadata_ = reinterpret_cast<ArrowBlockMetadata *>(
          common::AllocationUtil::AllocateAligned(ArrowBlockMetadata::Size(layout.NumColumns())));
      new_block_metadata_->Initialize(layout.NumColumns());
    }

    std::vector<TupleSlot> filled_, empty_;
    std::unordered_map<col_id_t, uint32_t> total_varlen_sizes_;
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
      blocks_to_compact_.emplace(std::piecewise_construct, std::forward_as_tuple(block),
                                 std::forward_as_tuple(table_->accessor_.GetBlockLayout()));
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
  void ProcessCompactionQueue(transaction::TransactionManager *txn_manager) {
    std::forward_list<std::pair<RawBlock *, DataTable *>> to_process = std::move(compaction_queue_);

    for (auto &entry : to_process) {
      // Block can still be inserted into. Hands off.
      if (entry.first->insert_head_ != entry.second->accessor_.GetBlockLayout().NumSlots()) continue;
      BlockAccessController &controller = entry.first->controller_;
      // Then the last transactional access must be the compacting transaction, which we can safely ignore.
      if (controller.IsFrozen()) continue;
      // TODO(Tianyu): This is probably fine for now, but we will want to not only compact within a block
      // but also across blocks to eventually free up slots
      CompactionGroup cg(txn_manager->BeginTransaction(), entry.second);
      cg.AddBlock(entry.first);
      if (CheckForActiveVersionsAndGaps(&cg)) {
        if (EliminateGaps(&cg)) {
          if (GatherVarlens(&cg)) {
            Cleanup(&cg, true);
            controller.MarkFrozen();
            // We should not need to wait for logs to finish as this is purely internal
            txn_manager->Commit(cg.txn_, NoOp, nullptr);
            continue;
          }
        }
      }
      // Otherwise, we just abort the transaction and all changes will be undone.
      Cleanup(&cg, false);
      txn_manager->Abort(cg.txn_);
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
          task.new_block_metadata_->NumRecords()++;
          task.filled_.push_back(slot);
          // If the tuple is present, we also need to count the size of all the varlens and nulls. This count needs to
          // be adjusted as we shuffle tuples around blocks to fill gaps and will not be final count.
          if (!layout.Varlens().empty()) {
            bool valid UNUSED_ATTRIBUTE = cg->table_->Select(cg->txn_, slot, cg->read_buffer_);
            TERRIER_ASSERT(valid, "this read should not return an invisible tuple");
            for (uint16_t i = 0; i < cg->read_buffer_->NumColumns(); i++) {
              col_id_t col_id = cg->read_buffer_->ColumnIds()[i];
              byte *entry = cg->read_buffer_->AccessWithNullCheck(i);
              if (entry == nullptr) task.new_block_metadata_->NullCount(col_id)++;
              if (layout.IsVarlen(col_id)) {
                auto *varlen = reinterpret_cast<VarlenEntry *>(cg->read_buffer_->AccessWithNullCheck(i));
                task.total_varlen_sizes_[col_id] += (varlen == nullptr ? 0 : varlen->Size());
              }
            }
          }
        }
      }
    }
    return true;
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
    std::sort(all_blocks.begin(), all_blocks.end(), [&](RawBlock *a, RawBlock *b) {
      // We know these finds will not return end() because we constructed the vector from the map
      return cg->blocks_to_compact_.find(a)->second.filled_.size() >
             cg->blocks_to_compact_.find(b)->second.filled_.size();
    });

    // Because we constructed the two lists from sequential scan, slots will always appear in order. We
    // essentially will fill gaps in order, by using the real tuples in reverse order. (Take the last tuple to
    // fill the first empty slot)
    for (auto taker = all_blocks.begin(), giver = all_blocks.end() - 1; taker <= giver; taker++) {
      // Again, we know these finds will not return end() because we constructed the vector from the map
      BlockCompactionTask &taker_bct = cg->blocks_to_compact_.find(*taker)->second;
      BlockCompactionTask &giver_bct = cg->blocks_to_compact_.find(*giver)->second;
      for (TupleSlot empty_slot : taker_bct.empty_) {
        // fill the first empty slot with the last filled slot, essentially
        TupleSlot filled_slot = giver_bct.filled_.back();
        // We will only shuffle tuples within a block if it is the last block to compact. Then, we can stop
        // when the next empty slot is logically after the next filled slot (which implies we are processing
        // an empty slot that would be empty in a compact block)
        if (taker == giver && filled_slot.GetOffset() < empty_slot.GetOffset()) break;
        RedoRecord *record = cg->txn_->StageWrite(cg->table_, empty_slot, cg->all_cols_initializer_);
        bool valid UNUSED_ATTRIBUTE = cg->table_->Select(cg->txn_, filled_slot, record->Delta());
        TERRIER_ASSERT(valid, "this read should not return an invisible tuple");
        accessor.Reallocate(empty_slot);
        // This operation cannot fail since a logically deleted slot can only be reclaimed by the compaction thread
        cg->table_->InsertInto(cg->txn_, *record->Delta(), empty_slot);
        // The delete can fail if a concurrent transaction is updating said tuple. We will have to abort if this is
        // the case.
        if (!cg->table_->Delete(cg->txn_, filled_slot)) return false;
        giver_bct.filled_.pop_back();
        // Also update the total size of varlens in the blocks accordingly
        for (col_id_t col_id : layout.Varlens()) {
          // Since we initialized the projection list to be all the columns, we know this relation to hold.
          auto projection_list_index = static_cast<uint16_t>((!col_id) - NUM_RESERVED_COLUMNS);
          auto *varlen = reinterpret_cast<VarlenEntry *>(record->Delta()->AccessWithNullCheck(projection_list_index));
          if (varlen == nullptr) {
            taker_bct.new_block_metadata_->NullCount(col_id)++;
            giver_bct.new_block_metadata_->NullCount(col_id)--;
          }
          taker_bct.total_varlen_sizes_[col_id] += varlen->Size();
          giver_bct.total_varlen_sizes_[col_id] -= varlen->Size();
        }
        taker_bct.new_block_metadata_->NumRecords()++;
        giver_bct.new_block_metadata_->NumRecords()--;
        if (giver_bct.filled_.empty()) giver--;
      }
    }

    // TODO(Tianyu): This compaction process could leave blocks empty within a group and we will need to figure out
    // how those blocks are garbage collected. These blocks should have the same life-cycle as the compacting
    // transaction itself. (i.e. when the txn context is being GCed, we should be able to free these blocks as well)
    // For now we are not implementing this because each compaction group is one block.

    return true;
  }

  // After all the tuples are logically contiguous within a group, we can scan through the blocks individually
  // and update all the varlens to point to an offset within our new, arrow-compatible varlen buffer. This process
  // also serves as some kind of lock for the blocks as we are updating every single tuple within the block. This
  // verifies that there are no other versions within the block and we are safe to mark the block cold after the
  // compacting transaction commits. For this reason, we will need to install dummy updates (locks) even if a block
  // has no varlen column.
  // TODO(Tianyu): If we ever write bulk-updates, this will benefit from that greatly
  bool GatherVarlens(CompactionGroup *cg) {
    const TupleAccessStrategy &accessor = cg->table_->accessor_;
    const BlockLayout &layout = accessor.GetBlockLayout();
    if (layout.Varlens().empty()) {
      // Although there are no varlens, We still need to install an empty update as lock. If we cannot, there is a
      // conflict and we should abort.
      for (auto &entry : cg->blocks_to_compact_) {
        RawBlock *block = entry.first;
        BlockCompactionTask &bct = entry.second;
        for (uint32_t offset = 0; offset < bct.new_block_metadata_->NumRecords(); offset++)
          if (!cg->table_->Lock(cg->txn_, {block, offset})) return false;
      }
    } else {
      // Varlens need to be copied.
      ProjectedRowInitializer varlens_initializer(layout, layout.Varlens());
      varlens_initializer.InitializeRow(cg->read_buffer_);  // Okay to reuse this buffer: it is guaranteed to be larger
      for (auto &entry : cg->blocks_to_compact_) {
        RawBlock *block = entry.first;
        BlockCompactionTask &bct = entry.second;
        // allocate varlen buffers for update
        for (col_id_t col_id : layout.Varlens())
          bct.new_block_metadata_->GetVarlenColumn(layout, col_id)
              .Allocate(bct.new_block_metadata_->NumRecords(), bct.total_varlen_sizes_[col_id]);
        // This will accumulate the prefix sum of varlen sizes that we need as the offsets array for arrow
        std::unordered_map<col_id_t, uint32_t> acc;
        for (col_id_t varlen_col_id : layout.Varlens()) acc[varlen_col_id] = 0;

        // Scan through all the tuples and copy them if there are varlens.
        for (uint32_t offset = 0; offset < bct.new_block_metadata_->NumRecords(); offset++) {
          TupleSlot slot(block, offset);
          // we will need to copy varlen into the allocated buffer. If we cannot, there is a conflict and we should
          // abort.
          bool valid UNUSED_ATTRIBUTE = cg->table_->Select(cg->txn_, slot, cg->read_buffer_);
          TERRIER_ASSERT(valid, "this read should not return an invisible tuple");
          RedoRecord *update = cg->txn_->StageWrite(cg->table_, slot, varlens_initializer);
          for (uint16_t i = 0; i < cg->read_buffer_->NumColumns(); i++) {
            col_id_t col_id = cg->read_buffer_->ColumnIds()[i];
            ArrowVarlenColumn &varlen_col = bct.new_block_metadata_->GetVarlenColumn(layout, col_id);
            auto offset_in_values_buffer = varlen_col.offsets_[offset] = acc[col_id];
            auto *varlen = reinterpret_cast<VarlenEntry *>(cg->read_buffer_->AccessWithNullCheck(i));
            if (varlen == nullptr) {
              update->Delta()->SetNull(i);
            } else {
              // If the old varlen is not gathered, it will be GCed by the normal transaction code path. Otherwise,
              // we will replace and delete the old varlen buffer and we should be okay
              memcpy(varlen_col.values_ + offset_in_values_buffer, varlen->Content(), varlen->Size());
              *reinterpret_cast<VarlenEntry *>(update->Delta()->AccessForceNotNull(i)) = {
                  varlen_col.values_ + offset_in_values_buffer, varlen->Size(), true};
              acc[col_id] += varlen->Size();
            }
            if (!cg->table_->Update(cg->txn_, slot, *update->Delta())) return false;
          }
        }
        // Need to write one last offsets to denote the end of the last varlen
        for (uint16_t i = 0; i < cg->read_buffer_->NumColumns(); i++) {
          col_id_t col_id = cg->read_buffer_->ColumnIds()[i];
          TERRIER_ASSERT(bct.total_varlen_sizes_[col_id] == acc[col_id],
                         "Prefix sum result should match previous calculation");
          bct.new_block_metadata_->GetVarlenColumn(layout, col_id).offsets_[bct.new_block_metadata_->NumRecords()] =
              acc[col_id];
        }
      }
    }
    return true;
  }

  // When the compaction process is done (i.e. all transactional operations are done, which guaratees successful
  // commit under SI), we need to cleanup some metadata and free up some memory
  void Cleanup(CompactionGroup *cg, bool successful) {
    const TupleAccessStrategy &accessor = cg->table_->accessor_;
    const BlockLayout &layout = accessor.GetBlockLayout();
    // There is no cleanup to do if there is no varlen in the compaction group
    if (layout.Varlens().empty()) return;

    for (auto &entry : cg->blocks_to_compact_) {
      RawBlock *block = entry.first;
      BlockCompactionTask &bct = entry.second;
      for (col_id_t varlen_col_id : layout.Varlens()) {
        // Depending on whether the compaction was successful, we either need to swap in the new metadata and
        // deallocate the old, or throw away the new one if compaction failed.
        ArrowVarlenColumn &col = (successful ? accessor.GetArrowBlockMetadata(block) : *bct.new_block_metadata_)
                                     .GetVarlenColumn(layout, varlen_col_id);
        cg->txn_->loose_ptrs_.push_back(reinterpret_cast<byte *>(col.offsets_));
        cg->txn_->loose_ptrs_.push_back(col.values_);
      }
      if (successful)
        memcpy(&accessor.GetArrowBlockMetadata(block), bct.new_block_metadata_,
               ArrowBlockMetadata::Size(layout.NumColumns()));
    }
  }

  std::forward_list<std::pair<RawBlock *, DataTable *>> compaction_queue_;
};
}  // namespace terrier::storage
