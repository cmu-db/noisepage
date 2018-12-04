#pragma once
#include <vector>
#include <forward_list>
#include <utility>
#include "storage/data_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage {

class BlockCompactor {
 private:
  static void NoOp(void *) {}

 public:
  void ProcessCompactionQueue() {
    std::forward_list<std::pair<RawBlock *, DataTable *>> to_process = std::move(compaction_queue_);
    for (auto &entry : to_process) {
      if (entry.first->insert_head_ != entry.second->accessor_.GetBlockLayout().NumSlots()) continue;
      BlockAccessController &controller = entry.first->controller_;
      // Then the last transactional access must be the compacting transaction, which we can safely ignore.
      if (controller.IsFrozen()) continue;
      // We need to first run compaction, and then mark the block as frozen.
      transaction::TransactionContext *compacting_txn = txn_manager_.BeginTransaction();
      if (Compact(compacting_txn, entry)) {
        if (Gather(compacting_txn, entry)) {
          // TODO(Tianyu): I think this is safe if in update, we mark hot immediately before compare and swap.
          // It should be correct, but double check
          controller.MarkFrozen();
          // No need to wait for logs as this is a purely internal operation
          txn_manager_.Commit(compacting_txn, NoOp, nullptr);
          continue;
        }
      }
      // One of the steps have failed, we should just abort
      txn_manager_.Abort(compacting_txn);
    }
  }

  void PutInQueue(const std::pair<RawBlock *, DataTable *> &entry) { compaction_queue_.push_front(entry); }

 private:
  bool Compact(transaction::TransactionContext *txn, const std::pair<RawBlock *, DataTable *> &entry) {
    RawBlock *block = entry.first;
    DataTable *table = entry.second;
    const TupleAccessStrategy &accessor = table->accessor_;
    const BlockLayout &layout = accessor.GetBlockLayout();
    TERRIER_ASSERT(block->insert_head_ == layout.NumSlots(), "The block should be full to stop inserts from coming in");
    std::vector<TupleSlot> filled, empty;
    // Scan through and identify empty slots
    for (uint32_t offset = 0; offset < layout.NumSlots(); offset++) {
      TupleSlot slot(block, offset);
      // If there is a version pointer in the table, maybe it is not fully cold yet, so hands off
      if (table->AtomicallyReadVersionPtr(slot, accessor) != nullptr) return false;
      bool allocated = accessor.Allocated(slot);
      // A logically deleted column implies that some changes are happening since the GC put this block into the
      // compaction queue. We should do anything to this block further.
      if (allocated && accessor.IsNull(slot, VERSION_POINTER_COLUMN_ID)) return false;
      // Push this slots to be either in the list of empty slots of filled slots
      (allocated ? empty : filled).push_back(slot);
    }

    // TODO(Tianyu): Fish out the thing from tests and use that
    std::vector<col_id_t> all_cols;
    for (uint16_t i = 1; i < layout.NumColumns(); i++) all_cols.emplace_back(i);
    ProjectedRowInitializer all_cols_initializer(layout, all_cols);

    // Scan through all the empty slots, which are always in order, and choose the last element not empty to fill it in.
    for (auto &empty_slot : empty) {
      // At the end of filled portion of the block
      if (empty_slot.GetOffset() >= filled.size()) break;

      // otherwise, insert the last filled slot into the empty slot and delete the filled one
      TupleSlot filled_slot = filled.back();
      RedoRecord *record = txn->StageWrite(table, empty_slot, all_cols_initializer);
      table->Select(txn, filled_slot, record->Delta());
      table->InsertInto(txn, *record->Delta(), empty_slot);
      txn->StageDelete(table, filled_slot);
      table->Delete(txn, filled_slot);
      filled.pop_back();
    }
    return true;
  }

  bool Gather(transaction::TransactionContext *txn, const std::pair<RawBlock *, DataTable *> &entry) {
    return false;
  }

  std::forward_list<std::pair<RawBlock *, DataTable *>> compaction_queue_;
  transaction::TransactionManager txn_manager_;
};
}  // namespace terrier::storage
