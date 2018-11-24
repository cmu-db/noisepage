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
      BlockAccessController &controller = entry.first->controller_;
      // Then the last transactional access must be the compacting transaction, which we can safely ignore.
      if (controller.IsFrozen()) continue;
      // We need to first run compaction, and then mark the block as freezing.
      transaction::TransactionContext *compacting_txn = txn_manager_.BeginTransaction();
      if (Compact(compacting_txn, entry)) {
        if (Gather(compacting_txn, entry)) {
          DeallocateLogicallyDeletedTuples();
          // No need to wait for logs as this is a purely internal operation
          txn_manager_.Commit(compacting_txn, NoOp, nullptr);
          controller.MarkFrozen();
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
    // We need to claim all the slots temporarily to stop any more inserts from going in. They will show
    // up to external transactions as logically deleted anyways so it should be fine, and no other transactions can
    // update it. It is also okay for us to not roll back this as eventually a compaction thread should get it.
    // TODO(Tianyu): Is there a better way of doing this?
    while (!accessor.Allocate(block, nullptr)) {}
    TERRIER_ASSERT(block->num_records_ == layout.NumSlots(), "The block should be full to stop inserts from coming in");
    std::vector<TupleSlot> filled, empty;
    // Scan through and identify empty slots
    for (uint32_t offset = 0; offset < layout.NumSlots(); offset++) {
      TupleSlot slot(block, offset);
      TERRIER_ASSERT(accessor.Allocated(slot), "All slots should already be allocated in the block at this point");
      // If there is a version pointer in the table, maybe it is not fully cold yet, so hands off
      if (table->AtomicallyReadVersionPtr(slot, accessor) != nullptr) return false;
      // TODO(Tianyu): Here we are assuming that GC is NOT flipping logically deleted tuples back to claimable slots
      // We will see if the slot is empty or not by checking the logically deleted bit
      (accessor.IsNull(slot, VERSION_POINTER_COLUMN_ID) ? empty : filled).push_back(slot);
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

  bool Gather(transaction::TransactionContext *txn, const std::pair<RawBlock *, DataTable *> &entry) { return false; }

  void DeallocateLogicallyDeletedTuples() {}

  std::forward_list<std::pair<RawBlock *, DataTable *>> compaction_queue_;
  transaction::TransactionManager txn_manager_;
};
}  // namespace terrier::storage
