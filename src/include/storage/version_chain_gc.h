#pragma once
#include <vector>
#include <unordered_set>
#include <utility>
#include "storage/data_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/deferred_action_manager.h"

namespace terrier::storage {
class VersionChainGC {
 public:
  explicit VersionChainGC(transaction::DeferredActionManager *deferred_action_manager)
      : deferred_action_manager_(deferred_action_manager) {}

  void Unlink(transaction::TransactionContext *txn, transaction::timestamp_t safe_epoch) {
    // Can immediately delete read-only transactions
    if (txn->undo_buffer_.Empty()) {
      delete txn;
      return;
    }

    // Clear the filtering set of slots if the epoch we are working in changes
    if (safe_epoch != curr_safe_epoch_) visited_slots_.clear();

    for (auto &undo_record : txn->undo_buffer_) {
      // It is possible for the table field to be null, for aborted transaction's last conflicting record
      DataTable *&table = undo_record.Table();
      // Each version chain needs to be traversed and truncated at most once every GC period. Check
      // if we have already visited this tuple slot; if not, proceed to prune the version chain.
      if (table != nullptr && visited_slots_.insert(undo_record.Slot()).second)
        TruncateVersionChain(table, undo_record.Slot(), safe_epoch);
      // Regardless of the version chain we will need to reclaim deleted slots and any dangling pointers to varlens,
      // unless the transaction is aborted, and the record holds a version that is still visible.
      if (!txn->Aborted()) {
        ReclaimSlotIfDeleted(&undo_record);
        ReclaimBufferIfVarlen(txn, &undo_record);
      }
    }

    // Delete only after no threads can see the unlinked version
    deferred_action_manager_->RegisterDeferredAction([=](transaction::timestamp_t) {
      // If the log manager is not done with this transaction, it is not safe to deallocate
      if (!txn->log_processed_) return false;
      delete txn;
      return true;
    });
  }

 private:
  // It is sufficient to truncate each version chain once in a GC invocation because we only read the maximal safe
  // timestamp once, and the version chain is sorted by timestamp. Here we keep a set of slots to truncate to avoid
  // wasteful traversals of the version chain.
  transaction::DeferredActionManager *deferred_action_manager_;
  std::unordered_set<TupleSlot> visited_slots_;
  transaction::timestamp_t curr_safe_epoch_{-1}; // invalid epoch to start

  static void TruncateVersionChain(DataTable *const table,
                                   const TupleSlot slot,
                                   const transaction::timestamp_t oldest) {
    const TupleAccessStrategy &accessor = table->accessor_;
    UndoRecord *const version_ptr = table->AtomicallyReadVersionPtr(slot, accessor);
    // This is a legitimate case where we truncated the version chain but had to restart because the previous head
    // was aborted.
    if (version_ptr == nullptr) return;

    // We need to special case the head of the version chain because contention with running transactions can happen
    // here. Instead of a blind update we will need to CAS and prune the entire version chain if the head of the version
    // chain can be GCed.
    if (transaction::TransactionUtil::NewerThan(oldest, version_ptr->Timestamp().load())) {
      if (!table->CompareAndSwapVersionPtr(slot, accessor, version_ptr, nullptr))
        // Keep retrying while there are conflicts, since we only invoke truncate once per GC period for every
        // version chain.
        TruncateVersionChain(table, slot, oldest);
      return;
    }

    // a version chain is guaranteed to not change when not at the head (assuming single-threaded GC), so we are safe
    // to traverse and update pointers without CAS
    UndoRecord *curr = version_ptr;
    UndoRecord *next;
    // Traverse until we find the earliest UndoRecord that can be unlinked.
    while (true) {
      next = curr->Next();
      // This is a legitimate case where we truncated the version chain but had to restart because the previous head
      // was aborted.
      if (next == nullptr) return;
      if (transaction::TransactionUtil::NewerThan(oldest, next->Timestamp().load())) break;
      curr = next;
    }
    // The rest of the version chain must also be invisible to any running transactions since our version
    // is newest-to-oldest sorted.
    curr->Next().store(nullptr);

    // If the head of the version chain was not committed, it could have been aborted and requires a retry.
    if (curr == version_ptr && !transaction::TransactionUtil::Committed(version_ptr->Timestamp().load()) &&
        table->AtomicallyReadVersionPtr(slot, accessor) != version_ptr)
      TruncateVersionChain(table, slot, oldest);
  }

  void ReclaimSlotIfDeleted(UndoRecord *undo_record) const {
    if (undo_record->Type() == DeltaRecordType::DELETE) undo_record->Table()->accessor_.Deallocate(undo_record->Slot());
  }

  void ReclaimBufferIfVarlen(transaction::TransactionContext *txn, UndoRecord *undo_record) const {
    const TupleAccessStrategy &accessor = undo_record->Table()->accessor_;
    const BlockLayout &layout = accessor.GetBlockLayout();
    switch (undo_record->Type()) {
      case DeltaRecordType::INSERT:return;  // no possibility of outdated varlen to gc
      case DeltaRecordType::DELETE:
        // TODO(Tianyu): Potentially need to be more efficient than linear in column size?
        for (uint16_t i = 0; i < layout.NumColumns(); i++) {
          col_id_t col_id(i);
          // Okay to include version vector, as it is never varlen
          if (layout.IsVarlen(col_id)) {
            auto *varlen = reinterpret_cast<VarlenEntry *>(accessor.AccessWithNullCheck(undo_record->Slot(), col_id));
            if (varlen != nullptr && varlen->NeedReclaim()) txn->loose_ptrs_.push_back(varlen->Content());
          }
        }
        break;
      case DeltaRecordType::UPDATE:
        // TODO(Tianyu): This might be a really bad idea for large deltas...
        for (uint16_t i = 0; i < undo_record->Delta()->NumColumns(); i++) {
          col_id_t col_id = undo_record->Delta()->ColumnIds()[i];
          if (layout.IsVarlen(col_id)) {
            auto *varlen = reinterpret_cast<VarlenEntry *>(undo_record->Delta()->AccessWithNullCheck(i));
            if (varlen != nullptr && varlen->NeedReclaim()) txn->loose_ptrs_.push_back(varlen->Content());
          }
        }
        break;
      default:
        throw std::runtime_error("unexpected delta record type");
    }
  }
};
}  // namespace terrier::storage
