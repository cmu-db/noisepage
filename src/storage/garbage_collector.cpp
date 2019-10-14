#include "storage/garbage_collector.h"
#include <unordered_set>
#include <utility>
#include "common/macros.h"
#include "loggers/storage_logger.h"
#include "storage/data_table.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {

std::pair<uint32_t, uint32_t> GarbageCollector::PerformGarbageCollection(transaction::TransactionQueue &txns_to_unlink) {
  if (observer_ != nullptr) observer_->ObserveGCInvocation();
  timestamp_manager_->CheckOutTimestamp();
  const transaction::timestamp_t oldest_txn = timestamp_manager_->OldestTransactionStartTime();
  uint32_t txns_unlinked = ProcessUnlinkQueue(oldest_txn, txns_to_unlink);
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): txns_unlinked: {}", txns_unlinked);

  ProcessDeferredActions(oldest_txn); //TODO (Yashwanth) move this to some other function, this isn't going to be called on a thread
  ProcessIndexes(); // TODO (Yashwanth) move this to some other function, this isn't going to be called on a thread
  return std::make_pair(0, txns_unlinked);
}

void GarbageCollector::CleanupTransaction(transaction::TransactionContext *txn) {
  if (txn->IsReadOnly()) {
      // This is a read-only transaction so this is safe to immediately delete
      delete txn;
  } else {
    // TODO (yash) If we don't have async threads handling the perform gc below we need to change process here
    // 1. Acquire txn_to_unlink_latch_ 
    // 2. push_front
    // 3. num_txns_to_unlink++
    // 4. Check if num_txns_to_unlink > MAX_OUTSTANDING_UNLINK_TRANSACTIONS
    //    4a. If true: transaction::TransactionQueue txns_unlink(std::move(txns_to_unlink_));
    //    4b. Set perform gc bool to true
    // 5. Release latch
    // 6. PerformGarbageCollection(txns_to_unlink_); //don't want to block txns from comitting while this is running
    common::SpinLatch::ScopedSpinLatch guard(&txn_to_unlink_latch_);
    txns_to_unlink_.push_front(txn);
    num_txns_to_unlink_++;

    // If more more than max number of transactions to link then clear list and unlink
    if(num_txns_to_unlink_ > MAX_OUTSTANDING_UNLINK_TRANSACTIONS){
      transaction::TransactionQueue txns_unlink(std::move(txns_to_unlink_));
      deferred_action_manager_->RegisterDeferredAction([=]() { //TODO (Yashwanth) don't need to this to be a deferred event, just need some kind of async task registration mechanism
        PerformGarbageCollection(txns_unlink);
      });
      num_txns_to_unlink_ = 0;
    }
  } 
}

uint32_t GarbageCollector::ProcessUnlinkQueue(
  transaction::timestamp_t oldest_txn, transaction::TransactionQueue &txns_to_unlink) {
  transaction::TransactionContext *txn = nullptr;

  uint32_t txns_processed = 0;
  // Certain transactions might not be yet safe to gc. Need to requeue them
  transaction::TransactionQueue requeue;
  // It is sufficient to truncate each version chain once in a GC invocation because we only read the maximal safe
  // timestamp once, and the version chain is sorted by timestamp. Here we keep a set of slots to truncate to avoid
  // wasteful traversals of the version chain.
  std::unordered_set<TupleSlot> visited_slots;
  transaction::TransactionQueue txns_to_deallocate;

  // Process every transaction in the unlink queue
  while (!txns_to_unlink.empty()) {
    txn = txns_to_unlink.front();
    txns_to_unlink.pop_front();

    if (txn->IsReadOnly()) {
      // This is a read-only transaction so this is safe to immediately delete
      delete txn;
      txns_processed++;
    } else {
      // Safe to garbage collect.
      for (auto &undo_record : txn->undo_buffer_) {
        // It is possible for the table field to be null, for aborted transaction's last conflicting record
        DataTable *&table = undo_record.Table();
        // Each version chain needs to be traversed and truncated at most once every GC period. Check
        // if we have already visited this tuple slot; if not, proceed to prune the version chain.
        if (table != nullptr && visited_slots.insert(undo_record.Slot()).second)
          TruncateVersionChain(table, undo_record.Slot(), oldest_txn);
        // Regardless of the version chain we will need to reclaim deleted slots and any dangling pointers to varlens,
        // unless the transaction is aborted, and the record holds a version that is still visible.
        if (!txn->Aborted()) {
          ReclaimSlotIfDeleted(&undo_record);
          ReclaimBufferIfVarlen(txn, &undo_record);
        }
        if (observer_ != nullptr) observer_->ObserveWrite(undo_record.Slot().GetBlock());
      }
      txns_to_deallocate.push_front(txn);
      txns_processed++;
    } 
  }


  deferred_action_manager_->RegisterDeferredAction([=]() {
    for(transaction::TransactionContext *ctx : txns_to_deallocate) {
      delete ctx;
    }
  });

  return txns_processed;
}

void GarbageCollector::ProcessDeferredActions(transaction::timestamp_t oldest_txn) {
  if (deferred_action_manager_ != DISABLED) {
    // TODO(Tianyu): Eventually we will remove the GC and implement version chain pruning with deferred actions
    deferred_action_manager_->Process(oldest_txn);
  }
}

void GarbageCollector::TruncateVersionChain(DataTable *const table, const TupleSlot slot,
                                            const transaction::timestamp_t oldest) const {
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

void GarbageCollector::ReclaimSlotIfDeleted(UndoRecord *const undo_record) const {
  if (undo_record->Type() == DeltaRecordType::DELETE) undo_record->Table()->accessor_.Deallocate(undo_record->Slot());
}

void GarbageCollector::ReclaimBufferIfVarlen(transaction::TransactionContext *const txn,
                                             UndoRecord *const undo_record) const {
  const TupleAccessStrategy &accessor = undo_record->Table()->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();
  switch (undo_record->Type()) {
    case DeltaRecordType::INSERT:
      return;  // no possibility of outdated varlen to gc
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

// TODO (yash) move this to some other thread
void GarbageCollector::RegisterIndexForGC(const common::ManagedPointer<index::Index> index) {
  TERRIER_ASSERT(index != nullptr, "Index cannot be nullptr.");
  common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
  TERRIER_ASSERT(indexes_.count(index) == 0, "Trying to register an index that has already been registered.");
  indexes_.insert(index);
}

// TODO (yash) move this to some other thread
void GarbageCollector::UnregisterIndexForGC(const common::ManagedPointer<index::Index> index) {
  TERRIER_ASSERT(index != nullptr, "Index cannot be nullptr.");
  common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
  TERRIER_ASSERT(indexes_.count(index) == 1, "Trying to unregister an index that has not been registered.");
  indexes_.erase(index);
}

void GarbageCollector::ProcessIndexes() {
  common::SharedLatch::ScopedSharedLatch guard(&indexes_latch_);
  for (const auto &index : indexes_) index->PerformGarbageCollection();
}

}  // namespace terrier::storage
