#include "storage/garbage_collector.h"
#include <utility>
#include "common/container/concurrent_queue.h"
#include "common/macros.h"
#include "loggers/storage_logger.h"
#include "storage/data_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {

std::pair<uint32_t, uint32_t> GarbageCollector::PerformGarbageCollection() {
  uint32_t txns_deallocated = ProcessDeallocateQueue();
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): txns_deallocated: {}", txns_deallocated);
  uint32_t txns_unlinked = ProcessUnlinkQueue();
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): txns_unlinked: {}", txns_unlinked);
  if (txns_unlinked > 0) {
    // Only update this field if we actually unlinked anything, otherwise we're being too conservative about when it's
    // safe to deallocate the transactions in our queue.
    last_unlinked_ = txn_manager_->GetTimestamp();
  }
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): last_unlinked_: {}",
                    static_cast<uint64_t>(last_unlinked_));
  return std::make_pair(txns_deallocated, txns_unlinked);
}

uint32_t GarbageCollector::ProcessDeallocateQueue() {
  const transaction::timestamp_t oldest_txn = txn_manager_->OldestTransactionStartTime();
  uint32_t txns_processed = 0;
  transaction::TransactionContext *txn = nullptr;

  if (transaction::TransactionUtil::NewerThan(oldest_txn, last_unlinked_)) {
    transaction::TransactionQueue requeue;
    // All of the transactions in my deallocation queue were unlinked before the oldest running txn in the system.
    // We are now safe to deallocate these txns because no running transaction should hold a reference to them anymore
    while (!txns_to_deallocate_.empty()) {
      txn = txns_to_deallocate_.front();
      txns_to_deallocate_.pop_front();
      if (txn->log_processed_) {
        // If the log manager is already done with this transaction, it is safe to deallocate
        delete txn;
        txns_processed++;
      } else {
        // Otherwise, the log manager may need to read the varlen pointer, so we cannot deallocate yet
        requeue.push_front(txn);
      }
    }
    txns_to_deallocate_ = std::move(requeue);
  }

  return txns_processed;
}

uint32_t GarbageCollector::ProcessUnlinkQueue() {
  transaction::TransactionContext *txn = nullptr;

  // Get the completed transactions from the TransactionManager
  transaction::TransactionQueue completed_txns = txn_manager_->CompletedTransactionsForGC();
  if (!completed_txns.empty()) {
    // Append to our local unlink queue
    txns_to_unlink_.splice_after(txns_to_unlink_.cbefore_begin(), std::move(completed_txns));
  }

  uint32_t txns_processed = 0;
  transaction::TransactionQueue requeue;

  // Get active_txns in descending sorted order
  std::vector<transaction::timestamp_t> active_txns = txn_manager_->GetActiveTxns();
  ;
  std::sort(active_txns.begin(), active_txns.end(), std::greater<transaction::timestamp_t>());

  // Process every transaction in the unlink queue

  while (!txns_to_unlink_.empty()) {
    txn = txns_to_unlink_.front();
    txns_to_unlink_.pop_front();
    if (txn->undo_buffer_.Empty()) {
      // This is a read-only transaction so this is safe to immediately delete
      delete txn;
      txns_processed++;
    } else if (!transaction::TransactionUtil::Committed(txn->TxnId().load())) {
      // This is an aborted txn. There is nothing to unlink because Rollback() handled that already, but we still need
      // to safely free the txn
      txns_to_deallocate_.push_front(txn);
      txns_processed++;
    } else {
      // This is a txn that may or may not be visible to any running txns. Proceed with unlinking its UndoRecords
      // with an Interval GC approach
      bool all_unlinked = true;
      for (auto &undo_record : txn->undo_buffer_) {
        all_unlinked = all_unlinked && ProcessUndoRecord(txn, &undo_record, &active_txns);
      }
      if (all_unlinked) {
        // We unlinked all of the UndoRecords for this txn, so we can add it to the deallocation queue
        txns_to_deallocate_.push_front(txn);
        txns_processed++;
      } else {
        // We didn't unlink all of the UndoRecords (UnlinkUndoRecord returned false due to a write-write conflict),
        // requeue txn for next GC run. Unlinked UndoRecords will be skipped on the next time around since we use the
        // table pointer of an UndoRecord as the internal marker of being unlinked or not
        requeue.push_front(txn);
      }
    }
  }

  // Requeue any txns that we were still visible to running transactions
  if (!requeue.empty()) {
    txns_to_unlink_ = transaction::TransactionQueue(std::move(requeue));
  }

  return txns_processed;
}

bool GarbageCollector::ProcessUndoRecord(transaction::TransactionContext *const txn, UndoRecord *const undo_record,
                                         std::vector<transaction::timestamp_t> *const active_txns) {
  DataTable *&table = undo_record->Table();
  // if this UndoRecord has already been processed, we can skip it
  if (table == nullptr) return true;
  // no point in trying to reclaim slots or do any further operation if cannot safely unlink
  if (!UnlinkUndoRecord(txn, undo_record, active_txns)) return false;
  return true;
}

// TODO(pulkit): rename this function to UnlinkUndoRecordTuple
bool GarbageCollector::UnlinkUndoRecord(transaction::TransactionContext *const txn, UndoRecord *const undo_record,
                                        std::vector<transaction::timestamp_t> *const active_txns) {
  TERRIER_ASSERT(txn->TxnId().load() == undo_record->Timestamp().load(),
                 "This undo_record does not belong to this txn.");
  DataTable *table = undo_record->Table();
  if (table == nullptr) {
    // This UndoRecord has already been unlinked, so we can skip it
    return true;
  }
  const TupleSlot slot = undo_record->Slot();
  const TupleAccessStrategy &accessor = table->accessor_;

  UndoRecord *version_ptr;
  version_ptr = table->AtomicallyReadVersionPtr(slot, accessor);
  TERRIER_ASSERT(version_ptr != nullptr, "GC should not be trying to unlink in an empty version chain.");

  // Perform interval gc for the entire version chain excluding the head of the chain
  bool rest_collected = UnlinkUndoRecordRestOfChain(txn, version_ptr, active_txns);
  bool head_collected = UnlinkUndoRecordHead(txn, version_ptr, active_txns);
  return head_collected || rest_collected;
}

bool GarbageCollector::UnlinkUndoRecordHead(transaction::TransactionContext *const txn, UndoRecord *const head,
                                            std::vector<transaction::timestamp_t> *const active_txns) const {
  DataTable *table = head->Table();
  if (table == nullptr) {
    // This UndoRecord has already been unlinked, so we can skip it
    return true;
  }
  const TupleSlot slot = head->Slot();
  const TupleAccessStrategy &accessor = table->accessor_;
  // Perform gc for head of the chain
  // Assuming can garbage collect any version greater than the oldest timestamp
  transaction::timestamp_t version_ptr_timestamp = head->Timestamp().load();
  // If there are no active transactions, or if the version pointer is older than the oldest active transaction,
  // Collect the head of the chain using compare and swap
  // Note that active_txns is sorted in descending order, so its tail should have the oldest txn's timestamp
  if (active_txns->empty() || version_ptr_timestamp < active_txns->back()) {
    if (transaction::TransactionUtil::Committed(head->Timestamp().load())) {
      UndoRecord *to_be_unlinked = head;
      // Our UndoRecord is the first in the chain, handle contention on the write lock with CAS
      if (table->CompareAndSwapVersionPtr(slot, accessor, head, head->Next())) {
        UnlinkUndoRecordVersion(txn, to_be_unlinked);
        if (version_ptr_timestamp == txn->TxnId().load()) {
          // If I was the header, make collected true, because I was collected
          return true;
        }
      }
      // Someone swooped the VersionPointer while we were trying to swap it (aka took the write lock)
    }
  }
  return false;
}

bool GarbageCollector::UnlinkUndoRecordRestOfChain(transaction::TransactionContext *const txn,
                                                   UndoRecord *const version_chain_head,
                                                   std::vector<transaction::timestamp_t> *const active_txns) {
  // If no chain is passed, nothing is collected
  if (version_chain_head == nullptr) {
    return false;
  }

  DataTable *table = version_chain_head->Table();
  if (table == nullptr) {
    transaction::timestamp_t version_ptr_timestamp = version_chain_head->Timestamp().load();
    // This UndoRecord has already been unlinked, so we can skip it
    if (version_ptr_timestamp == txn->TxnId().load()) {
      // If I was the header, make collected true, because I was collected
      return true;
    }
    return false;
  }
  // Otherwise collect as much as possible and return true if version belonging to txn was collected
  // Don't collect version_chain_head though
  bool collected = false;
  const TupleAccessStrategy &accessor = table->accessor_;
  UndoRecord *curr = version_chain_head;
  UndoRecord *next = curr->Next();
  auto active_txns_iter = active_txns->begin();
  bool do_compaction = false;

  // Create a temporary projected row for undo record compaction
  std::pair<RecordBufferSegment *, ProjectedRow *> result;
  storage::RecordBufferSegment *buffer_segment = nullptr;
  storage::ProjectedRow *projected_row = nullptr;

  // a version chain is guaranteed to not change when not at the head (assuming single-threaded GC), so we are safe
  // to traverse and update pointers without CAS
  while (next != nullptr && active_txns_iter != active_txns->end()) {
    if (*active_txns_iter >= curr->Timestamp().load()) {
      // curr is the version that *active_txns_iter would be reading
      active_txns_iter++;
    } else if (next->Timestamp().load() > *active_txns_iter) {
      // curr is visible to txns traversed till now, so can't GC curr. next is not visible to any txn, attempt GC
      // Collect next only if it was committed, this prevents collection of partial rollback versions
      if (transaction::TransactionUtil::Committed(next->Timestamp().load())) {
        // Since *active_txns_iter is not reading next, that means no one is reading this
        // And so we can reclaim next
        if (!do_compaction) {
          // This is the first undo record to compact
          if (buffer_segment != nullptr) {
            // Release the buffer segment for projected row
            ReleaseProjectedRow(buffer_segment);
          }
          // Initialise the projected row with next's undo record
          result = NewProjectedRow(next->Delta());
          buffer_segment = result.first;
          projected_row = result.second;
          do_compaction = true;
        } else {
          // Already have a base undo record. Apply this undo record on top of that
          switch (next->Type()) {
            case DeltaRecordType::UPDATE:
              // Normal delta to be applied. Does not modify the logical delete column.
              StorageUtil::ApplyDelta(accessor.GetBlockLayout(), *(next->Delta()), projected_row);
              break;
            case DeltaRecordType::INSERT:
            case DeltaRecordType::DELETE:;
          }
        }
        if (next->Timestamp().load() == txn->TxnId().load()) {
          // Was my undo record reclaimed?
          collected = true;
        }

        // Unlink next
        curr->Next().store(next->Next().load());
        UnlinkUndoRecordVersion(txn, next);
      } else {
        // If next wasn't committed, don't collect it
        curr = curr->Next();
      }
    } else {
      if (do_compaction) {
        // We have compacted some undo records till now
        // next undo record can't be GC'd. Merge next with the compacted undo records.
        switch (next->Type()) {
          case DeltaRecordType::UPDATE:
            StorageUtil::ApplyDelta(accessor.GetBlockLayout(), *(next->Delta()), projected_row);
            break;
          case DeltaRecordType::INSERT:
          case DeltaRecordType::DELETE:;
        }
        UndoRecord *compacted_undo_record =
            UndoRecordForUpdate(table, next->Slot(), *projected_row, next->Timestamp().load());
        // Add this to the version chain
        compacted_undo_record->Next().store(next->Next().load());
        // Set curr to point to the compacted undo record
        curr->Next().store(compacted_undo_record);
        // Compaction is over
        do_compaction = false;
      }
      // curr was not claimed in the previous iteration, so possibly someone might use it
      curr = curr->Next();
    }
    next = curr->Next();
  }

  // active_trans_iter ends but there are still elements in the version chain, Can GC everything below
  while (next != nullptr) {
    if (next->Timestamp().load() == txn->TxnId().load()) {
      // Was my undo record reclaimed?
      collected = true;
    }

    // Unlink next
    curr->Next().store(next->Next().load());
    UnlinkUndoRecordVersion(txn, next);

    // Move curr pointer ahead
    curr = curr->Next();
    next = curr->Next();
  }

  if (buffer_segment != nullptr) {
    // Release the buffer segment for projected row
    ReleaseProjectedRow(buffer_segment);
  }
  return collected;
}

void GarbageCollector::UnlinkUndoRecordVersion(transaction::TransactionContext *const txn,
                                               UndoRecord *const undo_record) const {
  DataTable *&table = undo_record->Table();
  ReclaimSlotIfDeleted(undo_record);
  // TODO(pulkit): This should not require a specific txn to unlink, check how to do this => important
  ReclaimBufferIfVarlen(txn, undo_record);
  // mark the record as fully processed
  table = nullptr;
}

void GarbageCollector::ReclaimSlotIfDeleted(UndoRecord *undo_record) const {
  if (undo_record->Type() == DeltaRecordType::DELETE) undo_record->Table()->accessor_.Deallocate(undo_record->Slot());
}

void GarbageCollector::ReclaimBufferIfVarlen(transaction::TransactionContext *txn, UndoRecord *undo_record) const {
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
  }
}
storage::UndoRecord *GarbageCollector::UndoRecordForUpdate(storage::DataTable *const table,
                                                           const storage::TupleSlot slot,
                                                           const storage::ProjectedRow &redo,
                                                           const transaction::timestamp_t ts) {
  const uint32_t size = storage::UndoRecord::Size(redo);
  return storage::UndoRecord::InitializeUpdate(undo_buffer_.NewEntry(size), ts, slot, table, redo);
}

std::pair<RecordBufferSegment *, ProjectedRow *> GarbageCollector::NewProjectedRow(const ProjectedRow *row) {
  RecordBufferSegment *new_segment = txn_manager_->buffer_pool_->Get();
  auto *projected_row = new_segment->Reserve(row->Size());
  memcpy(projected_row, row, row->Size());
  return {new_segment, reinterpret_cast<ProjectedRow *>(projected_row)};
}

void GarbageCollector::ReleaseProjectedRow(RecordBufferSegment *buffer_segment) {
  txn_manager_->buffer_pool_->Release(buffer_segment);
}
}  // namespace terrier::storage
