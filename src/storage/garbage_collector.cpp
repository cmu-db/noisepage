#include "storage/garbage_collector.h"
#include <algorithm>
#include <functional>
#include <unordered_set>
#include <utility>
#include <vector>
#include "common/container/concurrent_queue.h"
#include "common/macros.h"
#include "loggers/storage_logger.h"
#include "storage/data_table.h"
#include "storage/projected_row.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {

std::pair<uint32_t, uint32_t> GarbageCollector::PerformGarbageCollection() {
  // Create the UndoBuffer for this GC run
  delta_record_compaction_buffer_ = new UndoBuffer(txn_manager_->GetBufferPool());

  ProcessDeferredActions();
  uint32_t txns_deallocated = ProcessDeallocateQueue();
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): txns_deallocated: {}", txns_deallocated);
  uint32_t txns_unlinked = ProcessUnlinkQueue();
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): txns_unlinked: {}", txns_unlinked);
  // Update the last unlinked timestamp every GC run conservatively whether or not any transaction/compacted
  // undo record is unlinked.
  last_unlinked_ = txn_manager_->GetTimestamp();
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): last_unlinked_: {}",
                    static_cast<uint64_t>(last_unlinked_));

  // The compaction buffer will contain compacted undo records created during the last run of Interval GC above.
  // Handover the buffer containing compacted undo records to the GC.
  // The GC will take care of deallocating the compacted undo records contained in the buffer and the buffer itself.
  if (delta_record_compaction_buffer_->Empty()) {
    // Can directly deallocate compaction buffer as it is empty
    delete delta_record_compaction_buffer_;
  } else {
    // Push the compaction buffer for unlinking
    buffers_to_unlink_.push_front(delta_record_compaction_buffer_);
  }

  // Deallocate varlen pointers
  for (const byte *ptr : varlen_loose_ptrs_) delete[] ptr;
  varlen_loose_ptrs_.clear();

  return std::make_pair(txns_deallocated, txns_unlinked);
}

uint32_t GarbageCollector::ProcessDeallocateQueue() {
  const transaction::timestamp_t oldest_txn = txn_manager_->OldestTransactionStartTime();
  uint32_t txns_processed = 0;
  transaction::TransactionContext *txn = nullptr;
  storage::UndoBuffer *buf = nullptr;

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

    // All of the undo records in my buffers were unlinked before the oldest running txn in the system.
    // We are now safe to deallocate these buffers because no running transaction should hold a reference to them
    // anymore
    while (!buffers_to_deallocate_.empty()) {
      buf = buffers_to_deallocate_.front();
      buffers_to_deallocate_.pop_front();
      delete buf;
    }
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
  std::sort(active_txns.begin(), active_txns.end(), std::greater<>());

  // The GC should not collect an undo record by a txn that started after getting the active txns. So the txn manager
  // ensures that there is at least one timestamp in the vector (might be the time that the GC requested active_txns)
  TERRIER_ASSERT(!active_txns.empty(), "There should be at least one active txn timestamp for the GC!");

  std::unordered_set<TupleSlot> visited_slots;

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
        all_unlinked = all_unlinked && ProcessUndoRecord(&undo_record, &active_txns, &visited_slots);
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

  storage::UndoBuffer *buf = nullptr;
  std::forward_list<storage::UndoBuffer *> buf_requeue;

  // Process every compaction buffer in the buffer unlink queue
  while (!buffers_to_unlink_.empty()) {
    buf = buffers_to_unlink_.front();
    buffers_to_unlink_.pop_front();
    if (buf->Empty()) {
      // This compaction buffer is empty so it is safe to delete it
      delete buf;
    } else {
      // This is a list of undo records that may or may not be visible to any running txns. Proceed with unlinking them
      // with an Interval GC approach
      bool all_unlinked = true;
      for (auto &undo_record : *buf) {
        all_unlinked = ProcessUndoRecord(&undo_record, &active_txns, &visited_slots) && all_unlinked;
      }
      if (all_unlinked) {
        // We unlinked all of the UndoRecords for this compaction buffer, so we can add it to the deallocation queue
        buffers_to_deallocate_.push_front(buf);
      } else {
        // We didn't unlink all of the UndoRecords (UnlinkUndoRecord returned false due to a write-write conflict),
        // requeue buf for next GC run. Unlinked UndoRecords will be skipped on the next time around since we use the
        // table pointer of an UndoRecord as the internal marker of being unlinked or not
        buf_requeue.push_front(buf);
      }
    }
  }

  // Requeue any txns that have an undo record still visible to some running transaction
  if (!requeue.empty()) {
    txns_to_unlink_ = transaction::TransactionQueue(std::move(requeue));
  }

  // Requeue any compaction buffers that that have an undo record still visible to some running transaction
  if (!buf_requeue.empty()) {
    buffers_to_unlink_ = std::forward_list<storage::UndoBuffer *>(std::move(buf_requeue));
  }

  return txns_processed;
}

bool GarbageCollector::ProcessUndoRecord(UndoRecord *const undo_record,
                                         std::vector<transaction::timestamp_t> *const active_txns,
                                         std::unordered_set<TupleSlot> *visited_slots) {
  DataTable *table = undo_record->Table();
  // If this UndoRecord has already been processed, we can skip it
  if (undo_record->txnptr_.IsNull()) return true;
  const TupleSlot slot = undo_record->Slot();
  // Process this tuple only
  if (visited_slots->insert(slot).second) {
    // Perform interval gc for the entire version chain excluding the head of the chain
    ProcessTupleVersionChain(table, slot, active_txns);
    ProcessTupleVersionChainHead(table, slot, active_txns);
  }

  return undo_record->txnptr_.IsNull();
}

void GarbageCollector::ProcessTupleVersionChainHead(DataTable *const table, TupleSlot slot,
                                                    std::vector<transaction::timestamp_t> *const active_txns) {
  UndoRecord *version_chain_head;
  const TupleAccessStrategy &accessor = table->accessor_;
  version_chain_head = table->AtomicallyReadVersionPtr(slot, accessor);
  if (version_chain_head == nullptr) {
    // This version chain is empty, so we can skip it
    return;
  }
  // Perform gc for head of the chain
  // Assuming can garbage collect any version greater than the oldest timestamp
  transaction::timestamp_t version_ptr_timestamp = version_chain_head->Timestamp().load();
  // If there are no active transactions, or if the version pointer is older than the oldest active transaction,
  // Collect the head of the chain using compare and swap
  // Note that active_txns is sorted in descending order, so its tail should have the oldest txn's timestamp
  if (version_ptr_timestamp < active_txns->back()) {
    if (transaction::TransactionUtil::Committed(version_ptr_timestamp)) {
      // Our UndoRecord is the first in the chain, handle contention on the write lock with CAS
      if (table->CompareAndSwapVersionPtr(slot, accessor, version_chain_head, version_chain_head->Next())) {
        UnlinkUndoRecordVersion(version_chain_head);
      }
      // Someone swooped the VersionPointer while we were trying to swap it (aka took the write lock)
    }
  }
}

void GarbageCollector::ProcessDeferredActions() {
  auto new_actions = txn_manager_->DeferredActionsForGC();
  while (!new_actions.empty()) {
    deferred_actions_.push(new_actions.front());
    new_actions.pop();
  }

  const transaction::timestamp_t oldest_txn = txn_manager_->OldestTransactionStartTime();

  // Execute as many deferred actions as we can at this time.
  while ((!deferred_actions_.empty()) && deferred_actions_.front().first <= oldest_txn) {
    deferred_actions_.front().second();
    deferred_actions_.pop();
  }
}

void GarbageCollector::ProcessTupleVersionChain(DataTable *const table, TupleSlot slot,
                                                std::vector<transaction::timestamp_t> *const active_txns) {
  // Read the Version Pointer of this tuple
  const TupleAccessStrategy &accessor = table->accessor_;
  UndoRecord *version_chain_head;
  version_chain_head = table->AtomicallyReadVersionPtr(slot, accessor);

  // If no chain is passed, nothing is collected
  if (version_chain_head == nullptr) {
    return;
  }

  // Otherwise collect as much as possible
  // Don't collect version_chain_head though
  auto active_txns_iter = active_txns->begin();
  // The undo record which will point to the compacted undo record
  UndoRecord *start_record = version_chain_head;
  uint32_t interval_length = 0;

  UndoRecord *curr = version_chain_head;
  UndoRecord *next = curr->Next();
  // Skip all the uncommitted undo records, this prevents collection of partial rollback versions.
  // Also skip the first committed record to simplify contention on the version chain between Rollback and GC.
  while (next != nullptr && !transaction::TransactionUtil::Committed(curr->Timestamp().load())) {
    // Update curr and next
    curr = curr->Next();
    next = curr->Next();
  }

  // a version chain is guaranteed to not change when not at the head (assuming single-threaded GC), so we are safe
  // to traverse and update pointers without CAS
  while (next != nullptr && active_txns_iter != active_txns->end()) {
    if (transaction::TransactionUtil::NewerThan(*active_txns_iter, curr->Timestamp().load())) {
      // curr is the version that *active_txns_iter would be reading
      active_txns_iter++;
    } else if (transaction::TransactionUtil::NewerThan(next->Timestamp().load(), *active_txns_iter)) {
      // curr is visible to txns traversed till now, so can't GC curr. next is not visible to any txn, attempt GC
      // Since *active_txns_iter is not reading next, that means no one is reading this
      // And so we can reclaim next
      if (interval_length == 0) {
        // This only happens when the first compaction is initiated. Other compactions begin in Case 3.
        // This is the first undo record to compact
        BeginCompaction(&start_record, curr, next, &interval_length);
      } else {
        // Process the undo record to determine the set of columns for the compacted record.
        ReadUndoRecord(start_record, next, &interval_length);
      }
      // Update curr
      curr = curr->Next();
    } else {
      if (interval_length > 1) {
        // Create the undo record by a second traversal through the records to be compacted.
        UndoRecord *compacted_undo_record = CreateUndoRecord(start_record, next);
        // Copy the varlen attributes of the compacted record
        CopyVarlen(compacted_undo_record);
        // Link the compacted record to the version chain.
        LinkCompactedUndoRecord(start_record, &curr, next, compacted_undo_record);
      }
      // Set interval length to 0
      EndCompaction(&interval_length);
      // Begin compaction for the next interval
      BeginCompaction(&start_record, curr, next, &interval_length);
      // curr was not claimed in the previous iteration, so possibly someone might use it
      curr = curr->Next();
    }
    next = curr->Next();
  }

  if (interval_length > 1) {
    // Create the undo record by a second traversal through the records to be compacted.
    UndoRecord *compacted_undo_record = CreateUndoRecord(start_record, next);
    // Copy the varlen attributes of the compacted record
    CopyVarlen(compacted_undo_record);
    // Link the compacted record to the version chain.
    LinkCompactedUndoRecord(start_record, &curr, next, compacted_undo_record);
  } else if (interval_length == 1) {
    if (transaction::TransactionUtil::NewerThan(active_txns->back(), curr->Timestamp().load())) {
      // Exited out of previous loop such that the last record is not visible to any transaction
      UnlinkUndoRecordVersion(start_record->Next());
      start_record->Next().store(nullptr);
    }
  }
  // Set interval length to 0
  EndCompaction(&interval_length);

  // active_trans_iter ends but there are still elements in the version chain. Can GC everything below
  curr->Next().store(nullptr);
  while (next != nullptr) {
    // Unlink next
    UnlinkUndoRecordVersion(next);
    // Move next pointer ahead
    next = next->Next();
  }
}

void GarbageCollector::UnlinkUndoRecordVersion(UndoRecord *const undo_record) {
  TERRIER_ASSERT(undo_record != nullptr, "Undo Record should not be NULL while Unlinking");
  TERRIER_ASSERT(!undo_record->txnptr_.IsNull(), "Table should not be NULL here");
  ReclaimSlotIfDeleted(undo_record);
  ReclaimVarlen(undo_record);
  // Mark the record as fully processed
  undo_record->txnptr_.Put(nullptr);
}

void GarbageCollector::ReclaimSlotIfDeleted(UndoRecord *const undo_record) const {
  if (undo_record->Type() == DeltaRecordType::DELETE) undo_record->Table()->accessor_.Deallocate(undo_record->Slot());
}

void GarbageCollector::BeginCompaction(UndoRecord **start_record_ptr, UndoRecord *curr, UndoRecord *next,
                                       uint32_t *interval_length_ptr) {
  col_set_.clear();
  // Case 1: next is of type UPDATE
  //         Standard undo record compaction begins
  // Case 2: next is of type INSERT
  //         This can only happen when a transaction is sandwiched between INSERT and another Delta record
  //         INSERT is the last record in the version chain
  // Case 3: next is of type DELETE
  //         DELETE can only be the newest record in the version chain
  TERRIER_ASSERT(next != nullptr, "The next pointer should not be NULL while Unlinking");
  TERRIER_ASSERT(next->Type() != DeltaRecordType::DELETE, "Delete cannot be compacted");
  *start_record_ptr = curr;
  *interval_length_ptr = 1;

  // Compact only if it is an UPDATE record, so only process attributes if it is UPDATE
  if (next->Type() == DeltaRecordType::UPDATE) {
    ProcessUpdateUndoRecordAttributes(next);
  }
}

void GarbageCollector::LinkCompactedUndoRecord(UndoRecord *start_record, UndoRecord **curr_ptr, UndoRecord *end_record,
                                               UndoRecord *compacted_undo_record) {
  UnlinkUndoRecordVersion(start_record->Next());
  // We have compacted some undo records till now
  // end_record undo record can't be GC'd. Set compacted undo record to point to end_record.
  // Add this to the version chain
  compacted_undo_record->Next().store(end_record);
  // Set start_record to point to the compacted undo record
  start_record->Next().store(compacted_undo_record);
  // Added a compacted undo record. So it should be curr
  *curr_ptr = compacted_undo_record;
}

void GarbageCollector::ReadUndoRecord(UndoRecord *start_record, UndoRecord *next, uint32_t *interval_length_ptr) {
  // Already have a base undo record. Apply this undo record on top of that
  switch (next->Type()) {
    case DeltaRecordType::UPDATE:
      // Update the interval length
      (*interval_length_ptr)++;
      // Find the attributes contained in the undo record so that the initialised compacted undo record
      // knows all the columns that will be contained in the compacted undo record
      ProcessUpdateUndoRecordAttributes(next);
      // Mark the undo record as unlinked
      UnlinkUndoRecordVersion(next);
      break;
    case DeltaRecordType::INSERT:
      // Compacting here. So unlink start_record's next
      UnlinkUndoRecordVersion(start_record->Next());
      EndCompaction(interval_length_ptr);
      // Insert undo record can be GC'd so this tuple is not visible
      // Set start_record to point to Insert's next undo record
      start_record->Next().store(next);
      break;
    case DeltaRecordType::DELETE: {
    }
  }
}

void GarbageCollector::EndCompaction(uint32_t *interval_length_ptr) { *interval_length_ptr = 0; }

void GarbageCollector::ProcessUpdateUndoRecordAttributes(UndoRecord *const undo_record) {
  TERRIER_ASSERT(undo_record->Type() == DeltaRecordType::UPDATE, "Only process Update Delta Records here");
  for (uint16_t i = 0; i < undo_record->Delta()->NumColumns(); i++) {
    col_id_t col_id = undo_record->Delta()->ColumnIds()[i];

    // Add col to the col_set_
    col_set_.insert(col_id);
  }
}

UndoRecord *GarbageCollector::CreateUndoRecord(UndoRecord *const start_record, UndoRecord *const end_record) {
  UndoRecord *curr = start_record->Next();
  UndoRecord *first_compacted_record = start_record->Next();
  DataTable *table = first_compacted_record->Table();
  const TupleAccessStrategy &accessor = table->accessor_;

  // Initialize the base Undo record
  UndoRecord *base_undo_record =
      InitializeUndoRecord(first_compacted_record->Timestamp().load(), first_compacted_record->Slot(), table);

  // Apply all the undo records which have to be compacted
  while (curr != end_record) {
    StorageUtil::ApplyDelta(accessor.GetBlockLayout(), *(curr->Delta()), base_undo_record->Delta());
    curr = curr->Next();
  }
  return base_undo_record;
}

UndoRecord *GarbageCollector::InitializeUndoRecord(const transaction::timestamp_t timestamp, const TupleSlot slot,
                                                   DataTable *const table) {
  const TupleAccessStrategy &accessor = table->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();

  // Initialize the projected row with the set of columns we have seen in the first pass
  std::vector<col_id_t> col_id_list(col_set_.begin(), col_set_.end());
  auto init = storage::ProjectedRowInitializer::Create(layout, col_id_list);

  // Get new entry for the undo record from the buffer
  uint32_t size = static_cast<uint32_t>(sizeof(UndoRecord)) + init.ProjectedRowSize();
  byte *head = delta_record_compaction_buffer_->NewEntry(size);

  TERRIER_ASSERT(head != nullptr, "Delta Record Compaction Buffer should not fail to provide memory");

  // Initialize UndoRecord with the projected row
  auto *result = reinterpret_cast<UndoRecord *>(head);
  init.InitializeRow(result->varlen_contents_);

  // Initialize UndoRecord metadata
  result->type_ = DeltaRecordType ::UPDATE;
  result->next_ = nullptr;
  result->timestamp_.store(timestamp);
  result->table_ = table;
  result->slot_ = slot;
  result->txnptr_.SetCompacted();
  return result;
}

void GarbageCollector::CopyVarlen(UndoRecord *undo_record) {
  const TupleAccessStrategy &accessor = undo_record->Table()->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();

  // Iterate through the columns in the delta record, copy the varlen entries and assign the copied ones
  // to the compacted record.
  for (uint16_t i = 0; i < undo_record->Delta()->NumColumns(); i++) {
    col_id_t col_id = undo_record->Delta()->ColumnIds()[i];
    if (layout.IsVarlen(col_id)) {
      auto *varlen = reinterpret_cast<VarlenEntry *>(undo_record->Delta()->AccessWithNullCheck(i));
      if (varlen != nullptr && varlen->NeedReclaim()) {
        // Copy the varlen entry from the original undo record to the compacted undo record
        uint32_t size = varlen->Size();
        byte *buffer = common::AllocationUtil::AllocateAligned(size);
        std::memcpy(buffer, varlen->Content(), size);
        *reinterpret_cast<storage::VarlenEntry *>(undo_record->Delta()->AccessForceNotNull(i)) =
            storage::VarlenEntry::Create(buffer, size, true);
      }
    }
  }
}

void GarbageCollector::ReclaimVarlen(UndoRecord *const undo_record) {
  if (undo_record->txnptr_.IsCompacted()) {
    ReclaimBufferIfVarlenCompacted(undo_record);
  } else {
    ReclaimBufferIfVarlen(undo_record);
  }
}

void GarbageCollector::ReclaimBufferIfVarlenCompacted(UndoRecord *const undo_record) {
  const TupleAccessStrategy &accessor = undo_record->Table()->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();
  for (uint16_t i = 0; i < undo_record->Delta()->NumColumns(); i++) {
    col_id_t col_id = undo_record->Delta()->ColumnIds()[i];
    if (layout.IsVarlen(col_id)) {
      auto *varlen = reinterpret_cast<VarlenEntry *>(undo_record->Delta()->AccessWithNullCheck(i));
      // The varlen entries in the compacted undo record don't need to be logged
      // Can deallocate them right away
      if (varlen != nullptr && varlen->NeedReclaim()) varlen_loose_ptrs_.push_back(varlen->Content());
    }
  }
}

void GarbageCollector::ReclaimBufferIfVarlen(UndoRecord *const undo_record) const {
  transaction::TransactionContext *txn = undo_record->txnptr_.Get();
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

}  // namespace terrier::storage
