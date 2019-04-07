#include "storage/garbage_collector.h"
#include <algorithm>
#include <functional>
#include <utility>
#include <vector>
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
  visited_slots_.clear();
  reclaim_varlen_map_.clear();
  delta_record_compaction_buffer_ = new UndoBuffer(txn_manager_->buffer_pool_);

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
  // Handover compacted buffer for GC
  buffers_to_unlink_.push_front(delta_record_compaction_buffer_);

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
        DeallocateVarlen(&txn->undo_buffer_);
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
      DeallocateVarlen(buf);
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
        all_unlinked = all_unlinked && ProcessUndoRecord(&undo_record, &active_txns);
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
        all_unlinked = ProcessUndoRecord(&undo_record, &active_txns) && all_unlinked;
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
                                         std::vector<transaction::timestamp_t> *const active_txns) {
  DataTable *&table = undo_record->Table();
  // if this UndoRecord has already been processed, we can skip it
  if (table == nullptr) return true;
  // no point in trying to reclaim slots or do any further operation if cannot safely unlink

  if (visited_slots_.insert(undo_record->Slot()).second) {
    UnlinkUndoRecord(undo_record, active_txns);
  }

  table = undo_record->Table();
  return table == nullptr;
}

void GarbageCollector::UnlinkUndoRecord(UndoRecord *const undo_record,
                                        std::vector<transaction::timestamp_t> *const active_txns) {
  DataTable *table = undo_record->Table();
  if (table == nullptr) {
    // This UndoRecord has already been unlinked, so we can skip it
    return;
  }
  const TupleSlot slot = undo_record->Slot();
  const TupleAccessStrategy &accessor = table->accessor_;

  UndoRecord *version_ptr;
  version_ptr = table->AtomicallyReadVersionPtr(slot, accessor);
  TERRIER_ASSERT(version_ptr != nullptr, "GC should not be trying to unlink in an empty version chain.");

  // Perform interval gc for the entire version chain excluding the head of the chain
  UnlinkUndoRecordRestOfChain(version_ptr, active_txns);
}

void GarbageCollector::UnlinkUndoRecordRestOfChain(UndoRecord *const version_chain_head,
                                                   std::vector<transaction::timestamp_t> *const active_txns) {
  // If no chain is passed, nothing is collected
  if (version_chain_head == nullptr) {
    return;
  }

  DataTable *table = version_chain_head->Table();
  if (table == nullptr) {
    // This UndoRecord has already been unlinked, so we can skip it
    return;
  }
  // Otherwise collect as much as possible and return true if version belonging to txn was collected
  // Don't collect version_chain_head though
  auto active_txns_iter = active_txns->begin();
  // The undo record which will point to the compacted undo record
  UndoRecord *start_record = version_chain_head;
  // True if we compacted anything, false otherwise
  uint32_t interval_length = 0;

  UndoRecord *curr = version_chain_head;
  UndoRecord *next = curr->Next();
  // Skip all the uncommitted undo records
  // Collect only committed undo records, this prevents collection of partial rollback versions
  while (next != nullptr && !transaction::TransactionUtil::Committed(next->Timestamp().load())) {
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
      // Collect next only if it was committed, this prevents collection of partial rollback versions
      // Since *active_txns_iter is not reading next, that means no one is reading this
      // And so we can reclaim next
      if (interval_length == 0) {
        // This only happens when the first compaction is initiated. Other compactions begin in Case 3.
        // This is the first undo record to compact
        BeginCompaction(&start_record, curr, next, &interval_length);
      } else {
        ReadUndoRecord(start_record, next, &interval_length);
      }
      // Update curr
      curr = curr->Next();
    } else {
      if (interval_length > 1) {
        UndoRecord *compacted_undo_record = CreateUndoRecord(start_record, next);
        CopyVarlen(compacted_undo_record);
        // Compacted more than one undo record, link it to the version chain.
        LinkCompactedUndoRecord(start_record, &curr, next, compacted_undo_record);
      }
      EndCompaction(&interval_length);
      BeginCompaction(&start_record, curr, next, &interval_length);
      // curr was not claimed in the previous iteration, so possibly someone might use it
      curr = curr->Next();
    }
    next = curr->Next();
  }

  SwapwithSafeAbort(curr, nullptr, table, curr->Slot());
  // active_trans_iter ends but there are still elements in the version chain. Can GC everything below
  while (next != nullptr) {
    // Unlink next
    UnlinkUndoRecordVersion(next);
    // Move next pointer ahead
    next = next->Next();
  }
}

void GarbageCollector::UnlinkUndoRecordVersion(UndoRecord *const undo_record) {
  DataTable *&table = undo_record->Table();
  TERRIER_ASSERT(table != nullptr, "Table should not be NULL here");
  ReclaimSlotIfDeleted(undo_record);

  MarkVarlenReclaimable(undo_record);
  // mark the record as fully processed
  table = nullptr;
}

void GarbageCollector::ReclaimSlotIfDeleted(UndoRecord *undo_record) const {
  if (undo_record->Type() == DeltaRecordType::DELETE) undo_record->Table()->accessor_.Deallocate(undo_record->Slot());
}

void GarbageCollector::BeginCompaction(UndoRecord **start_record_ptr, UndoRecord *curr, UndoRecord *next,
                                       uint32_t *interval_length_ptr) {
  col_set_.clear();
  // Compaction can only be done for a series of Update Undo Records
  if (next->Type() == DeltaRecordType::UPDATE) {
    *start_record_ptr = curr;
    *interval_length_ptr = 1;
    ProcessUndoRecordAttributes(next);
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
  SwapwithSafeAbort(start_record, compacted_undo_record, compacted_undo_record->table_, compacted_undo_record->Slot());
  // Added a compacted undo record. So it should be curr
  *curr_ptr = compacted_undo_record;
}

// Returns true if compaction terminates here because of an Insert record
void GarbageCollector::ReadUndoRecord(UndoRecord *start_record, UndoRecord *next, uint32_t *interval_length_ptr) {
  // Already have a base undo record. Apply this undo record on top of that
  switch (next->Type()) {
    case DeltaRecordType::UPDATE:
      (*interval_length_ptr)++;
      ProcessUndoRecordAttributes(next);
      UnlinkUndoRecordVersion(next);
      break;
    case DeltaRecordType::INSERT:
      // Compacting here. So unlink start_record's next
      UnlinkUndoRecordVersion(start_record->Next());
      EndCompaction(interval_length_ptr);
      // Insert undo record can be GC'd so this tuple is not visible
      // Set start_record to point to Insert's next undo record
      SwapwithSafeAbort(start_record, next, next->table_, next->Slot());
      break;
    case DeltaRecordType::DELETE: {
    }
  }
}

void GarbageCollector::EndCompaction(uint32_t *interval_length_ptr) { *interval_length_ptr = 0; }

void GarbageCollector::ProcessUndoRecordAttributes(UndoRecord *const undo_record) {
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

  UndoRecord *base_undo_record =
      InitializeUndoRecord(first_compacted_record->Timestamp().load(), first_compacted_record->Slot(), table);

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

  std::vector<col_id_t> col_id_list(col_set_.begin(), col_set_.end());
  auto init = terrier::storage::ProjectedRowInitializer(layout, col_id_list);

  uint32_t size = static_cast<uint32_t>(sizeof(UndoRecord)) + init.ProjectedRowSize();
  byte *head = delta_record_compaction_buffer_->NewEntry(size);

  auto *result = reinterpret_cast<UndoRecord *>(head);
  init.InitializeRow(result->varlen_contents_);

  result->type_ = DeltaRecordType ::UPDATE;
  result->next_ = nullptr;
  result->timestamp_.store(timestamp);
  result->table_ = table;
  result->slot_ = slot;
  return result;
}

void GarbageCollector::MarkVarlenReclaimable(UndoRecord *undo_record) {
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
          if (varlen != nullptr && varlen->NeedReclaim()) {
            reclaim_varlen_map_[undo_record].push_front(varlen->Content());
          }
        }
      }
      break;
    case DeltaRecordType::UPDATE:
      // TODO(Tianyu): This might be a really bad idea for large deltas...
      for (uint16_t i = 0; i < undo_record->Delta()->NumColumns(); i++) {
        col_id_t col_id = undo_record->Delta()->ColumnIds()[i];
        if (layout.IsVarlen(col_id)) {
          auto *varlen = reinterpret_cast<VarlenEntry *>(undo_record->Delta()->AccessWithNullCheck(i));
          if (varlen != nullptr && varlen->NeedReclaim()) {
            reclaim_varlen_map_[undo_record].push_front(varlen->Content());
          }
        }
      }
  }
}

void GarbageCollector::CopyVarlen(UndoRecord *undo_record) {
  const TupleAccessStrategy &accessor = undo_record->Table()->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();
  for (uint16_t i = 0; i < undo_record->Delta()->NumColumns(); i++) {
    col_id_t col_id = undo_record->Delta()->ColumnIds()[i];

    if (layout.IsVarlen(col_id)) {
      auto *varlen = reinterpret_cast<VarlenEntry *>(undo_record->Delta()->AccessWithNullCheck(i));
      if (varlen != nullptr && varlen->NeedReclaim()) {
        // Copy the varlen entry from the original undo record to the compacted undo record
        uint32_t size = varlen->Size();
        byte *buffer = common::AllocationUtil::AllocateAligned(size);
        *reinterpret_cast<storage::VarlenEntry *>(undo_record->Delta()->AccessForceNotNull(i)) =
            storage::VarlenEntry::Create(buffer, size, true);
      }
    }
  }
}

void GarbageCollector::DeallocateVarlen(UndoBuffer *undo_buffer) {
  for (auto &undo_record : *undo_buffer) {
    for (const byte *ptr : reclaim_varlen_map_[&undo_record]) {
      delete[] ptr;
    }
  }
}

void GarbageCollector::SwapwithSafeAbort(UndoRecord *curr, UndoRecord *to_link, DataTable *table, TupleSlot slot) {
  UndoRecord *version_ptr = table->AtomicallyReadVersionPtr(slot, table->accessor_);
  if (curr == version_ptr) {
    curr->Next().store(to_link);
    while (curr == version_ptr && !transaction::TransactionUtil::Committed(version_ptr->Timestamp().load()) &&
           table->AtomicallyReadVersionPtr(slot, table->accessor_) != version_ptr) {
      table->AtomicallyWriteVersionPtr(slot, table->accessor_, to_link);
      version_ptr = table->AtomicallyReadVersionPtr(slot, table->accessor_);
    }
  } else {
    curr->Next().store(to_link);
  }
}

}  // namespace terrier::storage
