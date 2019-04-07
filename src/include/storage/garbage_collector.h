#pragma once

#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "storage/record_buffer.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage {

/**
 * The garbage collector is responsible for processing a queue of completed transactions from the transaction manager.
 * Based on the contents of this queue, it unlinks the UndoRecords from their version chains when no running
 * transactions can view those versions anymore. It then stores those transactions to attempt to deallocate on the next
 * iteration if no running transactions can still hold references to them.
 */
class GarbageCollector {
 public:
  /**
   * Constructor for the Garbage Collector that requires a pointer to the TransactionManager. This is necessary for the
   * GC to invoke the TM's function for handing off the completed transactions queue.
   * @param txn_manager pointer to the TransactionManager
   */
  explicit GarbageCollector(transaction::TransactionManager *txn_manager)
      : txn_manager_(txn_manager), last_unlinked_{0} {
    TERRIER_ASSERT(txn_manager_->GCEnabled(),
                   "The TransactionManager needs to be instantiated with gc_enabled true for GC to work!");
    delta_record_compaction_buffer_ = nullptr;
  }

  /**
   * Deallocates transactions that can no longer be referenced by running transactions, and unlinks UndoRecords that
   * are no longer visible to running transactions. This needs to be invoked twice to actually free memory, since the
   * first invocation will unlink a transaction's UndoRecords, while the second time around will allow the GC to free
   * the transaction if safe to do so. The only exception is read-only transactions, which can be deallocated in a
   * single GC pass.
   * @return A pair of numbers: the first is the number of transactions deallocated (deleted) on this iteration, while
   * the second is the number of transactions unlinked on this iteration.
   */
  std::pair<uint32_t, uint32_t> PerformGarbageCollection();

 private:
  /**
   * Process the deallocate queue
   * @return number of txns (not UndoRecords) processed for debugging/testing
   */
  uint32_t ProcessDeallocateQueue();

  /**
   * Process the unlink queue
   * @return number of txns (not UndoRecords) processed for debugging/testing
   */
  uint32_t ProcessUnlinkQueue();

  bool ProcessUndoRecord(UndoRecord *undo_record, std::vector<transaction::timestamp_t> *active_txns);

  void ReclaimSlotIfDeleted(UndoRecord *undo_record) const;

  /**
   * Given a UndoRecord that has been deemed safe to unlink by the GC, attempts to remove it from the version chain.
   * It's possible that this process will fail because the GC is conservative with conflicts. If the UndoRecord in the
   * version chain to be updated in order to unlink the target UndoRecord is not yet committed, we will fail and
   * expect this txn to be requeued and we'll try again on the next GC invocation, hopefully after the conflicting txn
   * is either committed or aborted.
   * @param undo_record UndoRecord to be unlinked
   */
  void UnlinkUndoRecord(UndoRecord *undo_record, std::vector<transaction::timestamp_t> *active_txns);

  /**
   * Given a version chain, perform interval gc on all versions except the head of the chain
   * @param version_chain_head pointer to the head of the chain
   * @param active_txns vector containing all active transactions
   */
  void UnlinkUndoRecordRestOfChain(UndoRecord *version_chain_head, std::vector<transaction::timestamp_t> *active_txns);

  /**
   * Straight up unlink the undo_record and reclaim its space
   * @param txn
   * @param undo_record
   */
  void UnlinkUndoRecordVersion(UndoRecord *undo_record);

  /**
   * Given the first undo record to be compacted, set up variables and begin first phase of interval GC
   * @param start_record_ptr Stores the beginning of the interval. Required to find the start of interval for the second
   * pass
   * @param curr the undo record which will point to the compacted undo record
   * @param next first undo record to be compacted
   * @param interval_length_ptr the length of the compaction interval
   */
  void BeginCompaction(UndoRecord **start_record_ptr, UndoRecord *curr, UndoRecord *next,
                       uint32_t *interval_length_ptr);

  /**
   * Given the compacted undo record to be linked to the version chain, link it
   * @param start_record the undo record which marks the beginning of compacted interval and
   * which will point to the compacted record
   * @param curr_ptr the pointer marking the current position of interval GC in the version chain
   * @param end_record the undo record which marks the end of compacted interval and compacted undo record will point
   * to this record
   * @param compacted_undo_record the compacted undo record to be linked to the version chain
   */
  void LinkCompactedUndoRecord(UndoRecord *start_record, UndoRecord **curr_ptr, UndoRecord *end_record,
                               UndoRecord *compacted_undo_record);

  /**
   * Given the undo record to be compacted in the first GC pass, process it
   * @param start_record the undo record which marks the beginning of compacted interval and
   * which will point to the compacted record
   * @param next the undo record to be compacted
   * @param interval_length_ptr the length of the compaction interval
   */
  void ReadUndoRecord(UndoRecord *start_record, UndoRecord *next, uint32_t *interval_length_ptr);

  /**
   * Marks the end of two pass Interval GC
   * @param interval_length_ptr the length of the compaction interval
   */
  void EndCompaction(uint32_t *interval_length_ptr);

  /**
   * Given the undo record to be compacted in the first pass of GC, find all the columns contained in the delta
   * @param undo_record the undo record to be compacted in the first pass of GC
   */
  void ProcessUndoRecordAttributes(UndoRecord *undo_record);

  /**
   * Given the start and the end of the compaction interval after first GC pass, do a second pass and create the
   * compacted undo record
   * @param start_record the undo record which marks the beginning of compacted interval
   * @param end_record the undo record which marks the end of compacted interval
   * @return the compacted undo record
   */
  UndoRecord *CreateUndoRecord(UndoRecord *start_record, UndoRecord *end_record);

  /**
   * Given the time stamp and table slot after first GC pass, create a undo record which can accommodate all the columns
   * found in the combined delta in the first GC pass
   * @param timestamp the timestamp which will be allotted to the compacted undo record
   * @param slot the tuple slot corresponding to the tuple associated with the compacted undo record
   * @param table the table corresponding to the tuple associated with the compacted undo record
   * @return the blank undo record which is the placeholder for the compacted undo record
   */
  UndoRecord *InitializeUndoRecord(transaction::timestamp_t timestamp, TupleSlot slot, DataTable *table);

  /**
   * Given the undo record,  mark all the varlen entries in the delta to be available for deallocation later
   * @param undo_record the undo record whose varlen entries are to be marked for deallocation
   */
  void MarkVarlenReclaimable(UndoRecord *undo_record);

  /**
   * Given the undo buffer, deallocate all the varlen entries contained in all the undo records in the undo buffer
   * @param undo_buffer the undo buffer whose varlen entries are to deallocated
   */
  void DeallocateVarlen(UndoBuffer *undo_buffer);

  /**
   * Given the compacted undo record, duplicate all the varlens associated with it
   * @param undo_record the compacted undo record
   */
  void CopyVarlen(UndoRecord *undo_record);

  /**
   * Given the undo record to be linked to the version chain, safely link it to the given undo record
   * @param curr the undo record which will point to the given undo record
   * @param to_link the undo to be linked to the version chain
   * @param slot the tuple slot corresponding to the tuple associated with the undo record
   * @param table the table corresponding to the tuple associated with the undo record
   */
  void SwapwithSafeAbort(UndoRecord *curr, UndoRecord *to_link, DataTable *table, TupleSlot slot);

  // reference to the transaction manager class object
  transaction::TransactionManager *txn_manager_;
  // timestamp of the last time GC unlinked anything. We need this to know when unlinked versions are safe to deallocate
  transaction::timestamp_t last_unlinked_;
  // queue of txns that have been unlinked, and should possible be deleted on next GC run
  transaction::TransactionQueue txns_to_deallocate_;
  // queue of txns that need to be unlinked
  transaction::TransactionQueue txns_to_unlink_;
  // Undo buffer to hold compacted undo records
  storage::UndoBuffer *delta_record_compaction_buffer_;
  // queue of undo buffers containing compacted undo records which are pending unlinking
  std::forward_list<storage::UndoBuffer *> buffers_to_unlink_;
  // queue of undo buffers containing compacted undo records which ahve been unlinked abd are pending deallocation
  std::forward_list<storage::UndoBuffer *> buffers_to_deallocate_;
  // set of all column ids in the compacted interval
  std::unordered_set<col_id_t> col_set_;
  // list of varlen entries per undo record which need to be reclaimed
  std::unordered_map<storage::UndoRecord *, std::forward_list<const byte *> > reclaim_varlen_map_;
  // set of tuple slots which have already been visited in this GC run
  std::unordered_set<TupleSlot> visited_slots_;
};

}  // namespace terrier::storage
