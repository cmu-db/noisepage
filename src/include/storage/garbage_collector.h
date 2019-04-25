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
   * Given the compacted undo record, duplicate all the varlens associated with it
   * @param undo_record the compacted undo record
   */
  void CopyVarlen(UndoRecord *undo_record);

  /**
   * Given the start and the end of the compaction interval after first GC pass, do a second pass and create the
   * compacted undo record
   * @param start_record the undo record which marks the beginning of compacted interval
   * @param end_record the undo record which marks the end of compacted interval
   * @return the compacted undo record
   */
  UndoRecord *CreateUndoRecord(UndoRecord *start_record, UndoRecord *end_record);

  /**
   * Marks the end of two pass Interval GC
   * @param interval_length_ptr the length of the compaction interval
   */
  void EndCompaction(uint32_t *interval_length_ptr);

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
   * Process the deallocate queue
   * @return number of txns (not UndoRecords) processed for debugging/testing
   */
  uint32_t ProcessDeallocateQueue();

  /*
   * Process deferred actions
   */
  void ProcessDeferredActions();

  /**
   * Given a version chain, perform interval gc on all versions except the head of the chain
   * @param table data table
   * @param slot tuple slot
   * @param active_txns vector containing all active transactions
   */
  void ProcessTupleVersionChain(DataTable *table, TupleSlot slot, std::vector<transaction::timestamp_t> *active_txns);

  /**
   * Given the data table and the tuple slot, try unlinking the version chain head for that tuple slot
   * @param table data table
   * @param slot tuple slot
   * @param active_txns list of currently running active transactions
   */
  void ProcessTupleVersionChainHead(DataTable *table, TupleSlot slot,
                                    std::vector<transaction::timestamp_t> *active_txns);

  /**
   * Given a UndoRecord that has been deemed safe to unlink by the GC, attempts to remove it from the version chain.
   * It's possible that this process will fail because the GC is conservative with conflicts. If the UndoRecord in the
   * version chain to be updated in order to unlink the target UndoRecord is not yet committed, we will fail and
   * expect this txn to be requeued and we'll try again on the next GC invocation, hopefully after the conflicting txn
   * is either committed or aborted.
   * @param undo_record UndoRecord to be unlinked
   * @param active_txns list of timestamps of running transactions
   * @param visited_slots Tuple slots that have already been garbage collected in this run
   * @return true, if the undo record was unlinked
   */
  bool ProcessUndoRecord(UndoRecord *undo_record, std::vector<transaction::timestamp_t> *active_txns,
                         std::unordered_set<TupleSlot> *visited_slots);

  /**
   * Process the unlink queue
   * @return number of txns (not UndoRecords) processed for debugging/testing
   */
  uint32_t ProcessUnlinkQueue();

  /**
   * Given the undo record to be compacted in the first pass of GC, find all the columns contained in the delta
   * @param undo_record the undo record to be compacted in the first pass of GC
   */
  void ProcessUpdateUndoRecordAttributes(UndoRecord *undo_record);

  /**
   * Given the undo record to be compacted in the first GC pass, process it
   * @param start_record the undo record which marks the beginning of compacted interval and
   * which will point to the compacted record
   * @param next the undo record to be compacted
   * @param interval_length_ptr the length of the compaction interval
   */
  void ReadUndoRecord(UndoRecord *start_record, UndoRecord *next, uint32_t *interval_length_ptr);

  /**
   * Given the undo record, mark all the varlen entries contained in the regular undo record to be deallocated later
   * by pushing them in the corresponding transaction's loose pointers
   * @param undo_record the undo buffer whose varlen entries are to deallocated
   */
  void ReclaimBufferIfVarlen(UndoRecord *undo_record) const;

  /**
   * Given the undo buffer, deallocate all the varlen entries contained in compacted undo record
   * @param undo_record the undo buffer whose varlen entries are to deallocated
   */
  void ReclaimBufferIfVarlenCompacted(UndoRecord *undo_record) const;

  /**
   * Delete the slot corresponding to the unlinked undo record if the undo record was a DELETE
   * @param undo_record the unlinked undo record
   */
  void ReclaimSlotIfDeleted(UndoRecord *undo_record) const;

  /**
   * Deallocate varlen entries or mark them to be deleted later from the unlinked undo record
   * @param undo_record the unlinked undo record
   */
  void ReclaimVarlen(UndoRecord *undo_record) const;

  /**
   * Straight up unlink the undo_record and reclaim its space
   * @param undo_record
   */
  void UnlinkUndoRecordVersion(UndoRecord *undo_record);

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
  // Variable to mark that undo buffer to hold compacted undo records is empty so that it can be deallocated without
  // unlinking
  bool compaction_buffer_empty;
  // queue of undo buffers containing compacted undo records which are pending unlinking
  std::forward_list<storage::UndoBuffer *> buffers_to_unlink_;
  // queue of undo buffers containing compacted undo records which have been unlinked and are pending deallocation
  std::forward_list<storage::UndoBuffer *> buffers_to_deallocate_;
  // set of all column ids in the compacted interval
  std::unordered_set<col_id_t> col_set_;
  // queue of unexecuted deferred actions
  std::queue<std::pair<transaction::timestamp_t, transaction::Action>> deferred_actions_;
};

}  // namespace terrier::storage
