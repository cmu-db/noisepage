#include "storage/data_table.h"
#include <unordered_map>
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {
DataTable::DataTable(BlockStore *const store,
                     const BlockLayout &layout,
                     layout_version_t layout_version)
    : block_store_(store), layout_version_(layout_version), accessor_(layout) {
  TERRIER_ASSERT(layout.AttrSize(VERSION_POINTER_COLUMN_ID) == 8,
                 "First column must have size 8 for the version chain.");
  TERRIER_ASSERT(layout.NumCols() > 1, "First column is reserved for version info.");
}

void DataTable::Select(transaction::TransactionContext *const txn, const TupleSlot slot,
                       ProjectedRow *const out_buffer) const {
  TERRIER_ASSERT(out_buffer->NumColumns() < accessor_.GetBlockLayout().NumCols(),
                 "The output buffer never returns the version pointer, so it should have fewer attributes.");
  TERRIER_ASSERT(out_buffer->NumColumns() > 0, "The output buffer should return at least one attribute.");

  UndoRecord *version_ptr;
  do {
    version_ptr = AtomicallyReadVersionPtr(slot, accessor_);
    // Copy the current (most recent) tuple into the output buffer. These operations don't need to be atomic,
    // because so long as we set the version ptr before updating in place, the reader will know if a conflict
    // can potentially happen, and chase the version chain before returning anyway,
    for (uint16_t i = 0; i < out_buffer->NumColumns(); i++)
      StorageUtil::CopyAttrIntoProjection(accessor_, slot, out_buffer, i);
    // Here we will need to check that the version pointer did not change during our read. If it did, the content
    // we have read might have been rolled back and an abort has already unlinked the associated undo-record,
    // we will have to loop around to avoid a dirty read.
  } while (version_ptr != AtomicallyReadVersionPtr(slot, accessor_));

  // Nullptr in version chain means no version visible to any transaction alive at this point.
  // Alternatively, if the current transaction holds the write lock, it should be able to read its own updates.
  if (version_ptr == nullptr || version_ptr->Timestamp().load() == txn->TxnId().load()) return;

  // Apply deltas until we reconstruct a version safe for us to read
  // If the version chain becomes null, this tuple does not exist for this version, and the last delta
  // record would be an undo for insert that sets the primary key to null, which is intended behavior.
  while (version_ptr != nullptr &&
      transaction::TransactionUtil::NewerThan(version_ptr->Timestamp().load(), txn->StartTime())) {
    StorageUtil::ApplyDelta(accessor_.GetBlockLayout(), *(version_ptr->Delta()), out_buffer);
    version_ptr = version_ptr->Next();
  }
}

bool DataTable::Update(transaction::TransactionContext *const txn, const TupleSlot slot, const ProjectedRow &redo) {
  // We never bother deallocating this entry, which is why we need to remember to check on abort
  // whether the transaction actually holds a write lock
  UndoRecord *const undo = txn->UndoRecordForUpdate(this, slot, redo);
  UndoRecord *const version_ptr = AtomicallyReadVersionPtr(slot, accessor_);
  // Since we disallow write-write conflicts, the version vector pointer is essentially an implicit
  // write lock on the tuple.
  if (HasConflict(version_ptr, txn)) return false;

  // Update the next pointer of the new head of the version chain
  undo->Next() = version_ptr;

  // Store before-image before making any changes or grabbing lock
  for (uint16_t i = 0; i < undo->Delta()->NumColumns(); i++)
    StorageUtil::CopyAttrIntoProjection(accessor_, slot, undo->Delta(), i);
  // At this point, either tuple write lock is ownable, or the current transaction already owns this slot.
  if (!CompareAndSwapVersionPtr(slot, accessor_, version_ptr, undo)) return false;
  // Update in place with the new value.
  for (uint16_t i = 0; i < redo.NumColumns(); i++) StorageUtil::CopyAttrFromProjection(accessor_, slot, redo, i);

  return true;
}

TupleSlot DataTable::Insert(transaction::TransactionContext *const txn, const ProjectedRow &redo) {
  // Attempt to allocate a new tuple from the block we are working on right now.
  // If that block is full, try to request a new block. Because other concurrent
  // inserts could have already created a new block, we need to use compare and swap
  // to change the insertion head. We do not expect this loop to be executed more than
  // twice, but there is technically a possibility for blocks with only a few slots.
  TupleSlot result;
  while (true) {
    RawBlock *block = insertion_head_.load();
    if (block != nullptr && accessor_.Allocate(block, &result)) break;
    NewBlock(block);
  }
  // At this point, sequential scan down the block can still see this, except it thinks it is logically deleted if we 0
  // the primary key column
  UndoRecord *undo = txn->UndoRecordForInsert(this, result, insert_record_initializer_);

  // Populate undo record with the before image of presence column
  undo->Delta()->SetNull(0);

  // Update the version pointer atomically so that a sequential scan will not see inconsistent version pointer, which
  // may result in a segfault
  AtomicallyWriteVersionPtr(result, accessor_, undo);

  // At this point, a sequential scan can see this tuple, but will follow the version chain to see a logically deleted
  // version

  // Update in place with the new value.
  for (uint16_t i = 0; i < redo.NumColumns(); i++) StorageUtil::CopyAttrFromProjection(accessor_, result, redo, i);

  return result;
}

UndoRecord *DataTable::AtomicallyReadVersionPtr(const TupleSlot slot, const TupleAccessStrategy &accessor) const {
  byte *ptr_location = accessor.AccessWithoutNullCheck(slot, VERSION_POINTER_COLUMN_ID);
  return reinterpret_cast<std::atomic<UndoRecord *> *>(ptr_location)->load();
}

void DataTable::AtomicallyWriteVersionPtr(const TupleSlot slot, const TupleAccessStrategy &accessor,
                                          UndoRecord *const desired) {
  byte *ptr_location = accessor.AccessWithoutNullCheck(slot, VERSION_POINTER_COLUMN_ID);
  reinterpret_cast<std::atomic<UndoRecord *> *>(ptr_location)->store(desired);
}

bool DataTable::HasConflict(UndoRecord *const version_ptr, transaction::TransactionContext *const txn) const {
  if (version_ptr == nullptr) return false;  // Nobody owns this tuple's write lock, no older version visible
  const timestamp_t version_timestamp = version_ptr->Timestamp().load();
  const timestamp_t txn_id = txn->TxnId().load();
  const timestamp_t start_time = txn->StartTime();
  return (!transaction::TransactionUtil::Committed(version_timestamp) && version_timestamp != txn_id)
      // Someone else owns this tuple, write-write-conflict
      || (transaction::TransactionUtil::Committed(version_timestamp) &&
          transaction::TransactionUtil::NewerThan(version_timestamp, start_time));
  // Someone else already committed an update to this tuple while we were running, we can't update this under SI
}

bool DataTable::CompareAndSwapVersionPtr(const TupleSlot slot, const TupleAccessStrategy &accessor,
                                         UndoRecord *expected, UndoRecord *const desired) {
  byte *ptr_location = accessor.AccessWithoutNullCheck(slot, VERSION_POINTER_COLUMN_ID);
  return reinterpret_cast<std::atomic<UndoRecord *> *>(ptr_location)->compare_exchange_strong(expected, desired);
}

void DataTable::NewBlock(RawBlock *expected_val) {
  common::SpinLatch::ScopedSpinLatch guard(&blocks_latch_);
  // Want to stop early if another thread is already getting a new block
  if (expected_val != insertion_head_) return;
  RawBlock *new_block = block_store_->Get();
  accessor_.InitializeRawBlock(new_block, layout_version_);
  blocks_.push_back(new_block);
  insertion_head_ = new_block;
}
}  // namespace terrier::storage
