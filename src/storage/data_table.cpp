#include "storage/data_table.h"
#include <unordered_map>
#include "common/allocator.h"
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {
DataTable::DataTable(BlockStore *const store, const BlockLayout &layout, const layout_version_t layout_version)
    : block_store_(store), layout_version_(layout_version), accessor_(layout) {
  TERRIER_ASSERT(layout.AttrSize(VERSION_POINTER_COLUMN_ID) == 8,
                 "First column must have size 8 for the version chain.");
  TERRIER_ASSERT(layout.NumColumns() > NUM_RESERVED_COLUMNS,
                 "First column is reserved for version info, second column is reserved for logical delete.");
}

bool DataTable::Select(terrier::transaction::TransactionContext *txn, terrier::storage::TupleSlot slot,
                       terrier::storage::ProjectedRow *out_buffer) const {
  data_table_counter_.IncrementNumSelect(1);
  return SelectIntoBuffer(txn, slot, out_buffer);
}

void DataTable::Scan(transaction::TransactionContext *const txn, SlotIterator *start_pos,
                     ProjectedColumns *out_buffer) const {
  // TODO(Tianyu): So far this is not that much better than tuple-at-a-time access,
  // but can be improved if block is read-only, or if we implement version synopsis, to just use memcpy when it's safe
  for (uint32_t &i = out_buffer->NumTuples() = 0; i < out_buffer->MaxTuples() && *start_pos != end();
       ++i, ++(*start_pos)) {
    bool valid;
    ProjectedColumns::RowView row = out_buffer->InterpretAsRow(accessor_.GetBlockLayout(), i);
    do {
      TupleSlot slot = **start_pos;
      // Only fill the buffer with valid, visible tuples
      valid = !accessor_.ValidSlot(slot) && SelectIntoBuffer(txn, slot, &row);
    } while (!valid);
  }
}

bool DataTable::Update(transaction::TransactionContext *const txn, const TupleSlot slot, const ProjectedRow &redo) {
  TERRIER_ASSERT(redo.NumColumns() <= accessor_.GetBlockLayout().NumColumns() - NUM_RESERVED_COLUMNS,
                 "The input buffer never changes both the version pointer and logical delete columns, so it should "
                 "have fewer attributes.");
  TERRIER_ASSERT(redo.NumColumns() > 0, "The input buffer should modify at least one attribute.");
  UndoRecord *undo = txn->UndoRecordForUpdate(this, slot, redo);
  UndoRecord *const version_ptr = AtomicallyReadVersionPtr(slot, accessor_);

  // Since we disallow write-write conflicts, the version vector pointer is essentially an implicit
  // write lock on the tuple.
  if (HasConflict(version_ptr, txn) || !Visible(slot, accessor_)) {
    // Mark this UndoRecord as never installed by setting the table pointer to nullptr. This is inspected in the
    // TransactionManager's Rollback() and GC's Unlink logic
    undo->Table() = nullptr;
    return false;
  }

  // Store before-image before making any changes or grabbing lock
  for (uint16_t i = 0; i < undo->Delta()->NumColumns(); i++)
    StorageUtil::CopyAttrIntoProjection(accessor_, slot, undo->Delta(), i);
  // Update the next pointer of the new head of the version chain
  undo->Next() = version_ptr;

  if (!CompareAndSwapVersionPtr(slot, accessor_, version_ptr, undo)) {
    // Mark this UndoRecord as never installed by setting the table pointer to nullptr. This is inspected in the
    // TransactionManager's Rollback() and GC's Unlink logic
    undo->Table() = nullptr;
    return false;
  }

  // Update in place with the new value.
  for (uint16_t i = 0; i < redo.NumColumns(); i++) {
    TERRIER_ASSERT(redo.ColumnIds()[i] != VERSION_POINTER_COLUMN_ID,
                   "Input buffer should not change the version pointer column.");
    // TODO(Matt): It would be nice to check that a ProjectedRow that modifies the logical delete column only originated
    // from the DataTable calling Update() within Delete(), rather than an outside soure modifying this column, but
    // that's difficult with this implementation
    StorageUtil::CopyAttrFromProjection(accessor_, slot, redo, i);
  }
  data_table_counter_.IncrementNumUpdate(1);
  return true;
}

TupleSlot DataTable::Insert(transaction::TransactionContext *const txn, const ProjectedRow &redo) {
  TERRIER_ASSERT(redo.NumColumns() == accessor_.GetBlockLayout().NumColumns() - NUM_RESERVED_COLUMNS,
                 "The input buffer never changes the version pointer or logical delete columns, so it should have "
                 "exactly 2 fewer attributes than the DataTable's layout.");

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
  UndoRecord *undo = txn->UndoRecordForInsert(this, result);

  // Update the version pointer atomically so that a sequential scan will not see inconsistent version pointer, which
  // may result in a segfault
  AtomicallyWriteVersionPtr(result, accessor_, undo);

  // Set the logically deleted bit to present as the undo record is ready
  accessor_.AccessForceNotNull(result, VERSION_POINTER_COLUMN_ID);

  // Update in place with the new value.
  for (uint16_t i = 0; i < redo.NumColumns(); i++) {
    TERRIER_ASSERT(redo.ColumnIds()[i] != VERSION_POINTER_COLUMN_ID,
                   "Insert buffer should not change the version pointer column.");
    StorageUtil::CopyAttrFromProjection(accessor_, result, redo, i);
  }

  data_table_counter_.IncrementNumInsert(1);
  return result;
}

bool DataTable::Delete(transaction::TransactionContext *const txn, const TupleSlot slot) {
  // This will mean Deletes get counted as a Delete and an Update in stats
  data_table_counter_.IncrementNumDelete(1);
  // Create a redo
  txn->StageDelete(this, slot);
  UndoRecord *const undo = txn->UndoRecordForDelete(this, slot);
  UndoRecord *const version_ptr = AtomicallyReadVersionPtr(slot, accessor_);
  // Since we disallow write-write conflicts, the version vector pointer is essentially an implicit
  // write lock on the tuple.
  if (HasConflict(version_ptr, txn) || !Visible(slot, accessor_)) {
    // Mark this UndoRecord as never installed by setting the table pointer to nullptr. This is inspected in the
    // TransactionManager's Rollback() and GC's Unlink logic
    undo->Table() = nullptr;
    return false;
  }

  // Update the next pointer of the new head of the version chain
  undo->Next() = version_ptr;

  if (!CompareAndSwapVersionPtr(slot, accessor_, version_ptr, undo)) {
    // Mark this UndoRecord as never installed by setting the table pointer to nullptr. This is inspected in the
    // TransactionManager's Rollback() and GC's Unlink logic
    undo->Table() = nullptr;
    return false;
  }
  // We have the write lock. Go ahead and flip the logically deleted bit to true
  accessor_.SetNull(slot, VERSION_POINTER_COLUMN_ID);
  return true;
}

template <class RowType>
bool DataTable::SelectIntoBuffer(transaction::TransactionContext *const txn, const TupleSlot slot,
                                 RowType *const out_buffer) const {
  TERRIER_ASSERT(accessor_.ValidSlot(slot), "Must select a tuple slot that is claimed by a tuple");
  TERRIER_ASSERT(out_buffer->NumColumns() <= accessor_.GetBlockLayout().NumColumns() - 1,
                 "The output buffer never returns the version pointer columns, so it should have "
                 "fewer attributes.");
  TERRIER_ASSERT(out_buffer->NumColumns() > 0, "The output buffer should return at least one attribute.");

  UndoRecord *version_ptr;
  bool visible;
  do {
    version_ptr = AtomicallyReadVersionPtr(slot, accessor_);
    // Copy the current (most recent) tuple into the output buffer. These operations don't need to be atomic,
    // because so long as we set the version ptr before updating in place, the reader will know if a conflict
    // can potentially happen, and chase the version chain before returning anyway,
    for (uint16_t i = 0; i < out_buffer->NumColumns(); i++) {
      TERRIER_ASSERT(out_buffer->ColumnIds()[i] != VERSION_POINTER_COLUMN_ID,
                     "Output buffer should not read the version pointer column.");
      StorageUtil::CopyAttrIntoProjection(accessor_, slot, out_buffer, i);
    }
    // Here we will need to check that the version pointer did not change during our read. If it did, the content
    // we have read might have been rolled back and an abort has already unlinked the associated undo-record,
    // we will have to loop around to avoid a dirty read.
    // TODO(Matt): might not need to read visible in the loop (move after?) but not confident without large random tests
    visible = Visible(slot, accessor_);
  } while (version_ptr != AtomicallyReadVersionPtr(slot, accessor_));

  // Nullptr in version chain means no version visible to any transaction alive at this point.
  // Alternatively, if the current transaction holds the write lock, it should be able to read its own updates.
  if (version_ptr == nullptr || version_ptr->Timestamp().load() == txn->TxnId().load()) {
    return visible;
  }

  // Apply deltas until we reconstruct a version safe for us to read
  // If the version chain becomes null, this tuple does not exist for this version, and the last delta
  // record would be an undo for insert that sets the primary key to null, which is intended behavior.
  while (version_ptr != nullptr &&
         transaction::TransactionUtil::NewerThan(version_ptr->Timestamp().load(), txn->StartTime())) {
    // TODO(Matt): It's possible that if we make some guarantees about where in the version chain INSERTs (last position
    // in version chain) and DELETEs (first position in version chain) can appear that we can optimize this check
    switch (version_ptr->Type()) {
      case DeltaRecordType::UPDATE:
        // Normal delta to be applied. Does not modify the logical delete column.
        StorageUtil::ApplyDelta(accessor_.GetBlockLayout(), *(version_ptr->Delta()), out_buffer);
        break;
      case DeltaRecordType::INSERT:
        visible = false;
        break;
      case DeltaRecordType::DELETE:
        visible = true;
    }
    // TODO(Matt): This logic might need revisiting if we start recycling slots and a chain can have a delete later in
    // the chain than an insert.
    version_ptr = version_ptr->Next();
  }

  return visible;
}

template bool DataTable::SelectIntoBuffer<ProjectedRow>(transaction::TransactionContext *txn, const TupleSlot slot,
                                                        ProjectedRow *out_buffer) const;
template bool DataTable::SelectIntoBuffer<ProjectedColumns::RowView>(transaction::TransactionContext *txn,
                                                                     const TupleSlot slot,
                                                                     ProjectedColumns::RowView *out_buffer) const;

UndoRecord *DataTable::AtomicallyReadVersionPtr(const TupleSlot slot, const TupleAccessStrategy &accessor) const {
  // Okay to ignore presence bit, because we use that for logical delete, not for validity of the version pointer value
  byte *ptr_location = accessor.AccessWithoutNullCheck(slot, VERSION_POINTER_COLUMN_ID);
  return reinterpret_cast<std::atomic<UndoRecord *> *>(ptr_location)->load();
}

void DataTable::AtomicallyWriteVersionPtr(const TupleSlot slot, const TupleAccessStrategy &accessor,
                                          UndoRecord *const desired) {
  // Okay to ignore presence bit, because we use that for logical delete, not for validity of the version pointer value
  byte *ptr_location = accessor.AccessWithoutNullCheck(slot, VERSION_POINTER_COLUMN_ID);
  reinterpret_cast<std::atomic<UndoRecord *> *>(ptr_location)->store(desired);
}

bool DataTable::Visible(const TupleSlot slot, const TupleAccessStrategy &accessor) const {
  const bool present = accessor.ValidSlot(slot);
  const bool not_deleted = !accessor.IsNull(slot, VERSION_POINTER_COLUMN_ID);
  return present && not_deleted;
}

bool DataTable::HasConflict(UndoRecord *const version_ptr, const transaction::TransactionContext *const txn) const {
  if (version_ptr == nullptr) return false;  // Nobody owns this tuple's write lock, no older version visible
  const timestamp_t version_timestamp = version_ptr->Timestamp().load();
  const timestamp_t txn_id = txn->TxnId().load();
  const timestamp_t start_time = txn->StartTime();
  const bool owned_by_other_txn =
      (!transaction::TransactionUtil::Committed(version_timestamp) && version_timestamp != txn_id);
  const bool newer_committed_version = transaction::TransactionUtil::Committed(version_timestamp) &&
                                       transaction::TransactionUtil::NewerThan(version_timestamp, start_time);
  return owned_by_other_txn || newer_committed_version;
}

bool DataTable::CompareAndSwapVersionPtr(const TupleSlot slot, const TupleAccessStrategy &accessor,
                                         UndoRecord *expected, UndoRecord *const desired) {
  // Okay to ignore presence bit, because we use that for logical delete, not for validity of the version pointer value
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
  data_table_counter_.IncrementNumNewBlock(1);
}
}  // namespace terrier::storage
