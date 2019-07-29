#include "storage/data_table.h"
#include <pthread.h>
#include <cstring>
#include <unordered_map>
#include "common/allocator.h"
#include "storage/block_access_controller.h"
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

DataTable::~DataTable() {
  common::SpinLatch::ScopedSpinLatch guard(&blocks_latch_);
  for (RawBlock *block : blocks_) {
    StorageUtil::DeallocateVarlens(block, accessor_);
    for (col_id_t i : accessor_.GetBlockLayout().Varlens())
      accessor_.GetArrowBlockMetadata(block).GetColumnInfo(accessor_.GetBlockLayout(), i).Deallocate();
    block_store_->Release(block);
  }
}

bool DataTable::Select(terrier::transaction::TransactionContext *txn, terrier::storage::TupleSlot slot,
                       terrier::storage::ProjectedRow *out_buffer) const {
  data_table_counter_.IncrementNumSelect(1);
  return SelectIntoBuffer(txn, slot, out_buffer);
}

void DataTable::Scan(transaction::TransactionContext *const txn, SlotIterator *const start_pos,
                     ProjectedColumns *const out_buffer) const {
  // TODO(Tianyu): So far this is not that much better than tuple-at-a-time access,
  // but can be improved if block is read-only, or if we implement version synopsis, to just use std::memcpy when it's
  // safe
  uint32_t filled = 0;
  while (filled < out_buffer->MaxTuples() && *start_pos != end()) {
    ProjectedColumns::RowView row = out_buffer->InterpretAsRow(filled);
    const TupleSlot slot = **start_pos;
    // Only fill the buffer with valid, visible tuples
    if (SelectIntoBuffer(txn, slot, &row)) {
      out_buffer->TupleSlots()[filled] = slot;
      filled++;
    }
    ++(*start_pos);
  }
  out_buffer->SetNumTuples(filled);
}

DataTable::SlotIterator &DataTable::SlotIterator::operator++() {
  common::SpinLatch::ScopedSpinLatch guard(&table_->blocks_latch_);
  // Jump to the next block if already the last slot in the block.
  if (current_slot_.GetOffset() == table_->accessor_.GetBlockLayout().NumSlots() - 1) {
    ++block_;
    // Cannot dereference if the next block is end(), so just use nullptr to denote
    current_slot_ = {block_ == table_->blocks_.end() ? nullptr : *block_, 0};
  } else {
    current_slot_ = {*block_, current_slot_.GetOffset() + 1};
  }
  return *this;
}

DataTable::SlotIterator DataTable::end() const {
  common::SpinLatch::ScopedSpinLatch guard(&blocks_latch_);
  // TODO(Tianyu): Need to look in detail at how this interacts with compaction when that gets in.

  // The end iterator could either point to an unfilled slot in a block, or point to nothing if every block in the
  // table is full. In the case that it points to nothing, we will use the end-iterator of the blocks list and
  // 0 to denote that this is the case. This solution makes increment logic simple and natural.
  if (blocks_.empty()) return {this, blocks_.end(), 0};
  auto last_block = --blocks_.end();
  uint32_t insert_head = (*last_block)->insert_head_;
  // Last block is full, return the default end iterator that doesn't point to anything
  if (insert_head == accessor_.GetBlockLayout().NumSlots()) return {this, blocks_.end(), 0};
  // Otherwise, insert head points to the slot that will be inserted next, which would be exactly what we want.
  return {this, last_block, insert_head};
}

bool DataTable::Update(transaction::TransactionContext *const txn, const TupleSlot slot, const ProjectedRow &redo) {
  TERRIER_ASSERT(redo.NumColumns() <= accessor_.GetBlockLayout().NumColumns() - NUM_RESERVED_COLUMNS,
                 "The input buffer cannot change the reserved columns, so it should have fewer attributes.");
  TERRIER_ASSERT(redo.NumColumns() > 0, "The input buffer should modify at least one attribute.");
  UndoRecord *const undo = txn->UndoRecordForUpdate(this, slot, redo);
  slot.GetBlock()->controller_.WaitUntilHot();
  UndoRecord *version_ptr;
  do {
    version_ptr = AtomicallyReadVersionPtr(slot, accessor_);

    // Since we disallow write-write conflicts, the version vector pointer is essentially an implicit
    // write lock on the tuple.
    if (HasConflict(*txn, version_ptr) || !Visible(slot, accessor_)) {
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
  } while (!CompareAndSwapVersionPtr(slot, accessor_, version_ptr, undo));

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
                 "The input buffer never changes the version pointer column, so it should have  exactly 1 fewer "
                 "attribute than the DataTable's layout.");

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
  InsertInto(txn, redo, result);
  data_table_counter_.IncrementNumInsert(1);
  return result;
}

void DataTable::InsertInto(transaction::TransactionContext *txn, const ProjectedRow &redo, TupleSlot dest) {
  TERRIER_ASSERT(accessor_.Allocated(dest), "destination slot must already be allocated");
  TERRIER_ASSERT(accessor_.IsNull(dest, VERSION_POINTER_COLUMN_ID),
                 "The slot needs to be logically deleted to every running transaction");
  // At this point, sequential scan down the block can still see this, except it thinks it is logically deleted if we 0
  // the primary key column
  UndoRecord *undo = txn->UndoRecordForInsert(this, dest);
  TERRIER_ASSERT(dest.GetBlock()->controller_.GetBlockState()->load() == BlockState::HOT,
                 "Should only be able to insert into hot blocks");
  AtomicallyWriteVersionPtr(dest, accessor_, undo);
  // Set the logically deleted bit to present as the undo record is ready
  accessor_.AccessForceNotNull(dest, VERSION_POINTER_COLUMN_ID);
  // Update in place with the new value.
  for (uint16_t i = 0; i < redo.NumColumns(); i++) {
    TERRIER_ASSERT(redo.ColumnIds()[i] != VERSION_POINTER_COLUMN_ID,
                   "Insert buffer should not change the version pointer column.");
    StorageUtil::CopyAttrFromProjection(accessor_, dest, redo, i);
  }
}

bool DataTable::Delete(transaction::TransactionContext *const txn, const TupleSlot slot) {
  data_table_counter_.IncrementNumDelete(1);
  UndoRecord *const undo = txn->UndoRecordForDelete(this, slot);
  slot.GetBlock()->controller_.WaitUntilHot();
  UndoRecord *version_ptr;
  do {
    version_ptr = AtomicallyReadVersionPtr(slot, accessor_);
    // Since we disallow write-write conflicts, the version vector pointer is essentially an implicit
    // write lock on the tuple.
    if (HasConflict(*txn, version_ptr) || !Visible(slot, accessor_)) {
      // Mark this UndoRecord as never installed by setting the table pointer to nullptr. This is inspected in the
      // TransactionManager's Rollback() and GC's Unlink logic
      undo->Table() = nullptr;
      return false;
    }

    // Update the next pointer of the new head of the version chain
    undo->Next() = version_ptr;
  } while (!CompareAndSwapVersionPtr(slot, accessor_, version_ptr, undo));

  // We have the write lock. Go ahead and flip the logically deleted bit to true
  accessor_.SetNull(slot, VERSION_POINTER_COLUMN_ID);
  return true;
}

template <class RowType>
bool DataTable::SelectIntoBuffer(transaction::TransactionContext *const txn, const TupleSlot slot,
                                 RowType *const out_buffer) const {
  TERRIER_ASSERT(out_buffer->NumColumns() <= accessor_.GetBlockLayout().NumColumns() - NUM_RESERVED_COLUMNS,
                 "The output buffer never returns the version pointer columns, so it should have "
                 "fewer attributes.");
  TERRIER_ASSERT(out_buffer->NumColumns() > 0, "The output buffer should return at least one attribute.");
  // This cannot be visible if it's already deallocated.
  if (!accessor_.Allocated(slot)) return false;

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

    // We still need to check the allocated bit because GC could have flipped it since last check
    visible = Visible(slot, accessor_);

    // Here we will need to check that the version pointer did not change during our read. If it did, the content
    // we have read might have been rolled back and an abort has already unlinked the associated undo-record,
    // we will have to loop around to avoid a dirty read.
    //
    // There is still an a-b-a problem if aborting transactions unlink themselves. Thus, in the system aborting
    // transactions still check out a timestamp and "commit" after rolling back their changes to guard against this,
    // The exact interleaving is this:
    //
    //      transaction 1         transaction 2
    //          begin
    //    read version_ptr
    //                                begin
    //                             write a -> a1
    //          read a1
    //                            rollback a1 -> a
    //    check version_ptr
    //         return a1
    //
    // For this to manifest, there has to be high contention on a given tuple slot, and insufficient CPU resources
    // (way more threads than there are cores, around 8x seems to work) such that threads are frequently swapped
    // out. compare-and-swap along with the pointer reduces the probability of this happening to be essentially
    // infinitesimal, but it's still a probabilistic fix. To 100% prevent this race, we have to wait until no
    // concurrent transaction with the abort that could have had a dirty read is alive to unlink this. The easiest
    // way to achieve that is to take a timestamp as well when all changes have been rolled back for an aborted
    // transaction, and let GC handle the unlinking.
  } while (version_ptr != AtomicallyReadVersionPtr(slot, accessor_));

  // Nullptr in version chain means no other versions visible to any transaction alive at this point.
  // Alternatively, if the current transaction holds the write lock, it should be able to read its own updates.
  if (version_ptr == nullptr || version_ptr->Timestamp().load() == txn->FinishTime()) {
    return visible;
  }

  // Apply deltas until we reconstruct a version safe for us to read
  while (version_ptr != nullptr &&
         transaction::TransactionUtil::NewerThan(version_ptr->Timestamp().load(), txn->StartTime())) {
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
        break;
      default:
        throw std::runtime_error("unexpected delta record type");
    }
    version_ptr = version_ptr->Next();
  }

  return visible;
}

template bool DataTable::SelectIntoBuffer<ProjectedRow>(transaction::TransactionContext *txn, const TupleSlot slot,
                                                        ProjectedRow *const out_buffer) const;
template bool DataTable::SelectIntoBuffer<ProjectedColumns::RowView>(transaction::TransactionContext *txn,
                                                                     const TupleSlot slot,
                                                                     ProjectedColumns::RowView *const out_buffer) const;

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
  const bool present = accessor.Allocated(slot);
  const bool not_deleted = !accessor.IsNull(slot, VERSION_POINTER_COLUMN_ID);
  return present && not_deleted;
}

bool DataTable::HasConflict(const transaction::TransactionContext &txn, UndoRecord *const version_ptr) const {
  if (version_ptr == nullptr) return false;  // Nobody owns this tuple's write lock, no older version visible
  const transaction::timestamp_t version_timestamp = version_ptr->Timestamp().load();
  const transaction::timestamp_t txn_id = txn.FinishTime();
  const transaction::timestamp_t start_time = txn.StartTime();
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
  accessor_.InitializeRawBlock(this, new_block, layout_version_);
  blocks_.push_back(new_block);
  insertion_head_ = new_block;
  data_table_counter_.IncrementNumNewBlock(1);
}

bool DataTable::HasConflict(const transaction::TransactionContext &txn, const TupleSlot slot) const {
  UndoRecord *const version_ptr = AtomicallyReadVersionPtr(slot, accessor_);
  return HasConflict(txn, version_ptr);
}

bool DataTable::IsVisible(const transaction::TransactionContext &txn, const TupleSlot slot) const {
  UndoRecord *version_ptr;
  bool visible;
  do {
    version_ptr = AtomicallyReadVersionPtr(slot, accessor_);
    // Here we will need to check that the version pointer did not change during our read. If it did, the visibility of
    // this tuple might have changed and we should check again.
    visible = Visible(slot, accessor_);
  } while (version_ptr != AtomicallyReadVersionPtr(slot, accessor_));

  // Nullptr in version chain means no other versions visible to any transaction alive at this point.
  // Alternatively, if the current transaction holds the write lock, it should be able to read its own updates.
  if (version_ptr == nullptr || version_ptr->Timestamp().load() == txn.FinishTime()) {
    return visible;
  }

  // Apply deltas until we determine a version safe for us to read
  while (version_ptr != nullptr &&
         transaction::TransactionUtil::NewerThan(version_ptr->Timestamp().load(), txn.StartTime())) {
    switch (version_ptr->Type()) {
      case DeltaRecordType::UPDATE:
        // Normal delta to be applied. Does not modify the logical delete column.
        break;
      case DeltaRecordType::INSERT:
        visible = false;
        break;
      case DeltaRecordType::DELETE:
        visible = true;
    }
    version_ptr = version_ptr->Next();
  }

  return visible;
}

}  // namespace terrier::storage
