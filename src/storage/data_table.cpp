#include "storage/data_table.h"

#include <list>

#include "common/allocator.h"
#include "execution/sql/vector_projection.h"
#include "storage/block_access_controller.h"
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_util.h"

namespace noisepage::storage {

DataTable::DataTable(common::ManagedPointer<BlockStore> store, const BlockLayout &layout,
                     const layout_version_t layout_version)
    : accessor_(layout), block_store_(store), layout_version_(layout_version) {
  NOISEPAGE_ASSERT(layout.AttrSize(VERSION_POINTER_COLUMN_ID) == 8,
                   "First column must have size 8 for the version chain.");
  NOISEPAGE_ASSERT(layout.NumColumns() > NUM_RESERVED_COLUMNS,
                   "First column is reserved for version info, second column is reserved for logical delete.");
  if (store != DISABLED) {
    blocks_.push_back(NewBlock());
    blocks_size_++;
  }
}

DataTable::~DataTable() {
  common::SharedLatch::ScopedExclusiveLatch latch(&blocks_latch_);
  for (auto block : blocks_) {
    StorageUtil::DeallocateVarlens(block, accessor_);
    for (col_id_t i : accessor_.GetBlockLayout().Varlens())
      accessor_.GetArrowBlockMetadata(block).GetColumnInfo(accessor_.GetBlockLayout(), i).Deallocate();
    block_store_.operator->()->Release(block);
  }
}

bool DataTable::Select(const common::ManagedPointer<transaction::TransactionContext> txn, TupleSlot slot,
                       ProjectedRow *out_buffer) const {
  return SelectIntoBuffer(txn, slot, out_buffer);
}

void DataTable::Scan(const common::ManagedPointer<transaction::TransactionContext> txn, SlotIterator *const start_pos,
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

void DataTable::Scan(const common::ManagedPointer<transaction::TransactionContext> txn, SlotIterator *const start_pos,
                     execution::sql::VectorProjection *const out_buffer) const {
  uint32_t filled = 0;
  while (filled < out_buffer->GetTupleCapacity() && *start_pos != end() &&
         **start_pos != SlotIterator::InvalidTupleSlot()) {
    execution::sql::VectorProjection::RowView row = out_buffer->InterpretAsRow(filled);
    const TupleSlot slot = **start_pos;
    // Only fill the buffer with valid, visible tuples
    if (SelectIntoBuffer(txn, slot, &row)) {
      row.SetTupleSlot(slot);
      filled++;
    }
    ++(*start_pos);
  }
  out_buffer->Reset(filled);
}

bool DataTable::Update(const common::ManagedPointer<transaction::TransactionContext> txn, const TupleSlot slot,
                       const ProjectedRow &redo) {
  NOISEPAGE_ASSERT(redo.NumColumns() <= accessor_.GetBlockLayout().NumColumns() - NUM_RESERVED_COLUMNS,
                   "The input buffer cannot change the reserved columns, so it should have fewer attributes.");
  NOISEPAGE_ASSERT(redo.NumColumns() > 0, "The input buffer should modify at least one attribute.");
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
    NOISEPAGE_ASSERT(redo.ColumnIds()[i] != VERSION_POINTER_COLUMN_ID,
                     "Input buffer should not change the version pointer column.");
    // TODO(Matt): It would be nice to check that a ProjectedRow that modifies the logical delete column only originated
    // from the DataTable calling Update() within Delete(), rather than an outside soure modifying this column, but
    // that's difficult with this implementation
    StorageUtil::CopyAttrFromProjection(accessor_, slot, redo, i);
  }

  return true;
}

TupleSlot DataTable::Insert(const common::ManagedPointer<transaction::TransactionContext> txn,
                            const ProjectedRow &redo) {
  NOISEPAGE_ASSERT(redo.NumColumns() == accessor_.GetBlockLayout().NumColumns() - NUM_RESERVED_COLUMNS,
                   "The input buffer never changes the version pointer column, so it should have  exactly 1 fewer "
                   "attribute than the DataTable's layout.");

  // Insertion index points to the first block that has free tuple slots
  // Once a txn arrives, it will start from the insertion index to find the first
  // idle (no other txn is trying to get tuple slots in that block) and non-full block.
  // If no such block is found, the txn will create a new block.
  // Before the txn writes to the block, it will set block status to busy.
  // The first bit of block insert_head_ is used to indicate if the block is busy
  // If the first bit is 1, it indicates one txn is writing to the block.
  TupleSlot result;
  uint64_t current_insert_idx = insert_index_.load();
  RawBlock *block;
  while (true) {
    // No free block left
    uint64_t size = blocks_size_;
    if (current_insert_idx >= size) {
      block = NewBlock();
      common::SharedLatch::ScopedExclusiveLatch latch(&blocks_latch_);
      blocks_.push_back(block);
      blocks_size_ = blocks_.size();
      current_insert_idx = blocks_size_ - 1;
    } else {
      common::SharedLatch::ScopedSharedLatch latch(&blocks_latch_);
      block = blocks_[current_insert_idx];
    }
    if (accessor_.SetBlockBusyStatus(block)) {
      // No one is inserting into this block
      if (accessor_.Allocate(block, &result)) {
        // The block is not full, succeed
        break;
      }

      // if the full block is the insertion_header, move the insertion_header
      // Next insert txn will search from the new insertion_header
      if (current_insert_idx == insert_index_.load()) {
        // if we fail, that's ok because that means that someone else incremented insert_index_
        // so we retry on the next index
        bool UNUSED_ATTRIBUTE result =
            insert_index_.compare_exchange_strong(current_insert_idx, current_insert_idx + 1);
        NOISEPAGE_ASSERT(result, "only one thread should be able to try (and fail) to insert into a block at a time");
      }

      // Fail to insert into the block, flip back the status bit
      accessor_.ClearBlockBusyStatus(block);
    }
    // The block is full or the block is being inserted by other txn, try next block
    ++current_insert_idx;
  }

  // Do not need to wait unit finish inserting,
  // can flip back the status bit once the thread gets the allocated tuple slot
  accessor_.ClearBlockBusyStatus(block);
  InsertInto(txn, redo, result);

  return result;
}

void DataTable::InsertInto(const common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &redo,
                           TupleSlot dest) {
  NOISEPAGE_ASSERT(accessor_.Allocated(dest), "destination slot must already be allocated");
  NOISEPAGE_ASSERT(accessor_.IsNull(dest, VERSION_POINTER_COLUMN_ID),
                   "The slot needs to be logically deleted to every running transaction");
  // At this point, sequential scan down the block can still see this, except it thinks it is logically deleted if we 0
  // the primary key column
  UndoRecord *undo = txn->UndoRecordForInsert(this, dest);
  NOISEPAGE_ASSERT(dest.GetBlock()->controller_.GetBlockState()->load() == BlockState::HOT,
                   "Should only be able to insert into hot blocks");
  AtomicallyWriteVersionPtr(dest, accessor_, undo);
  // Set the logically deleted bit to present as the undo record is ready
  accessor_.AccessForceNotNull(dest, VERSION_POINTER_COLUMN_ID);
  // Update in place with the new value.
  for (uint16_t i = 0; i < redo.NumColumns(); i++) {
    NOISEPAGE_ASSERT(redo.ColumnIds()[i] != VERSION_POINTER_COLUMN_ID,
                     "Insert buffer should not change the version pointer column.");
    StorageUtil::CopyAttrFromProjection(accessor_, dest, redo, i);
  }
}

bool DataTable::Delete(const common::ManagedPointer<transaction::TransactionContext> txn, const TupleSlot slot) {
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
bool DataTable::SelectIntoBuffer(const common::ManagedPointer<transaction::TransactionContext> txn,
                                 const TupleSlot slot, RowType *const out_buffer) const {
  NOISEPAGE_ASSERT(out_buffer->NumColumns() <= accessor_.GetBlockLayout().NumColumns() - NUM_RESERVED_COLUMNS,
                   "The output buffer never returns the version pointer columns, so it should have "
                   "fewer attributes.");
  NOISEPAGE_ASSERT(out_buffer->NumColumns() > 0, "The output buffer should return at least one attribute.");
  // This cannot be visible if it's already deallocated.
  if (!accessor_.Allocated(slot)) return false;

  // Copy the current (most recent) tuple into the output buffer. These operations don't need to be atomic,
  // because so long as we set the version ptr before updating in place, the reader will chase the version chain
  // and apply the pre-image of the writer before returning anyway.  In the worst case, we accidentally overwrite
  // a good read with the exact same data, but there is no way to detect this.
  for (uint16_t i = 0; i < out_buffer->NumColumns(); i++) {
    NOISEPAGE_ASSERT(out_buffer->ColumnIds()[i] != VERSION_POINTER_COLUMN_ID,
                     "Output buffer should not read the version pointer column.");
    StorageUtil::CopyAttrIntoProjection(accessor_, slot, out_buffer, i);
  }

  bool visible = !accessor_.IsNull(slot, VERSION_POINTER_COLUMN_ID);
  UndoRecord *version_ptr = AtomicallyReadVersionPtr(slot, accessor_);

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

template bool DataTable::SelectIntoBuffer<ProjectedRow>(
    const common::ManagedPointer<transaction::TransactionContext> txn, const TupleSlot slot,
    ProjectedRow *const out_buffer) const;
template bool DataTable::SelectIntoBuffer<ProjectedColumns::RowView>(
    const common::ManagedPointer<transaction::TransactionContext> txn, const TupleSlot slot,
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

RawBlock *DataTable::NewBlock() {
  RawBlock *new_block = block_store_->Get();
  accessor_.InitializeRawBlock(this, new_block, layout_version_);
  return new_block;
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

}  // namespace noisepage::storage
