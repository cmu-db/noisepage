#include "storage/data_table.h"

#include <list>

#include "common/allocator.h"
#include "execution/sql/vector_projection.h"
#include "storage/block_access_controller.h"
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {

DataTable::DataTable(const common::ManagedPointer<BlockStore> store, const BlockLayout &layout,
                     const layout_version_t layout_version)
    : block_store_(store), layout_version_(layout_version), accessor_(layout) {
  TERRIER_ASSERT(layout.AttrSize(VERSION_POINTER_COLUMN_ID) == 8,
                 "First column must have size 8 for the version chain.");
  TERRIER_ASSERT(layout.NumColumns() > NUM_RESERVED_COLUMNS,
                 "First column is reserved for version info, second column is reserved for logical delete.");
  if (block_store_ != nullptr) {
    RawBlock *new_block = NewBlock();
    // insert block
    blocks_.push_back(new_block);
  }
  insertion_head_ = 0;
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

DataTable::SlotIterator &DataTable::SlotIterator::operator++() {
  // Jump to the next block if already the last slot in the block.
  if (current_slot_.GetOffset() == table_->accessor_.GetBlockLayout().NumSlots() - 1) {
    if (num_advances_ > 0 || num_advances_ == SlotIterator::ADVANCE_TO_THE_END) {
      // Advance to the next block.
      ++block_index_;
      num_advances_ = num_advances_ == SlotIterator::ADVANCE_TO_THE_END ? num_advances_ : num_advances_ - 1;
      // Cannot dereference if the next block is end(), so just use nullptr to denote
      {
        // TODO(WAN): Lin, does this latch still need to be temporarily commented out for TPCH?
        common::SpinLatch::ScopedSpinLatch guard(&table_->blocks_latch_);
        if (block_index_ >= table_->blocks_.size()) {
          current_slot_ = DataTable::SlotIterator::InvalidTupleSlot();
        } else {
          current_slot_ = {table_->blocks_[block_index_], 0};
        }
      }
    } else {
      // Done advancing, time to give up.
      current_slot_ = DataTable::SlotIterator::InvalidTupleSlot();
    }
  } else {
    current_slot_ = {current_slot_.GetBlock(), current_slot_.GetOffset() + 1};
  }
  return *this;
}

DataTable::SlotIterator DataTable::end() const {  // NOLINT for STL name compability
  // TODO(Lin): We need to temporarily comment out this latch for the concurrent TPCH experiments. Should be replaced
  //  with a real solution
  common::SpinLatch::ScopedSpinLatch guard(&blocks_latch_);
  // TODO(Tianyu): Need to look in detail at how this interacts with compaction when that gets in.

  // The end iterator could either point to an unfilled slot in a block, or point to nothing if every block in the
  // table is full. In the case that it points to nothing, we will use the end of the blocks list and
  // 0 to denote that this is the case. This solution makes increment logic simple and natural.
  uint32_t num_blocks = blocks_.size();
  if (blocks_.empty()) return {this, num_blocks, SlotIterator::ADVANCE_TO_THE_END, 0};
  uint32_t last_block_index = num_blocks - 1;
  uint32_t insert_head = blocks_[last_block_index]->GetInsertHead();
  // Last block is full, return the default end iterator that doesn't point to anything
  if (insert_head == accessor_.GetBlockLayout().NumSlots())
    return {this, num_blocks, SlotIterator::ADVANCE_TO_THE_END, 0};
  // Otherwise, insert head points to the slot that will be inserted next, which would be exactly what we want.
  return {this, last_block_index, SlotIterator::ADVANCE_TO_THE_END, insert_head};
}

DataTable::SlotIterator DataTable::GetBlockedSlotIterator(uint32_t start, uint32_t end) const {
  TERRIER_ASSERT(start <= end, "Start index should come before ending index.");
  TERRIER_ASSERT(static_cast<int32_t>(end - start - 1) >= 0, "Too many blocks or sign issue.");

  common::SpinLatch::ScopedSpinLatch guard(&blocks_latch_);
  TERRIER_ASSERT(start <= blocks_.size() && end <= blocks_.size(), "Indexes must be within bounds.");
  return {this, start, static_cast<int32_t>(end - start - 1), 0};
}

bool DataTable::Update(const common::ManagedPointer<transaction::TransactionContext> txn, const TupleSlot slot,
                       const ProjectedRow &redo) {
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

  return true;
}

void DataTable::CheckMoveHead(uint32_t block_index) {
  // Assume block is full.
  if (block_index == insertion_head_.load()) {
    // If the header block is full, move the header to point to the next block.
    insertion_head_++;
  }

  // If there are no more free blocks, create a new empty block and point the insertion_head to it.
  if (insertion_head_.load() == blocks_.size()) {
    RawBlock *new_block = NewBlock();
    // Take the latch and insert the block. The insertion head will already have the right index.
    common::SpinLatch::ScopedSpinLatch guard_block(&blocks_latch_);
    blocks_.push_back(new_block);
  }
}

TupleSlot DataTable::Insert(const common::ManagedPointer<transaction::TransactionContext> txn,
                            const ProjectedRow &redo) {
  TERRIER_ASSERT(redo.NumColumns() == accessor_.GetBlockLayout().NumColumns() - NUM_RESERVED_COLUMNS,
                 "The input buffer never changes the version pointer column, so it should have  exactly 1 fewer "
                 "attribute than the DataTable's layout.");

  // Insertion header points to the first block that has free tuple slots
  // Once a txn arrives, it will start from the insertion header to find the first
  // idle (no other txn is trying to get tuple slots in that block) and non-full block.
  // If no such block is found, the txn will create a new block.
  // Before the txn writes to the block, it will set block status to busy.
  // The first bit of block insert_head_ is used to indicate if the block is busy
  // If the first bit is 1, it indicates one txn is writing to the block.

  TupleSlot result;
  auto block_index = insertion_head_.load();
  RawBlock *block;

  while (true) {
    // No free block left
    if (block_index == blocks_.size()) {
      RawBlock *new_block = NewBlock();
      TERRIER_ASSERT(accessor_.SetBlockBusyStatus(new_block), "Status of new block should not be busy");
      // No need to flip the busy status bit
      accessor_.Allocate(new_block, &result);
      // take latch
      common::SpinLatch::ScopedSpinLatch guard(&blocks_latch_);
      // insert block
      blocks_.push_back(new_block);
      block = new_block;
      break;
    }

    {
      common::SpinLatch::ScopedSpinLatch guard(&blocks_latch_);
      block = blocks_[block_index];
    }

    if (accessor_.SetBlockBusyStatus(block)) {
      // No one is inserting into this block
      if (accessor_.Allocate(block, &result)) {
        // The block is not full, succeed
        break;
      }
      // Fail to insert into the block, flip back the status bit
      accessor_.ClearBlockBusyStatus(block);
      // if the full block is the insertion_header, move the insertion_header
      // Next insert txn will search from the new insertion_header
      CheckMoveHead(block_index);
    }
    // The block is full or the block is being inserted by other txn, try next block
    ++block_index;
  }

  // Do not need to wait unit finish inserting,
  // can flip back the status bit once the thread gets the allocated tuple slot
  accessor_.ClearBlockBusyStatus(block);
  InsertInto(txn, redo, result);

  return result;
}

void DataTable::InsertInto(const common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &redo,
                           TupleSlot dest) {
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
  TERRIER_ASSERT(out_buffer->NumColumns() <= accessor_.GetBlockLayout().NumColumns() - NUM_RESERVED_COLUMNS,
                 "The output buffer never returns the version pointer columns, so it should have "
                 "fewer attributes.");
  TERRIER_ASSERT(out_buffer->NumColumns() > 0, "The output buffer should return at least one attribute.");
  // This cannot be visible if it's already deallocated.
  if (!accessor_.Allocated(slot)) return false;

  // Copy the current (most recent) tuple into the output buffer. These operations don't need to be atomic,
  // because so long as we set the version ptr before updating in place, the reader will chase the version chain
  // and apply the pre-image of the writer before returning anyway.  In the worst case, we accidentally overwrite
  // a good read with the exact same data, but there is no way to detect this.
  for (uint16_t i = 0; i < out_buffer->NumColumns(); i++) {
    TERRIER_ASSERT(out_buffer->ColumnIds()[i] != VERSION_POINTER_COLUMN_ID,
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

}  // namespace terrier::storage
