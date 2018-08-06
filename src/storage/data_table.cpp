#include <unordered_map>

#include "storage/storage_util.h"
#include "storage/data_table.h"
// All tuples potentially visible to txns should have a non-null attribute of version vector.
// This is not to be confused with a non-null version vector that has value nullptr (0).
#define VERSION_VECTOR_COLUMN_ID PRESENCE_COLUMN_ID

namespace terrier::storage {
DataTable::DataTable(BlockStore *store, const BlockLayout &layout) : block_store_(store) {
  // DataTable's first column must be a size of 8 for the version chain
  PELOTON_ASSERT(layout.attr_sizes_[0] == 8);
  // DataTable's number of columns must be greater than 1 (first column is version info)
  PELOTON_ASSERT(layout.num_cols_ > 1);
  layouts_.Emplace(curr_layout_version_, layout);
  NewBlock(nullptr);
  PELOTON_ASSERT(insertion_head_ != nullptr);
}

void DataTable::Select(const timestamp_t txn_start_time, const TupleSlot slot, ProjectedRow *out_buffer) const {
  // Retrieve the access strategy for the block's layout version. It is expected that
  // a valid block is given, thus the access strategy must have already been created.
  auto it = layouts_.Find(slot.GetBlock()->layout_version_);
  PELOTON_ASSERT(it != layouts_.CEnd());
  const TupleAccessStrategy &accessor = it->second;

  // ProjectedRow should always have fewer attributes than the block layout because we will never return the
  // version ptr above the DataTable layer
  // Also, the version ptr should not be in there: it is a concept hidden from callers.
  PELOTON_ASSERT(out_buffer->NumColumns() < accessor.GetBlockLayout().num_cols_);
  PELOTON_ASSERT(out_buffer->NumColumns() > 0);

  // Copy the current (most recent) tuple into the projection list. These operations don't need to be atomic,
  // because so long as we set the version ptr before updating in place, the reader will know if a conflict
  // can potentially happen, and chase the version chain before returning anyway,
  for (uint16_t i = 0; i < out_buffer->NumColumns(); i++)
    StorageUtil::CopyAttrIntoProjection(accessor, slot, out_buffer, i);

  // TODO(Tianyu): Potentially we need a memory fence here to make sure the check on version ptr
  // happens after the row is populated. For now, the compiler should not be smart (or rebellious)
  // enough to reorder this operation in or in front of the for loop.
  DeltaRecord *version_ptr = AtomicallyReadVersionPtr(slot, accessor);

  // Nullptr in version chain means no version visible to any transaction alive at this point.
  if (version_ptr == nullptr) return;

  // Creates a mapping from col offset to project list index. This allows us to efficiently
  // access columns since deltas can concern a different set of columns when chasing the
  // version chain
  std::unordered_map<uint16_t, uint16_t> col_to_projection_list_index;
  for (uint16_t i = 0; i < out_buffer->NumColumns(); i++)
    col_to_projection_list_index.emplace(out_buffer->ColumnIds()[i], i);

  // Apply deltas until we reconstruct a version safe for us to read
  // If the version chain becomes null, this tuple does not exist for this version, and the last delta
  // record would be an undo for insert that sets the primary key to null, which is intended behavior.
  while (version_ptr != nullptr
      && transaction::TransactionUtil::NewerThan(version_ptr->timestamp_, txn_start_time)) {
    StorageUtil::ApplyDelta(accessor.GetBlockLayout(),
                            *(version_ptr->Delta()),
                            out_buffer,
                            col_to_projection_list_index);
    version_ptr = version_ptr->next_;
  }
}

bool DataTable::Update(const TupleSlot slot, const ProjectedRow &redo, DeltaRecord *undo) {
  auto it = layouts_.Find(slot.GetBlock()->layout_version_);
  PELOTON_ASSERT(it != layouts_.End());
  const TupleAccessStrategy &accessor = it->second;

  // They should have the same layout.
  PELOTON_ASSERT(redo.NumColumns() == undo->Delta()->NumColumns());
  // TODO(Tianyu): Do we want to also check the column ids and order?

  DeltaRecord *version_ptr = AtomicallyReadVersionPtr(slot, accessor);
  // Since we disallow write-write conflicts, the version vector pointer is essentially an implicit
  // write lock on the tuple.
  if (HasConflict(version_ptr, undo)) return false;

  // Update the next pointer of the new head of the version chain
  undo->next_ = version_ptr;

  // TODO(Tianyu): Is it conceivable that the caller would have already obtained the values and don't need this?
  // Populate undo record with the before image of attribute
  for (uint16_t i = 0; i < redo.NumColumns(); i++)
    StorageUtil::CopyAttrIntoProjection(accessor, slot, undo->Delta(), i);

  // At this point, either tuple write lock is ownable, or the current transaction already owns this slot.
  if (!CompareAndSwapVersionPtr(slot, accessor, version_ptr, undo)) return false;
  // Update in place with the new value.
  for (uint16_t i = 0; i < redo.NumColumns(); i++) StorageUtil::CopyAttrFromProjection(accessor, slot, redo, i);

  return true;
}

TupleSlot DataTable::Insert(const ProjectedRow &redo, DeltaRecord *undo) {
  auto it = layouts_.Find(curr_layout_version_);
  PELOTON_ASSERT(it != layouts_.End());
  const TupleAccessStrategy &accessor = it->second;

  // The undo buffer for an insert should come in with a nullptr undo buffer
  PELOTON_ASSERT(undo->next_ == nullptr);

  // Since this is an insert, all the column values should be given in the redo.
  PELOTON_ASSERT(redo.NumColumns() == (accessor.GetBlockLayout().num_cols_ - 1));

  // Attempt to allocate a new tuple from the block we are working on right now.
  // If that block is full, try to request a new block. Because other concurrent
  // inserts could have already created a new block, we need to use compare and swap
  // to change the insertion head. We do not expect this loop to be executed more than
  // twice, but there is technically a possibility for blocks with only a few slots.
  TupleSlot result;
  while (true) {
    RawBlock *block = insertion_head_.load();
    if (accessor.Allocate(block, &result)) break;
    NewBlock(block);
  }

  // Version_vector column would have already been flipped to not null, but won't be in the redo given to us. We will
  // need to make sure that the new slot always have nullptr for version vector.
  StorageUtil::WriteBytes(sizeof(DeltaRecord *), 0, accessor.AccessForceNotNull(result, VERSION_VECTOR_COLUMN_ID));

  // Once the version vector is installed, the insert is the same as an update where columns from the
  // redo is copied into the block.
  // TODO(Tianyu): This is lazy. Realistically, we need one primary key column in the before-image as null
  // to denote an insert. Calling update means we are stupidly copying the entire tuple. That said, this won't
  // be a correctness issue. So we can fix later.
  UNUSED_ATTRIBUTE bool no_conflict = Update(result, redo, undo);
  // Expect no conflict because this version should only be visible to this transaction.
  PELOTON_ASSERT(no_conflict);
  return result;
}

DeltaRecord *DataTable::AtomicallyReadVersionPtr(const TupleSlot slot, const TupleAccessStrategy &accessor) const {
  // TODO(Tianyu): We can get rid of this and write a "AccessWithoutNullCheck" if this turns out to be
  // an issue (probably not, we are just reading one extra byte.)
  byte *ptr_location = accessor.AccessWithNullCheck(slot, VERSION_VECTOR_COLUMN_ID);
  PELOTON_ASSERT(ptr_location != nullptr);
  return reinterpret_cast<std::atomic<DeltaRecord *> *>(ptr_location)->load();
}

bool DataTable::CompareAndSwapVersionPtr(const TupleSlot slot,
                                         const TupleAccessStrategy &accessor,
                                         DeltaRecord *expected,
                                         DeltaRecord *desired) {
  byte *ptr_location = accessor.AccessWithNullCheck(slot, VERSION_VECTOR_COLUMN_ID);
  // The check should never fail. We should only update the version vector for a tuple that is present
  PELOTON_ASSERT(ptr_location != nullptr);
  return reinterpret_cast<std::atomic<DeltaRecord *> *>(ptr_location)->compare_exchange_strong(expected, desired);
}

void DataTable::NewBlock(RawBlock *expected_val) {
  // TODO(Tianyu): This shouldn't be performance-critical. So maybe use a latch instead of a cmpxchg?
  // This will eliminate retries, which could potentially be an expensive allocate (this is somewhat mitigated
  // by the object pool reuse)
  auto it = layouts_.Find(curr_layout_version_);
  PELOTON_ASSERT(it != layouts_.End());
  RawBlock *new_block = block_store_->Get();
  it->second.InitializeRawBlock(new_block, curr_layout_version_);
  if (insertion_head_.compare_exchange_strong(expected_val, new_block))
    blocks_.PushBack(new_block);
  else
    // If the compare and exchange failed, another thread might have already allocated a new block
    // We should release this new block and return. The caller would presumably try again on the new
    // block.
    block_store_->Release(new_block);
}
}  // namespace terrier::storage
