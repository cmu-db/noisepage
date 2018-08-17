#include <unordered_map>

#include "common/main_stat_registry.h"
#include "storage/storage_util.h"
#include "storage/data_table.h"
// All tuples potentially visible to txns should have a non-null attribute of version vector.
// This is not to be confused with a non-null version vector that has value nullptr (0).
#define VERSION_VECTOR_COLUMN_ID PRESENCE_COLUMN_ID

namespace terrier::storage {
DataTable::DataTable(BlockStore *store, const BlockLayout &layout) : block_store_(store), accessor_(layout) {
  data_table_counter_ = new DataTableCounter;
  STAT_REGISTER({"Storage"}, data_table_counter_, this);
  PELOTON_ASSERT(layout.attr_sizes_[0] == 8, "First column must have size 8 for the version chain.");
  PELOTON_ASSERT(layout.num_cols_ > 1, "First column is reserved for version info.");
  NewBlock(nullptr);
  PELOTON_ASSERT(insertion_head_ != nullptr, "Insertion head should not be null after creating new block.");
}

void DataTable::Select(transaction::TransactionContext *txn,
                       const TupleSlot slot,
                       ProjectedRow *out_buffer) const {
  PELOTON_ASSERT(out_buffer->NumColumns() < accessor_.GetBlockLayout().num_cols_,
                 "The projection never returns the version pointer, so it should have fewer attributes.");
  PELOTON_ASSERT(out_buffer->NumColumns() > 0, "The projection should return at least one attribute.");

  DeltaRecord *version_ptr;
  do {
    version_ptr = AtomicallyReadVersionPtr(slot, accessor_);
    // Copy the current (most recent) tuple into the projection list. These operations don't need to be atomic,
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
  if (version_ptr == nullptr || version_ptr->Timestamp().load() == txn->TxnId()) return;

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
  && transaction::TransactionUtil::NewerThan(version_ptr->Timestamp().load(), txn->StartTime())) {
    StorageUtil::ApplyDelta(accessor_.GetBlockLayout(),
                            *(version_ptr->Delta()),
                            out_buffer,
                            col_to_projection_list_index);
    version_ptr = version_ptr->Next();
  }
  data_table_counter_->Inc_num_select();
}

bool DataTable::Update(transaction::TransactionContext *txn,
                       const TupleSlot slot,
                       const ProjectedRow &redo) {
  // TODO(Tianyu): We never bother deallocating this entry, which is why we need to remember to check on abort
  // whether the transaction actually holds a write lock
  DeltaRecord *undo = txn->UndoRecordForUpdate(this, slot, redo);
  DeltaRecord *version_ptr = AtomicallyReadVersionPtr(slot, accessor_);
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

  data_table_counter_->Inc_num_update();
  return true;
}

TupleSlot DataTable::Insert(transaction::TransactionContext *txn,
                            const ProjectedRow &redo) {
  // Attempt to allocate a new tuple from the block we are working on right now.
  // If that block is full, try to request a new block. Because other concurrent
  // inserts could have already created a new block, we need to use compare and swap
  // to change the insertion head. We do not expect this loop to be executed more than
  // twice, but there is technically a possibility for blocks with only a few slots.
  TupleSlot result;
  while (true) {
    RawBlock *block = insertion_head_.load();
    if (accessor_.Allocate(block, &result)) break;
    NewBlock(block);
  }
  // At this point, sequential scan down the block can still see this, except it thinks it is logically deleted if we 0
  // the primary key column
  DeltaRecord *undo = txn->UndoRecordForInsert(this, accessor_.GetBlockLayout(), result);

  // Populate undo record with the before image of presence column
  undo->Delta()->SetNull(0);

  // Update the version pointer atomically so that a sequential scan will not see inconsistent version pointer, which
  // may result in a segfault
  AtomicallyWriteVersionPtr(result, accessor_, undo);

  // At this point, a sequential scan can see this tuple, but will follow the version chain to see a logically deleted
  // version

  // Update in place with the new value.
  for (uint16_t i = 0; i < redo.NumColumns(); i++) StorageUtil::CopyAttrFromProjection(accessor_, result, redo, i);

  data_table_counter_->Inc_num_insert();
  return result;
}

void DataTable::Rollback(timestamp_t txn_id, terrier::storage::TupleSlot slot) {
  DeltaRecord *version_ptr = AtomicallyReadVersionPtr(slot, accessor_);
  // We do not hold the lock. Should just return
  if (version_ptr == nullptr || version_ptr->Timestamp().load() != txn_id) return;
  // Re-apply the before image
  for (uint16_t i = 0; i < version_ptr->Delta()->NumColumns(); i++)
    StorageUtil::CopyAttrFromProjection(accessor_, slot, *(version_ptr->Delta()), i);
  // Remove this delta record from the version chain, effectively releasing the lock. At this point, the tuple
  // has been restored to its original form. No CAS needed since we still hold the write lock at time of the atomic
  // write.
  AtomicallyWriteVersionPtr(slot, accessor_, version_ptr->Next());
  data_table_counter_->Inc_num_rollback();
}

DeltaRecord *DataTable::AtomicallyReadVersionPtr(const TupleSlot slot, const TupleAccessStrategy &accessor) const {
  // TODO(Tianyu): We can get rid of this and write a "AccessWithoutNullCheck" if this turns out to be
  // an issue (probably not, we are just reading one extra byte.)
  byte *ptr_location = accessor.AccessWithNullCheck(slot, VERSION_VECTOR_COLUMN_ID);
  PELOTON_ASSERT(ptr_location != nullptr, "Version pointer cannot be null.");
  return reinterpret_cast<std::atomic<DeltaRecord *> *>(ptr_location)->load();
}

void DataTable::AtomicallyWriteVersionPtr(const TupleSlot slot,
                                          const TupleAccessStrategy &accessor,
                                          DeltaRecord *desired) {
  byte *ptr_location = accessor.AccessWithNullCheck(slot, VERSION_VECTOR_COLUMN_ID);
  PELOTON_ASSERT(ptr_location != nullptr, "Only write version vectors for tuples that are present.");
  reinterpret_cast<std::atomic<DeltaRecord *> *>(ptr_location)->store(desired);
}

bool DataTable::CompareAndSwapVersionPtr(const TupleSlot slot,
                                         const TupleAccessStrategy &accessor,
                                         DeltaRecord *expected,
                                         DeltaRecord *desired) {
  byte *ptr_location = accessor.AccessWithNullCheck(slot, VERSION_VECTOR_COLUMN_ID);
  PELOTON_ASSERT(ptr_location != nullptr, "Only write version vectors for tuples that are present.");
  return reinterpret_cast<std::atomic<DeltaRecord *> *>(ptr_location)
      ->compare_exchange_strong(expected, desired);
}

void DataTable::NewBlock(RawBlock *expected_val) {
  // TODO(Tianyu): This shouldn't be performance-critical. So maybe use a latch instead of a cmpxchg?
  // This will eliminate retries, which could potentially be an expensive allocate (this is somewhat mitigated
  // by the object pool reuse)
  RawBlock *new_block = block_store_->Get();
  accessor_.InitializeRawBlock(new_block, layout_version_t(0));
  if (insertion_head_.compare_exchange_strong(expected_val, new_block))
    blocks_.PushBack(new_block);
  else
    // If the compare and exchange failed, another thread might have already allocated a new block
    // We should release this new block and return. The caller would presumably try again on the new
    // block.
    block_store_->Release(new_block);
  data_table_counter_->Inc_num_new_block();
}
}  // namespace terrier::storage
