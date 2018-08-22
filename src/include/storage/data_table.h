#pragma once
#include <unordered_map>
#include <vector>
#include "common/container/concurrent_vector.h"
#include "storage/delta_record.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"

namespace terrier::transaction {
class TransactionContext;
class TransactionManager;
}  // namespace terrier::transaction

namespace terrier::storage {
// All tuples potentially visible to txns should have a non-null attribute of version vector.
// This is not to be confused with a non-null version vector that has value nullptr (0).
#define VERSION_POINTER_COLUMN_ID PRESENCE_COLUMN_ID
#define PRIMARY_KEY_COLUMN_ID 1
/**
 * A DataTable is a thin layer above blocks that handles visibility, schemas, and maintenance of versions for a
 * SQL table. This class should be the main outward facing API for the storage engine. SQL level concepts such
 * as SQL types, varlens and nullabilities are still not meaningful at this level.
 */
class DataTable {
 public:
  // TODO(Tianyu): Consider taking in some other info (like schema) to avoid copying layout. BlockLayout shouldn't
  // really exist above the storage layer
  /**
   * Constructs a new DataTable with the given layout, using the given BlockStore as the source
   * of its storage blocks.
   *
   * @param store the Block store to use.
   * @param layout the initial layout of this DataTable.
   */
  DataTable(BlockStore *store, const BlockLayout &layout);

  /**
   * Destructs a DataTable, frees all its blocks.
   */
  ~DataTable() {
    for (auto it = blocks_.Begin(); it != blocks_.End(); ++it) block_store_->Release(*it);
  }

  /**
   * Materializes a single tuple from the given slot, as visible at the timestamp.
   *
   * @param txn the calling transaction
   * @param slot the tuple slot to read
   * @param out_buffer output buffer. The object should already contain projection list information. @see ProjectedRow.
   */
  void Select(transaction::TransactionContext *txn, TupleSlot slot, ProjectedRow *out_buffer) const;

  /**
   * Update the tuple according to the redo buffer given, and update the version chain to link to the given
   * undo record. The undo record is populated with a before-image of the tuple in the process. Update will only
   * happen if there is no write-write conflict, otherwise, this is equivalent to a noop and false is returned,
   *
   * @param txn the calling transaction
   * @param slot the slot of the tuple to update.
   * @param redo the desired change to be applied. This should be the after-image of the attributes of interest.
   * @return whether the update is successful.
   */
  bool Update(transaction::TransactionContext *txn, TupleSlot slot, const ProjectedRow &redo);

  /**
   * Inserts a tuple, as given in the redo, and update the version chain the link to the given
   * delta record. The slot allocated for the tuple and returned.
   *
   * @param txn the calling transaction
   * @param redo after-image of the inserted tuple
   * @return the TupleSlot allocated for this insert, used to identify this tuple's physical location in indexes and
   * such.
   */
  const TupleSlot Insert(transaction::TransactionContext *txn, const ProjectedRow &redo);

 private:
  // The GarbageCollector needs to modify VersionPtrs when pruning version chains
  friend class GarbageCollector;
  // The TransactionManager needs to modify VersionPtrs when rolling back aborts
  friend class transaction::TransactionManager;

  BlockStore *const block_store_;
  // TODO(Tianyu): this is here for when we support concurrent schema, for now we only have one per DataTable
  // common::ConcurrentMap<layout_version_t, TupleAccessStrategy> layouts_;
  // layout_version_t curr_layout_version_{0};
  // TODO(Tianyu): For now, on insertion, we simply sequentially go through a block and allocate a
  // new one when the current one is full. Needless to say, we will need to revisit this when extending GC to handle
  // deleted tuples and recycle slots

  // TODO(Matt): remove this single TAS when using concurrent schema
  const TupleAccessStrategy accessor_;

  common::ConcurrentVector<RawBlock *> blocks_;
  std::atomic<RawBlock *> insertion_head_ = nullptr;

  // Atomically read out the version pointer value.
  UndoRecord *AtomicallyReadVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor) const;

  // Atomically write the version pointer value. Should only be used by Insert where there is guaranteed to be no
  // contention
  void AtomicallyWriteVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor, UndoRecord *desired);

  // Checks for Snapshot Isolation conflicts, used by Update
  bool HasConflict(UndoRecord *version_ptr, transaction::TransactionContext *txn);

  // Compares and swaps the version pointer to be the undo record, only if its value is equal to the expected one.
  bool CompareAndSwapVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor, UndoRecord *expected,
                                UndoRecord *desired);

  // Allocates a new block to be used as insertion head.
  void NewBlock(RawBlock *expected_val);
};
}  // namespace terrier::storage
