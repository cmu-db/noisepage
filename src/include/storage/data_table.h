#pragma once
#include <unordered_map>
#include <vector>
#include "common/container/concurrent_map.h"
#include "common/container/concurrent_vector.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {

/**
 * A DataTable is a thin layer above blocks that handles visibility, schemas, and maintainence of versions for a
 * SQL table. This class should be the main outward facing API for the storage engine. SQL level concepts such
 * as SQL types, varlens and nullabilities are still not meaningful at this level.
 */
class DataTable {
 public:
  // TODO(Tianyu): Consider taking in some other info to avoid copying layout
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
   * @param txn_start_time the timestamp threshold that the returned projection should be visible at. In practice this
   *                       will just be the start time of the caller transaction.
   * @param slot the tuple slot to read
   * @param out_buffer output buffer. The object should already contain projection list information. @see ProjectedRow.
   */
  void Select(timestamp_t txn_start_time, TupleSlot slot, ProjectedRow *out_buffer) const;

  /**
   * Update the tuple according to the redo slot given, and update the version chain to link to the given
   * delta record. The delta record is populated with a before-image of the tuple in the process. Update will only
   * happen if there is no write-write conflict, otherwise, this is equivalent to a noop and false is returned,
   *
   * @param slot the slot of the tuple to update.
   * @param redo the desired change to be applied. This should be the after-image of the attributes of interest.
   * @param undo the undo record to maintain and populate. It is expected that the projected row has the same structure
   *             as the redo, but the contents need not be filled beforehand, and will be populated with the
   * before-image after this method returns.
   * @return whether the update is successful.
   */
  bool Update(TupleSlot slot, const ProjectedRow &redo, DeltaRecord *undo);

  /**
   * Inserts a tuple, as given in the redo, and update the version chain the link to the given
   * delta record. The slot allocated for the tuple and returned.
   *
   * @param redo after-image of the inserted tuple
   * @param undo the undo record to maintain and populate. It is expected that this simply contains one column of
   *             the table's primary key, set to null (logically deleted) to denote that the tuple did not exist
   *             before.
   * @return the TupleSlot allocated for this insert, used to identify this tuple's physical location in indexes and
   * such.
   */
  TupleSlot Insert(const ProjectedRow &redo, DeltaRecord *undo);

 private:
  BlockStore *block_store_;
  // TODO(Tianyu): this is here for when we support concurrent schema, for now we only have one per DataTable
  // common::ConcurrentMap<layout_version_t, TupleAccessStrategy> layouts_;
  // layout_version_t curr_layout_version_{0};
  // TODO(Tianyu): For now, on insertion, we simply sequentially go through a block and allocate a
  // new one when the current one is full. Needless to say, we will need to revisit this when writing GC.

  // TODO(Matt): remove this single TAS when using concurrent schema
  TupleAccessStrategy accessor_;

  common::ConcurrentVector<RawBlock *> blocks_;
  std::atomic<RawBlock *> insertion_head_ = nullptr;

  // Atomically read out the version pointer value.
  DeltaRecord *AtomicallyReadVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor) const;

  // Atomically write the version pointer value. Should only be used by Insert where there is guaranteed to be no
  // contention
  void AtomicallyWriteVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor, DeltaRecord *desired);

  // If there will be a write-write conflict.
  bool HasConflict(DeltaRecord *version_ptr, DeltaRecord *undo) {
    return version_ptr != nullptr  // Nobody owns this tuple's write lock, no older version visible
           && version_ptr->timestamp_ != undo->timestamp_  // This tuple's write lock is already owned by the txn
           && !transaction::TransactionUtil::Committed(
                  version_ptr->timestamp_);  // Nobody owns this tuple's write lock, older version still visible
  }

  // Compares and swaps the version pointer to be the undo record, only if its value is equal to the expected one.
  bool CompareAndSwapVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor, DeltaRecord *expected,
                                DeltaRecord *desired);

  // Allocates a new block to be used as insertion head.
  void NewBlock(RawBlock *expected_val);
};
}  // namespace terrier::storage
