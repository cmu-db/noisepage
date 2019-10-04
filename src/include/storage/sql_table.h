#pragma once
#include <list>
#include <set>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "storage/data_table.h"
#include "storage/projected_columns.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"
#include "storage/write_ahead_log/log_record.h"
#include "transaction/transaction_context.h"

namespace terrier {
// Forward Declaration
class LargeSqlTableTestObject;
class RandomSqlTableTransaction;
}  // namespace terrier

namespace terrier::storage {

/**
 * A SqlTable is a thin layer above DataTable that replaces storage layer concepts like BlockLayout with SQL layer
 * concepts like Schema. The goal is to hide concepts like col_id_t and BlockLayout above the SqlTable level.
 * The SqlTable API should only refer to storage concepts via things like Schema and col_oid_t, and then perform the
 * translation to BlockLayout and col_id_t to talk to the DataTable and other areas of the storage layer.
 */
class SqlTable {
  /**
   * Contains all of the metadata the SqlTable needs to reference a DataTable. We shouldn't ever have to expose these
   * concepts to anyone above the SqlTable level. If you find yourself wanting to return BlockLayout or col_id_t above
   * this layer, consider alternatives.
   */
  struct DataTableVersion {
    DataTable *data_table_;
    BlockLayout layout_;
    ColumnMap column_map_;
  };

 public:
  /**
   * Constructs a new SqlTable with the given Schema, using the given BlockStore as the source
   * of its storage blocks.
   *
   * @param store the Block store to use.
   * @param schema the initial Schema of this SqlTable
   */
  SqlTable(BlockStore *store, const catalog::Schema &schema);

  /**
   * Destructs a SqlTable, frees all its members.
   */
  ~SqlTable() { delete table_.data_table_; }

  /**
   * Materializes a single tuple from the given slot, as visible at the timestamp of the calling txn.
   *
   * @param txn the calling transaction
   * @param slot the tuple slot to read
   * @param out_buffer output buffer. The object should already contain projection list information. @see ProjectedRow.
   * @return true if tuple is visible to this txn and ProjectedRow has been populated, false otherwise
   */
  bool Select(transaction::TransactionContext *const txn, const TupleSlot slot, ProjectedRow *const out_buffer) const {
    return table_.data_table_->Select(txn, slot, out_buffer);
  }

  /**
   * Update the tuple according to the redo buffer given. StageWrite must have been called as well in order for the
   * operation to be logged.
   *
   * @param txn the calling transaction
   * @param redo the desired change to be applied. This should be the after-image of the attributes of interest. The
   * TupleSlot in this RedoRecord must be set to the intended tuple.
   * @return true if successful, false otherwise
   */
  bool Update(transaction::TransactionContext *const txn, RedoRecord *const redo) const {
    TERRIER_ASSERT(redo->GetTupleSlot() != TupleSlot(nullptr, 0), "TupleSlot was never set in this RedoRecord.");
    TERRIER_ASSERT(redo == reinterpret_cast<LogRecord *>(txn->redo_buffer_.LastRecord())
                               ->LogRecord::GetUnderlyingRecordBodyAs<RedoRecord>(),
                   "This RedoRecord is not the most recent entry in the txn's RedoBuffer. Was StageWrite called "
                   "immediately before?");
    const auto result = table_.data_table_->Update(txn, redo->GetTupleSlot(), *(redo->Delta()));
    if (!result) {
      // For MVCC correctness, this txn must now abort for the GC to clean up the version chain in the DataTable
      // correctly.
      txn->MustAbort();
    }
    return result;
  }

  /**
   * Inserts a tuple, as given in the redo, and return the slot allocated for the tuple. StageWrite must have been
   * called as well in order for the operation to be logged.
   *
   * @param txn the calling transaction
   * @param redo after-image of the inserted tuple.
   * @return TupleSlot for the inserted tuple
   */
  TupleSlot Insert(transaction::TransactionContext *const txn, RedoRecord *const redo) const {
    TERRIER_ASSERT(redo->GetTupleSlot() == TupleSlot(nullptr, 0), "TupleSlot was set in this RedoRecord.");
    TERRIER_ASSERT(redo == reinterpret_cast<LogRecord *>(txn->redo_buffer_.LastRecord())
                               ->LogRecord::GetUnderlyingRecordBodyAs<RedoRecord>(),
                   "This RedoRecord is not the most recent entry in the txn's RedoBuffer. Was StageWrite called "
                   "immediately before?");
    const auto slot = table_.data_table_->Insert(txn, *(redo->Delta()));
    redo->SetTupleSlot(slot);
    return slot;
  }

  /**
   * Deletes the given TupleSlot. StageDelete must have been called as well in order for the operation to be logged.
   * @param txn the calling transaction
   * @param slot the slot of the tuple to delete
   * @return true if successful, false otherwise
   */
  bool Delete(transaction::TransactionContext *const txn, const TupleSlot slot) {
    TERRIER_ASSERT(txn->redo_buffer_.LastRecord() != nullptr,
                   "The RedoBuffer is empty even though StageDelete should have been called.");
    TERRIER_ASSERT(
        reinterpret_cast<LogRecord *>(txn->redo_buffer_.LastRecord())
                ->GetUnderlyingRecordBodyAs<DeleteRecord>()
                ->GetTupleSlot() == slot,
        "This Delete is not the most recent entry in the txn's RedoBuffer. Was StageDelete called immediately before?");

    const auto result = table_.data_table_->Delete(txn, slot);
    if (!result) {
      // For MVCC correctness, this txn must now abort for the GC to clean up the version chain in the DataTable
      // correctly.
      txn->MustAbort();
    }
    return result;
  }

  /**
   * Sequentially scans the table starting from the given iterator(inclusive) and materializes as many tuples as would
   * fit into the given buffer, as visible to the transaction given, according to the format described by the given
   * output buffer. The tuples materialized are guaranteed to be visible and valid, and the function makes best effort
   * to fill the buffer, unless there are no more tuples. The given iterator is mutated to point to one slot past the
   * last slot scanned in the invocation.
   *
   * @param txn the calling transaction
   * @param start_pos iterator to the starting location for the sequential scan
   * @param out_buffer output buffer. The object should already contain projection list information. This buffer is
   *                   always cleared of old values.
   */
  void Scan(transaction::TransactionContext *const txn, DataTable::SlotIterator *const start_pos,
            ProjectedColumns *const out_buffer) const {
    return table_.data_table_->Scan(txn, start_pos, out_buffer);
  }

  /**
   * @return the first tuple slot contained in the underlying DataTable
   */
  DataTable::SlotIterator begin() const { return table_.data_table_->begin(); }  // NOLINT for STL name compability

  /**
   * @return one past the last tuple slot contained in the underlying DataTable
   */
  DataTable::SlotIterator end() const { return table_.data_table_->end(); }  // NOLINT for STL name compability

  /**
   * Generates an ProjectedColumnsInitializer for the execution layer to use. This performs the translation from col_oid
   * to col_id for the Initializer's constructor so that the execution layer doesn't need to know anything about col_id.
   * @param col_oids set of col_oids to be projected
   * @param max_tuples the maximum number of tuples to store in the ProjectedColumn
   * @return initializer to create ProjectedColumns
   * @warning col_oids must be a set (no repeats)
   */
  ProjectedColumnsInitializer InitializerForProjectedColumns(const std::vector<catalog::col_oid_t> &col_oids,
                                                             const uint32_t max_tuples) const {
    TERRIER_ASSERT((std::set<catalog::col_oid_t>(col_oids.cbegin(), col_oids.cend())).size() == col_oids.size(),
                   "There should not be any duplicated in the col_ids!");
    auto col_ids = ColIdsForOids(col_oids);
    TERRIER_ASSERT(col_ids.size() == col_oids.size(),
                   "Projection should be the same number of columns as requested col_oids.");
    return ProjectedColumnsInitializer(table_.layout_, col_ids, max_tuples);
  }

  /**
   * Generates an ProjectedRowInitializer for the execution layer to use. This performs the translation from col_oid to
   * col_id for the Initializer's constructor so that the execution layer doesn't need to know anything about col_id.
   * @param col_oids set of col_oids to be projected
   * @return initializer to create ProjectedRow
   * @warning col_oids must be a set (no repeats)
   */
  ProjectedRowInitializer InitializerForProjectedRow(const std::vector<catalog::col_oid_t> &col_oids) const {
    TERRIER_ASSERT((std::set<catalog::col_oid_t>(col_oids.cbegin(), col_oids.cend())).size() == col_oids.size(),
                   "There should not be any duplicated in the col_ids!");
    auto col_ids = ColIdsForOids(col_oids);
    TERRIER_ASSERT(col_ids.size() == col_oids.size(),
                   "Projection should be the same number of columns as requested col_oids.");
    return ProjectedRowInitializer::Create(table_.layout_, col_ids);
  }

  /**
   * Generate a projection map given column oids
   * @param col_oids oids that will be scanned.
   * @return the projection map
   */
  ProjectionMap ProjectionMapForOids(const std::vector<catalog::col_oid_t> &col_oids);

 private:
  friend class RecoveryManager;  // Needs access to OID and ID mappings
  friend class terrier::RandomSqlTableTransaction;
  friend class terrier::LargeSqlTableTestObject;
  friend class RecoveryTests;

  BlockStore *const block_store_;

  // Eventually we'll support adding more tables when schema changes. For now we'll always access the one DataTable.
  DataTableVersion table_;

  /**
   * Given a set of col_oids, return a vector of corresponding col_ids to use for ProjectionInitialization
   * @param col_oids set of col_oids, they must be in the table's ColumnMap
   * @return vector of col_ids for these col_oids
   */
  std::vector<col_id_t> ColIdsForOids(const std::vector<catalog::col_oid_t> &col_oids) const;

  /**
   * @warning This function is expensive to call and should be used with caution and sparingly.
   * Returns the col oid for the given col id
   * @param col_id given col id
   * @return col oid for the provided col id
   */
  catalog::col_oid_t OidForColId(col_id_t col_id) const;
};
}  // namespace terrier::storage
