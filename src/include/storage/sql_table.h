#pragma once
#include <list>
#include <set>
#include <string>
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
    const BlockLayout layout_;
    ColumnOidToIdMap column_oid_to_id_map_;
    // TODO(Ling): used in transforming between different versions.
    //  It only works for adding and dropping columns, but not modifying type/constraint/default of the column
    //  Consider storing forward and backward delta of the schema change maybe in the future
    ColumnIdToOidMap column_id_to_oid_map_;
    common::ManagedPointer<const catalog::Schema> schema_;
    DefaultValueMap default_value_map_;
  };

 public:
  /**
   * Constructs a new SqlTable with the given Schema, using the given BlockStore as the source
   * of its storage blocks.
   *
   * @param store the Block store to use.
   * @param schema the initial Schema of this SqlTable
   */
  SqlTable(common::ManagedPointer<BlockStore> store, const catalog::Schema &schema);

  /**
   * Destructs a SqlTable, frees all its members.
   */
  ~SqlTable() {
    for (const auto &it : tables_) delete it.second.data_table_;
  }

  /**
   * Materializes a single tuple from the given slot, as visible at the timestamp of the calling txn.
   *
   * @param txn the calling transaction
   * @param slot the tuple slot to read
   * @param out_buffer output buffer. The object should already contain projection list information. @see ProjectedRow.
   * @return true if tuple is visible to this txn and ProjectedRow has been populated, false otherwise
   */
  bool Select(common::ManagedPointer<transaction::TransactionContext> txn, layout_version_t layout_version,
              TupleSlot slot, ProjectedRow *out_buffer) const;

  /**
   * Update the tuple according to the redo buffer given. StageWrite must have been called as well in order for the
   * operation to be logged.
   *
   * @param txn the calling transaction
   * @param redo the desired change to be applied. This should be the after-image of the attributes of interest. The
   * TupleSlot in this RedoRecord must be set to the intended tuple.
   * @return true if successful, false otherwise
   */
  bool Update(common::ManagedPointer<transaction::TransactionContext> txn, layout_version_t layout_version,
              RedoRecord *redo) const;

  /**
   * Inserts a tuple, as given in the redo, and return the slot allocated for the tuple. StageWrite must have been
   * called as well in order for the operation to be logged.
   *
   * @param txn the calling transaction
   * @param redo after-image of the inserted tuple.
   * @param layout_version schema layout version for the inserted tuple
   * @return TupleSlot for the inserted tuple
   */
  TupleSlot Insert(common::ManagedPointer<transaction::TransactionContext> txn, layout_version_t layout_version,
                   RedoRecord *redo) const;

  /**
   * Deletes the given TupleSlot. StageDelete must have been called as well in order for the operation to be logged.
   * @param txn the calling transaction
   * @param slot the slot of the tuple to delete
   * @return true if successful, false otherwise
   */
  bool Delete(const common::ManagedPointer<transaction::TransactionContext> txn, layout_version_t layout_version,
              const TupleSlot slot) {
    TERRIER_ASSERT(txn->redo_buffer_.LastRecord() != nullptr,
                   "The RedoBuffer is empty even though StageDelete should have been called.");
    TERRIER_ASSERT(
        reinterpret_cast<LogRecord *>(txn->redo_buffer_.LastRecord())
                ->GetUnderlyingRecordBodyAs<DeleteRecord>()
                ->GetTupleSlot() == slot,
        "This Delete is not the most recent entry in the txn's RedoBuffer. Was StageDelete called immediately before?");

    const auto tuple_version = slot.GetBlock()->data_table_->layout_version_;
    TERRIER_ASSERT(tables_.find(tuple_version) != tables_.end(), "we are not deleting any layout version for now");
    const auto result = tables_.at(tuple_version).data_table_->Delete(txn, slot);

    if (!result) {
      // For MVCC correctness, this txn must now abort for the GC to clean up the version chain in the DataTable
      // correctly.
      txn->SetMustAbort();
    }
    return result;
  }

  /**
   * Sequentially scans the table starting from the given iterator(inclusive) and materializes as many tuples as would
   * fit into the given buffer, as visible to the transaction given, according to the format described by the given
   * output buffer. The tuples materialized are guaranteed to be visible and valid, and the function makes best effort
   * to fill the buffer, unless there are no more tuples. The given iterator is mutated to point to one slot past the
   * last slot scanned in the invocation.
   * @warning a tuple inserted early than start_pos might appear in the scan (by migrating into a later version schema
   * datatable so the ordering of the scanning is not strictly defined here. However, if start_pos is obtained through
   * SqlTable::begin(), the results will always contain the same tuples
   * @param txn the calling transaction
   * @param start_pos iterator to the starting location for the sequential scan.
   * @param out_buffer output buffer. The object should already contain projection list information. This buffer is
   *                   always cleared of old values.
   */
  void Scan(const common::ManagedPointer<transaction::TransactionContext> txn, DataTable::SlotIterator *const start_pos,
            ProjectedColumns *const out_buffer, const layout_version_t layout_version) const;

  /**
   * Creates a new tableversion given a schema. Conccurent UpdateSchema is synchronized at the Catalog table.
   * Since the catalog table prevents write-write conflict with version pointer, calling UpdateSchema here is always
   * thread-safe.
   * @param txn
   * @param layout_version
   * @param schema
   */
  void UpdateSchema(const common::ManagedPointer<transaction::TransactionContext> txn,
                    const layout_version_t layout_version, const catalog::Schema &schema) {
    TERRIER_ASSERT(tables_.lower_bound(version) == tables_.end(),
                   "input version should be strictly larger than all versions");
    auto table = CreateTable(common::ManagedPointer<const catalog::Schema>(&schema), block_store_, version);
    const auto [_it, success] = tables_.insert({version, table});
    TERRIER_ASSERT(success, "inserting new tableversion should not fail");
  }

  // TODO(Schema-Change): Do we retain the begin() and end(), or implement begin and end function with version number?

  /**
   * @return the first tuple slot contained in the underlying DataTable
   */
  DataTable::SlotIterator begin() const { return table_.data_table_->begin(); }  // NOLINT for STL name compability

  /**
   * @return one past the last tuple slot contained in the underlying DataTable
   */
  DataTable::SlotIterator end() const { return table_.data_table_->end(); }  // NOLINT for STL name compability

  /**
   * @param: layout_version the last version I should be able to see
   * @return one past the last tuple slot contained in the underlying DataTable
   */
  DataTable::SlotIterator end(layout_version_t layout_version) const {
    return table_.tables_.at[layout_version]->end();
  }  // NOLINT for STL name compability

  // TODO(Schema-Change): add projection considering table
  //  We might have to seperate the use cases here: one implementation that does not expect schema chnage at all, one
  //  does. In many cases, this function is called not in transactional context (thus layout_version not really
  //  relevant). For example, in TPCC, the worker will pre-allocate a buffer with size equal to the projectedrow's size.
  //  For the version that does expect a version change: we can save the col_oids and the reference to the SqlTable in
  //  the ProjRow(Colum)Initlizer, and only later when a transactional context is known, we materialize this initializer
  //  with the correct layout_version
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
  ProjectedRowInitializer InitializerForProjectedRow(const std::vector<catalog::col_oid_t> &col_oids,
                                                     layout_version_t layout_version) const {
    // TODO(Schema-Change): this function is called for calculating the optimal layout of the columns
    //  (so they are not stored in logical order).
    //  This should not be using the inside data, but only the layout/column information.
    //  Therefore it should return the initializer of the current intended datatable version
    TERRIER_ASSERT((std::set<catalog::col_oid_t>(col_oids.cbegin(), col_oids.cend())).size() == col_oids.size(),
                   "There should not be any duplicated in the col_ids!");
    auto col_ids = ColIdsForOids(col_oids, layout_version);
    TERRIER_ASSERT(col_ids.size() == col_oids.size(),
                   "Projection should be the same number of columns as requested col_oids.");
    return ProjectedRowInitializer::Create(tables_.at(layout_version).layout_, col_ids);
  }

  /**
   * Generate a projection map given column oids
   * @param col_oids oids that will be scanned.
   * @return the projection map
   */
  ProjectionMap ProjectionMapForOids(const std::vector<catalog::col_oid_t> &col_oids, layout_version_t layout_version);

 private:
  friend class RecoveryManager;  // Needs access to OID and ID mappings
  friend class terrier::RandomSqlTableTransaction;
  friend class terrier::LargeSqlTableTestObject;
  friend class RecoveryTests;

  const common::ManagedPointer<BlockStore>
      block_store_;  // TODO(Matt): do we need this stashed at this layer? We don't use it.

  // Eventually we'll support adding more tables when schema changes. For now we'll always access the one DataTable.
  // TODO(Schema-Change): add concurrent access support. Implement single threaded version first
  // Used orderred map for traversing data table that are less or equal to curr version
  std::map<layout_version_t, DataTableVersion> tables_;

  // return if there is any columns touched in new schema but not old schema
  bool AlignHeaderToVersion(ProjectedRow *out_buffer, const DataTableVersion &tuple_version,
                            const DataTableVersion &desired_version, col_id_t *cached_ori_header) const;

  void FillMissingColumns(ProjectedRow *out_buffer, const DataTableVersion &desired_version) const;

  /**
   * Creates a new datatble version given the schema and version number
   * @param schema
   * @param store
   * @param version
   * @return DataTableVersion
   */
  DataTableVersion CreateTable(common::ManagedPointer<const catalog::Schema> schema,
                               common::ManagedPointer<BlockStore> store, layout_version_t version);

  /**
   * Given a set of col_oids, return a vector of corresponding col_ids to use for ProjectionInitialization
   * @param col_oids set of col_oids, they must be in the table's ColumnMap
   * @return vector of col_ids for these col_oids
   */
  std::vector<col_id_t> ColIdsForOids(const std::vector<catalog::col_oid_t> &col_oids,
                                      layout_version_t layout_version) const;

  /**
   * @warning This function is expensive to call and should be used with caution and sparingly.
   * Returns the col oid for the given col id
   * @param col_id given col id
   * @return col oid for the provided col id
   */
  catalog::col_oid_t OidForColId(col_id_t col_id, layout_version_t layout_version) const;
};
}  // namespace terrier::storage
