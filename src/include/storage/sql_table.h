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
    DataTable *data_table;
    BlockLayout layout;
    ColumnMap column_map;
  };

 public:
  /**
   * Constructs a new SqlTable with the given Schema, using the given BlockStore as the source
   * of its storage blocks.
   *
   * @param store the Block store to use.
   * @param schema the initial Schema of this SqlTable
   * @param oid unique identifier for this SqlTable
   */
  SqlTable(BlockStore *store, const catalog::Schema &schema, catalog::table_oid_t oid);

  /**
   * Destructs a SqlTable, frees all its members.
   */
  ~SqlTable() { delete table_.data_table; }

  /**
   * Get the schema use of the SQL table
   * @return the schema
   */
  catalog::Schema &GetSchema() { return schema_; }

  /**
   * Materializes a single tuple from the given slot, as visible at the timestamp of the calling txn.
   *
   * @param txn the calling transaction
   * @param slot the tuple slot to read
   * @param out_buffer output buffer. The object should already contain projection list information. @see ProjectedRow.
   * @return true if tuple is visible to this txn and ProjectedRow has been populated, false otherwise
   */
  bool Select(transaction::TransactionContext *const txn, const TupleSlot slot, ProjectedRow *const out_buffer) const {
    return table_.data_table->Select(txn, slot, out_buffer);
  }

  /**
   * Update the tuple according to the redo buffer given.
   *
   * @param txn the calling transaction
   * @param slot the slot of the tuple to update.
   * @param redo the desired change to be applied. This should be the after-image of the attributes of interest.
   * @return true if successful, false otherwise
   */
  bool Update(transaction::TransactionContext *const txn, const TupleSlot slot, const ProjectedRow &redo) const {
    return table_.data_table->Update(txn, slot, redo);
  }

  /**
   * Inserts a tuple, as given in the redo, and return the slot allocated for the tuple.
   *
   * @param txn the calling transaction
   * @param redo after-image of the inserted tuple.
   * @return the TupleSlot allocated for this insert, used to identify this tuple's physical location for indexes and
   * such.
   */
  TupleSlot Insert(transaction::TransactionContext *const txn, const ProjectedRow &redo) const {
    return table_.data_table->Insert(txn, redo);
  }

  /**
   * Deletes the given TupleSlot, this will call StageWrite on the provided txn to generate the RedoRecord for delete.
   * @param txn the calling transaction
   * @param slot the slot of the tuple to delete
   * @return true if successful, false otherwise
   */
  bool Delete(transaction::TransactionContext *const txn, const TupleSlot slot) {
    return table_.data_table->Delete(txn, slot);
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
    return table_.data_table->Scan(txn, start_pos, out_buffer);
  }

  /**
   * @return table's unique identifier
   */
  catalog::table_oid_t Oid() const { return oid_; }

  /**
   * @return the first tuple slot contained in the underlying DataTable
   */
  DataTable::SlotIterator begin() const { return table_.data_table->begin(); }

  /**
   * @return one past the last tuple slot contained in the underlying DataTable
   */
  DataTable::SlotIterator end() const { return table_.data_table->end(); }

  /**
   * Generates an ProjectedColumnsInitializer for the execution layer to use. This performs the translation from col_oid
   * to col_id for the Initializer's constructor so that the execution layer doesn't need to know anything about col_id.
   * @param col_oids set of col_oids to be projected
   * @param max_tuples the maximum number of tuples to store in the ProjectedColumn
   * @return pair of: initializer to create ProjectedColumns, and a mapping between col_oid and the offset within the
   * ProjectedColumn
   * @warning col_oids must be a set (no repeats)
   */
  std::pair<ProjectedColumnsInitializer, ProjectionMap> InitializerForProjectedColumns(
      const std::vector<catalog::col_oid_t> &col_oids, const uint32_t max_tuples) const {
    TERRIER_ASSERT((std::set<catalog::col_oid_t>(col_oids.cbegin(), col_oids.cend())).size() == col_oids.size(),
                   "There should not be any duplicated in the col_ids!");
    auto col_ids = ColIdsForOids(col_oids);
    TERRIER_ASSERT(col_ids.size() == col_oids.size(),
                   "Projection should be the same number of columns as requested col_oids.");
    ProjectedColumnsInitializer initializer(table_.layout, col_ids, max_tuples);
    auto projection_map = ProjectionMapForInitializer<ProjectedColumnsInitializer>(initializer);
    TERRIER_ASSERT(projection_map.size() == col_oids.size(),
                   "ProjectionMap be the same number of columns as requested col_oids.");
    return {initializer, projection_map};
  }

  /**
   * Generates an ProjectedRowInitializer for the execution layer to use. This performs the translation from col_oid to
   * col_id for the Initializer's constructor so that the execution layer doesn't need to know anything about col_id.
   * @param col_oids set of col_oids to be projected
   * @return pair of: initializer to create ProjectedRow, and a mapping between col_oid and the offset within the
   * ProjectedRow to create ProjectedColumns, and a mapping between col_oid and the offset within the
   * ProjectedColumn
   * @warning col_oids must be a set (no repeats)
   */
  std::pair<ProjectedRowInitializer, ProjectionMap> InitializerForProjectedRow(
      const std::vector<catalog::col_oid_t> &col_oids) const {
    TERRIER_ASSERT((std::set<catalog::col_oid_t>(col_oids.cbegin(), col_oids.cend())).size() == col_oids.size(),
                   "There should not be any duplicated in the col_ids!");
    auto col_ids = ColIdsForOids(col_oids);
    TERRIER_ASSERT(col_ids.size() == col_oids.size(),
                   "Projection should be the same number of columns as requested col_oids.");
    ProjectedRowInitializer initializer =
        ProjectedRowInitializer::CreateProjectedRowInitializer(table_.layout, col_ids);
    auto projection_map = ProjectionMapForInitializer<ProjectedRowInitializer>(initializer);
    TERRIER_ASSERT(projection_map.size() == col_oids.size(),
                   "ProjectionMap be the same number of columns as requested col_oids.");
    return {initializer, projection_map};
  }

 private:
  BlockStore *const block_store_;
  const catalog::table_oid_t oid_;

  catalog::Schema schema_;

  // Eventually we'll support adding more tables when schema changes. For now we'll always access the one DataTable.
  DataTableVersion table_;

  /**
   * Given a set of col_oids, return a vector of corresponding col_ids to use for ProjectionInitialization
   * @param col_oids set of col_oids, they must be in the table's ColumnMap
   * @return vector of col_ids for these col_oids
   */
  std::vector<col_id_t> ColIdsForOids(const std::vector<catalog::col_oid_t> &col_oids) const;

  /**
   * Given a ProjectionInitializer, returns a map between col_oid and the offset within the projection to access that
   * column
   * @tparam ProjectionInitializerType ProjectedRowInitializer or ProjectedColumnsInitializer
   * @param initializer the initializer to generate a map for
   * @return the projection map for this initializer
   */
  template <class ProjectionInitializerType>
  ProjectionMap ProjectionMapForInitializer(const ProjectionInitializerType &initializer) const;
};
}  // namespace terrier::storage
