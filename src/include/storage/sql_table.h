#pragma once
#include <list>
#include <map>
#include <set>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "loggers/storage_logger.h"
#include "storage/data_table.h"
#include "storage/projected_columns.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace terrier::storage {

/**
 * A SqlTable is a thin layer above DataTable that replaces storage layer concepts like BlockLayout with SQL layer
 * concepts like Schema. This layer will also handle index maintenance, and possibly constraint checking (confirm when
 * we bring in execution layer). The goal is to hide concepts like col_id_t and BlockLayout above the SqlTable level.
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
    InverseColumnMap inverse_column_map;
    // TODO(John): Add 'default_value_map' (dynamic) for col_oid->default_val
  };

  /**
   * Iterator for all the slots, claimed or otherwise, in the data table. This is useful for sequential scans.
   */
  class SlotIterator {
   public:
    /**
     * @return reference to the underlying tuple slot
     */
    const TupleSlot &operator*() const { return *current_it_; }

    /**
     * @return pointer to the underlying tuple slot
     */
    const TupleSlot *operator->() const { return &(*current_it_); }

    /**
     * pre-fix increment.
     * @return self-reference after the iterator is advanced
     */
    SlotIterator &operator++() {
      current_it_++;
      if (current_it_ == dt_version_->second.data_table->end()) {
        dt_version_++;
        current_it_ = dt_version_->second.data_table->begin();
      }
      return *this;
    }

    /**
     * post-fix increment.
     * @return copy of the iterator equal to this before increment
     */
    const SlotIterator operator++(int) {
      SlotIterator copy = *this;
      operator++();
      return copy;
    }

    /**
     * Equality check.
     * @param other other iterator to compare to
     * @return if the two iterators point to the same slot
     */
    bool operator==(const SlotIterator &other) const { return current_it_ == other.current_it_; }

    /**
     * Inequality check.
     * @param other other iterator to compare to
     * @return if the two iterators are not equal
     */
    bool operator!=(const SlotIterator &other) const { return !this->operator==(other); }

    DataTable::SlotIterator GetDataTableSlotIterator() { return current_it_; }

   private:
    friend class SqlTable;
    /**
     * @warning MUST BE CALLED ONLY WHEN CALLER HOLDS LOCK TO THE LIST OF RAW BLOCKS IN THE DATA TABLE
     */
    SlotIterator(std::map<layout_version_t, DataTableVersion>::const_iterator dt_version,
                 DataTable::SlotIterator dt_slot_it)
        : dt_version_(dt_version), current_it_(dt_slot_it) {}

    std::map<layout_version_t, DataTableVersion>::const_iterator dt_version_;
    DataTable::SlotIterator current_it_;
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
  ~SqlTable();

  /**
   * Adds the new schema to set of active data tables.  This functions should
   * only be called upon commit because there is no mechanism to rollback an
   * abort (which shouldn't be needed).
   *
   * @param schema the new Schema for the SqlTable (version must be unique)
   */
  void UpdateSchema(const catalog::Schema &schema);

  /**
   * Materializes a single tuple from the given slot, as visible at the timestamp of the calling txn.
   *
   * @param txn the calling transaction
   * @param slot the tuple slot to read
   * @param out_buffer output buffer. The object should already contain projection list information. @see ProjectedRow.
   * @param pr_map the ProjectionMap of the out_buffer
   * @param version_num the schema version which the transaction sees
   * @return true if tuple is visible to this txn and ProjectedRow has been populated, false otherwise
   */
  bool Select(transaction::TransactionContext *txn, TupleSlot slot, ProjectedRow *out_buffer,
              const ProjectionMap &pr_map, layout_version_t version_num) const;

  /**
   * Update the tuple according to the redo buffer given.
   *
   * @param txn the calling transaction
   * @param slot the slot of the tuple to update.
   * @param redo the desired change to be applied. This should be the after-image of the attributes of interest.
   * @param map the ProjectionMap of the ProjectedRow
   * @param version_num the schema version which the transaction sees
   * @return true if successful, false otherwise; If the update changed the location of the TupleSlot, a new TupleSlot
   * is returned. Otherwise, the same TupleSlot is returned.
   */
  std::pair<bool, storage::TupleSlot> Update(transaction::TransactionContext *txn, TupleSlot slot,
                                             const ProjectedRow &redo, const ProjectionMap &map,
                                             layout_version_t version_num);

  /**
   * Inserts a tuple, as given in the redo, and return the slot allocated for the tuple.
   *
   * @param txn the calling transaction
   * @param redo after-image of the inserted tuple.
   * @param version_num the schema version which the transaction sees
   * @return the TupleSlot allocated for this insert, used to identify this tuple's physical location for indexes and
   * such.
   */
  TupleSlot Insert(transaction::TransactionContext *const txn, const ProjectedRow &redo,
                   layout_version_t version_num) const {
    // TODO(Matt): check constraints? Discuss if that happens in execution layer or not
    // TODO(Matt): update indexes
    // always insert into the new DataTable
    return tables_.at(version_num).data_table->Insert(txn, redo);
  }

  /**
   * Deletes the given TupleSlot, this will call StageWrite on the provided txn to generate the RedoRecord for delete.
   * @param txn the calling transaction
   * @param slot the slot of the tuple to delete
   * @param version_num the schema version which the transaction sees
   * @return true if successful, false otherwise
   */
  bool Delete(transaction::TransactionContext *const txn, const TupleSlot slot, layout_version_t version_num) const {
    // TODO(Matt): check constraints? Discuss if that happens in execution layer or not
    // TODO(Matt): update indexes
    layout_version_t old_version = slot.GetBlock()->layout_version_;
    // always delete the tuple in the old block
    return tables_.at(old_version).data_table->Delete(txn, slot);
  }

  /**
   * Sequentially scans the table starting from the given iterator(inclusive) and materializes as many tuples as would
   * fit into the given buffer, as visible to the transaction given, according to the format described by the given
   * output buffer. The tuples materialized are guaranteed to be visible and valid, and the function makes best effort
   * to fill the buffer, unless there are no more tuples. The given iterator is mutated to point to one slot passed the
   * last slot scanned in the invocation.
   *
   * @param txn the calling transaction
   * @param start_pos iterator to the starting location for the sequential scan
   * @param out_buffer output buffer. The object should already contain projection list information. This buffer is
   *                   always cleared of old values.
   * @param pr_map the ProjectionMap for the Projected Columns
   * @param version_num the schema version which the transaction sees
   */
  void Scan(transaction::TransactionContext *txn, SqlTable::SlotIterator *start_pos, ProjectedColumns *out_buffer,
            const ProjectionMap &pr_map, layout_version_t version_num) const;

  /**
   * @return the first tuple slot contained in the data table
   */
  SlotIterator begin() const {
    // common::SpinLatch::ScopedSpinLatch guard(&tables_latch_);
    return SlotIterator(tables_.begin(), tables_.begin()->second.data_table->begin());
  }

  /**
   * Returns one past the last tuple slot contained in the last data table. Note that this is not an accurate number
   * when concurrent accesses are happening, as inserts maybe in flight. However, the number given is always
   * transactionally correct, as any inserts that might have happened is not going to be visible to the calling
   * transaction.
   *
   * @return one past the last tuple slot contained in the data table.
   */
  SlotIterator end() const {
    // common::SpinLatch::ScopedSpinLatch guard(&tables_latch_);
    return SlotIterator(--tables_.end(), (--tables_.end())->second.data_table->end());
  }

  /**
   * @return table's unique identifier
   */
  catalog::table_oid_t Oid() const { return oid_; }

  /**
   * Generates an ProjectedColumnsInitializer for the execution layer to use. This performs the translation from col_oid
   * to col_id for the Initializer's constructor so that the execution layer doesn't need to know anything about col_id.
   * @param col_oids set of col_oids to be projected
   * @param max_tuples the maximum number of tuples to store in the ProjectedColumn
   * @param version_num the schema version
   * @return pair of: initializer to create ProjectedColumns, and a mapping between col_oid and the offset within the
   * ProjectedColumn
   * @warning col_oids must be a set (no repeats)
   */
  std::pair<ProjectedColumnsInitializer, ProjectionMap> InitializerForProjectedColumns(
      const std::vector<catalog::col_oid_t> &col_oids, const uint32_t max_tuples, layout_version_t version_num) const {
    TERRIER_ASSERT((std::set<catalog::col_oid_t>(col_oids.cbegin(), col_oids.cend())).size() == col_oids.size(),
                   "There should not be any duplicated in the col_ids!");
    auto col_ids = ColIdsForOids(col_oids, version_num);
    TERRIER_ASSERT(col_ids.size() == col_oids.size(),
                   "Projection should be the same number of columns as requested col_oids.");
    ProjectedColumnsInitializer initializer(tables_.at(version_num).layout, col_ids, max_tuples);
    auto projection_map = ProjectionMapForInitializer<ProjectedColumnsInitializer>(initializer, version_num);
    TERRIER_ASSERT(projection_map.size() == col_oids.size(),
                   "ProjectionMap be the same number of columns as requested col_oids.");
    return {initializer, projection_map};
  }

  /**
   * Generates an ProjectedRowInitializer for the execution layer to use. This performs the translation from col_oid to
   * col_id for the Initializer's constructor so that the execution layer doesn't need to know anything about col_id.
   * @param col_oids set of col_oids to be projected
   * @param version_num the schema version
   * @return pair of: initializer to create ProjectedRow, and a mapping between col_oid and the offset within the
   * ProjectedRow to create ProjectedColumns, and a mapping between col_oid and the offset within the
   * ProjectedColumn
   * @warning col_oids must be a set (no repeats)
   */
  std::pair<ProjectedRowInitializer, ProjectionMap> InitializerForProjectedRow(
      const std::vector<catalog::col_oid_t> &col_oids, layout_version_t version_num) const {
    TERRIER_ASSERT((std::set<catalog::col_oid_t>(col_oids.cbegin(), col_oids.cend())).size() == col_oids.size(),
                   "There should not be any duplicated in the col_ids!");
    auto col_ids = ColIdsForOids(col_oids, version_num);
    TERRIER_ASSERT(col_ids.size() == col_oids.size(),
                   "Projection should be the same number of columns as requested col_oids.");
    ProjectedRowInitializer initializer(tables_.at(version_num).layout, col_ids);
    auto projection_map = ProjectionMapForInitializer<ProjectedRowInitializer>(initializer, version_num);
    TERRIER_ASSERT(projection_map.size() == col_oids.size(),
                   "ProjectionMap be the same number of columns as requested col_oids.");
    return {initializer, projection_map};
  }

 private:
  BlockStore *const block_store_;
  const catalog::table_oid_t oid_;

  // Eventually we'll support adding more tables when schema changes. For now we'll always access the one DataTable.
  std::map<layout_version_t, DataTableVersion> tables_;

  /**
   * Given a set of col_oids, return a vector of corresponding col_ids to use for ProjectionInitialization
   * @param col_oids set of col_oids, they must be in the table's ColumnMap
   * @param version the version of DataTable
   * @return vector of col_ids for these col_oids
   */
  std::vector<col_id_t> ColIdsForOids(const std::vector<catalog::col_oid_t> &col_oids, layout_version_t version) const;

  /**
   * Given a ProjectionInitializer, returns a map between col_oid and the offset within the projection to access that
   * column
   * @tparam ProjectionInitializerType ProjectedRowInitializer or ProjectedColumnsInitializer
   * @param initializer the initializer to generate a map for
   * @return the projection map for this initializer
   */
  template <class ProjectionInitializerType>
  ProjectionMap ProjectionMapForInitializer(const ProjectionInitializerType &initializer,
                                            layout_version_t version) const;
};
}  // namespace terrier::storage
