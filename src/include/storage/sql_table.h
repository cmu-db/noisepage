#pragma once
#include <list>
#include <map>
#include <set>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "common/container/concurrent_map.h"
#include "common/macros.h"
#include "loggers/storage_logger.h"
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
    InverseColumnMap inverse_column_map;
    // TODO(John): Add 'default_value_map' (dynamic) for col_oid->default_val
  };

  /**
   * Iterator for all the slots, claimed or otherwise, in the data table. This is useful for sequential scans.
   */
  class SlotIterator {
    // TODO(Yashwanth): Slot iterator currently flawed, for a scan on a certain version, it MUST begin on the latest
    // version it sees
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
      TERRIER_ASSERT(!is_end_, "Cannot increment an end iterator");
      current_it_++;
      AdvanceOnEndOfDatatable_();
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
     * Equality check.  Either both iterators are "end" or they must match
     * on both their observed version and the underlying tuple.
     * @param other other iterator to compare to
     * @return if the two iterators point to the same slotcolumn_ids
     */
    bool operator==(const SlotIterator &other) const {
      // First "is_end" check is that both are end, the second protects the iterator check through short-circuit
      return (is_end_ && other.is_end_) ||
             (is_end_ == other.is_end_ && txn_version_ == other.txn_version_ && current_it_ == other.current_it_);
    }

    /**
     * Inequality check.
     * @param other other iterator to compare to
     * @return if the two iterators are not equal
     */
    bool operator!=(const SlotIterator &other) const { return !this->operator==(other); }

    DataTable::SlotIterator *GetDataTableSlotIterator() { return &current_it_; }

   private:
    friend class SqlTable;

    SlotIterator(const common::ConcurrentMap<layout_version_t, DataTableVersion> *tables,
                 const layout_version_t txn_version, bool is_end)
        : tables_(tables), current_it_(tables_->Find(txn_version)->second.data_table->begin()) {
      txn_version_ = txn_version;
      curr_version_ = txn_version;
      is_end_ = is_end;
    }

    /**
     * Checks if DataTable::SlotIterator is at the end of the table and advances
     * the iterator to the next table or sets is_end flag as appropriate.  This
     * refactor is necessary to keep iterator in correct state when used in two
     * distinct ways:  increment called directly on this object or implicitly
     * through advancement of the DataTable iterator in SqlTable::Scan.
     */
    void AdvanceOnEndOfDatatable_() {
      TERRIER_ASSERT(curr_version_ <= txn_version_, "Current version cannot be newer than transaction");
      while (current_it_ == tables_->Find(curr_version_)->second.data_table->end()) {
        // layout_version_t is uint32_t so we need to protect against underflow.
        if (!curr_version_ == 0) {
          is_end_ = true;
          return;
        }
        curr_version_--;
        auto next_table = tables_->Find(curr_version_);
        if (next_table == tables_->CEnd()) {  // next_table does not exist (at end)
          is_end_ = true;
          break;
        }
        current_it_ = next_table->second.data_table->begin();
      }
    }

    const common::ConcurrentMap<layout_version_t, DataTableVersion> *tables_;
    layout_version_t txn_version_;
    layout_version_t curr_version_;
    DataTable::SlotIterator current_it_;
    bool is_end_;
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
    // always insert into the new DataTable
    TERRIER_ASSERT(tables_.Find(version_num) != tables_.CEnd(), "Table version must exist before insert");
    return tables_.Find(version_num)->second.data_table->Insert(txn, redo);
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
    layout_version_t old_version = slot.GetBlock()->layout_version_;
    // always delete the tuple in the old block
    return tables_.Find(old_version)->second.data_table->Delete(txn, slot);
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
  SlotIterator begin(layout_version_t txn_version) const {
    // common::SpinLatch::ScopedSpinLatch guard(&tables_latch_);
    return SlotIterator(&tables_, txn_version, false);
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
    return SlotIterator(&tables_, tables_.CBegin()->first, true);
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
    ProjectedColumnsInitializer initializer(tables_.Find(version_num)->second.layout, col_ids, max_tuples);
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
    TERRIER_ASSERT(tables_.Find(version_num) != tables_.CEnd(), "Table version must exist before insert");
    ProjectedRowInitializer initializer =
        ProjectedRowInitializer::CreateProjectedRowInitializer(tables_.Find(version_num)->second.layout, col_ids);
    auto projection_map = ProjectionMapForInitializer<ProjectedRowInitializer>(initializer, version_num);
    TERRIER_ASSERT(projection_map.size() == col_oids.size(),
                   "ProjectionMap be the same number of columns as requested col_oids.");
    return {initializer, projection_map};
  }

 private:
  BlockStore *const block_store_;
  const catalog::table_oid_t oid_;

  common::ConcurrentMap<layout_version_t, DataTableVersion> tables_;

  /**
   * Given a set of col_oids, return a vector of corresponding col_ids to use for ProjectionInitialization
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

  /**
   * Given a projected row/col translates the column id of each column to the column id of the version passed in
   * If a column doesn't exist in that version sets the column id to VERSION_POINTER_COLUMN_ID
   * @param out_buffer - projected row/col whose header to modify
   * @param curr_dt_version - schema version of the passed in projected row/col
   * @param old_dt_version - schema version that is desired
   * @param original_col_id_store - array to store the original column id's on. Should have space to fill all column_ids
   */
  template <class RowType>
  void ModifyProjectionHeaderForVersion(RowType *out_buffer, const DataTableVersion &curr_dt_version,
                                        const DataTableVersion &old_dt_version, col_id_t *original_col_id_store) const;
};
}  // namespace terrier::storage
