#pragma once
#include <list>
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
      if (current_it_ == dt_version_->data_table->end()) {
        dt_version_++;
        current_it_ = dt_version_->data_table->begin();
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
     * @return if the two iterators point to the same slotcolumn_ids
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
    SlotIterator(std::vector<DataTableVersion>::const_iterator dt_version, DataTable::SlotIterator dt_slot_it)
        : dt_version_(dt_version), current_it_(dt_slot_it) {}

    std::vector<DataTableVersion>::const_iterator dt_version_;
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
  SqlTable(BlockStore *const store, const catalog::Schema &schema, const catalog::table_oid_t oid)
      : block_store_(store), oid_(oid), schema_version_(0) {
    const auto layout_and_map = StorageUtil::BlockLayoutFromSchema(schema);

    DataTableVersion first_table = {new DataTable(block_store_, layout_and_map.first, layout_version_t(0)),
                                    layout_and_map.first, layout_and_map.second};
    tables_.emplace_back(first_table);
  }

  /**
   * Destructs a SqlTable, frees all its members.
   */
  ~SqlTable() {
    for (auto &t : tables_) delete t.data_table;
  }

  /**
   * Materializes a single tuple from the given slot, as visible at the timestamp of the calling txn.
   *
   * It assumes version_num starts from 0 and never decreases.
   * @param txn the calling transaction
   * @param slot the tuple slot to read
   * @param out_buffer output buffer. The object should already contain projection list information. @see ProjectedRow.
   * @param pr_map the ProjectionMap of the out_buffer
   * @param version_num the schema version which the transaction sees
   * @return true if tuple is visible to this txn and ProjectedRow has been populated, false otherwise
   */
  bool Select(transaction::TransactionContext *const txn, const TupleSlot slot, ProjectedRow *const out_buffer,
              const ProjectionMap &pr_map, layout_version_t version_num) const {
    STORAGE_LOG_INFO("slot version : {}, current version: {}", !slot.GetBlock()->layout_version_, !version_num);

    layout_version_t old_version_num = slot.GetBlock()->layout_version_;

    // The version of the current slot is the same as the version num
    if (old_version_num == version_num) {
      return tables_[!version_num].data_table->Select(txn, slot, out_buffer);
    }


    auto old_dt_version = tables_[!old_version_num];
    // The slot version is not the same as the version_num
    // 1. Get header of requested ProjectedRow
    // 2. Modify header to expected ProjectedRow
    // 3. Reset Header

    ProjectedRowHeader requestedHeader(out_buffer);
    ProjectedRowHeader expectedHeader(out_buffer);

    uint16_t num_attrs_expected = 0;
    for (auto &it : old_dt_version.column_map) {
      if (pr_map.count(it.first) > 0){
        expectedHeader.column_ids[num_attrs_expected] = tables_[!version_num].column_map.at(it.first);
        expectedHeader.attr_value_offsets[num_attrs_expected] = out_buffer->GetAttrValueOffset(pr_map.at(it.first));
        num_attrs_expected++;
      }
    }

    expectedHeader.num_columns_ = num_attrs_expected;

    expectedHeader.setAsHeaderOf(out_buffer);
    bool result = old_dt_version.data_table->Select(txn, slot, out_buffer);
    requestedHeader.setAsHeaderOf(out_buffer);

    return result;

    //TODO(yangjuns): fill in default values for newly added attributes
  }

  /**
   * Update the tuple according to the redo buffer given.
   *
   * @param txn txn the calling transaction
   * @param slot the slot of the tuple to update.
   * @param redo the desired change to be applied. This should be the after-image of the attributes of interest.
   * @param map the ProjectionMap of the ProjectedRow
   * @param version_num the schema version which the transaction sees
   * @return true if successful, false otherwise; If the update changed the location of the TupleSlot, a new TupleSlot
   * is returned. Otherwise, the same TupleSlot is returned.
   */
  std::pair<bool, storage::TupleSlot> Update(transaction::TransactionContext *const txn, const TupleSlot slot,
                                             const ProjectedRow &redo, const ProjectionMap &map,
                                             layout_version_t version_num) {
    // TODO(Matt): check constraints? Discuss if that happens in execution layer or not
    // TODO(Matt): update indexes
    STORAGE_LOG_INFO("Update slot version : {}, current version: {}", !slot.GetBlock()->layout_version_, !version_num);

    layout_version_t old_version = slot.GetBlock()->layout_version_;

    // The version of the current slot is the same as the version num
    if (old_version == version_num) {
      return std::make_pair(tables_[!version_num].data_table->Update(txn, slot, redo), slot);
    }

    // The versions are different
    // 1. Check if we can just update the old version
    // 2. If Yes:
    //    2.a) Convert ProjectedRow into old ProjectedRow
    //    2.b) Update the old DataTable using the old ProjectedRow
    // 3. Else:
    //    3.a) Get the old row
    //    3.b) Convert it into new row
    //    3.c) Insert new row into new table
    //    3.d) Delete old row
    //    3.e) Update the new row in the new table

    // Check if the Redo's attributes are a subset of old schema so that we can update old version in place
    bool is_subset = true;

    std::vector<catalog::col_oid_t> redo_col_oids;  // the set of col oids the redo touches
    for (auto &it : map) {
      redo_col_oids.emplace_back(it.first);
      // check if the col_oid exists in the old schema
      if (tables_[!old_version].column_map.count(it.first) == 0) {
        is_subset = false;
        break;
      }
    }

    storage::TupleSlot ret_slot;
    if (is_subset) {
      // we can update in place

      // We should create a buffer of old Projected Row and update in place. We can't just
      // directly erase the data without creating a redo and update the chain.

      auto old_pair = InitializerForProjectedRow(redo_col_oids, version_num);

      // 1. Create a ProjectedRow Buffer for the old version
      byte *buffer = common::AllocationUtil::AllocateAligned(old_pair.first.ProjectedRowSize());
      storage::ProjectedRow *pr = old_pair.first.InitializeRow(buffer);

      // 2. Copy from new ProjectedRow to old ProjectedRow
      StorageUtil::CopyProjectionIntoProjection(redo, map, tables_[!version_num].layout, pr, old_pair.second);

      // 3. Update the old data-table
      auto result = Update(txn, slot, *pr, old_pair.second, old_version);

      delete[] buffer;
      ret_slot = result.second;
    } else {
      STORAGE_LOG_INFO("have to insert and delete ... ");

      // need to create a new ProjectedRow of all columns
      // 1. Get the old row
      // 2. Convert it into new row
      // 3. Insert new row into new table
      // 4. Delete old row
      // 5. Update the new row in the new table

      // 1. Get old row
      std::vector<catalog::col_oid_t> old_col_oids;  // the set of col oids of the old schema
      for (auto &it : tables_[!old_version].column_map) {
        old_col_oids.emplace_back(it.first);
      }
      auto old_pair = InitializerForProjectedRow(old_col_oids, old_version);
      auto old_buffer = common::AllocationUtil::AllocateAligned(old_pair.first.ProjectedRowSize());
      ProjectedRow *old_pr = old_pair.first.InitializeRow(old_buffer);
      bool valid = Select(txn, slot, old_pr, old_pair.second, old_version);
      if (!valid) {
        return {false, slot};
      }

      // 2. Convert it into new row
      std::vector<catalog::col_oid_t> new_col_oids;  // the set of col oids which the new schema has
      for (auto &it : tables_[!version_num].column_map) new_col_oids.emplace_back(it.first);
      auto new_pair = InitializerForProjectedRow(new_col_oids, version_num);
      auto new_buffer = common::AllocationUtil::AllocateAligned(new_pair.first.ProjectedRowSize());
      ProjectedRow *new_pr = new_pair.first.InitializeRow(new_buffer);
      StorageUtil::CopyProjectionIntoProjection(*old_pr, old_pair.second, tables_[!old_version].layout, new_pr,
                                                new_pair.second);
      // 3. Insert the row into new table
      storage::TupleSlot new_slot = Insert(txn, *new_pr, version_num);

      // 4. Delete the old row
      Delete(txn, slot, old_version);

      // 5. Update the new row
      Update(txn, new_slot, redo, new_pair.second, version_num);
      //      TERRIER_ASSERT(result_pair.second.GetBlock() == new_slot.GetBlock(),
      //                     "updating the current version should return the same TupleSlot");
      delete[] old_buffer;
      delete[] new_buffer;
      // TODO(yangjuns): Need to update indices
      ret_slot = new_slot;
    }
    return std::make_pair(true, ret_slot);
  }

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
    return tables_[!version_num].data_table->Insert(txn, redo);
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
    return tables_[!old_version].data_table->Delete(txn, slot);
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
  void Scan(transaction::TransactionContext *const txn, SqlTable::SlotIterator *const start_pos,
            ProjectedColumns *const out_buffer, const ProjectionMap &pr_map, layout_version_t version_num) const {
    uint32_t max_tuples = out_buffer->MaxTuples();
    layout_version_t start_version = start_pos->operator*().GetBlock()->layout_version_;

    uint32_t total_filled = 0;
    // For each DataTable
    for (size_t i = 0; i < tables_.size(); i++) {
      if ((!start_version) > i) continue;

      DataTableVersion dt_ver = tables_[i];

      // Scan the DataTable
      std::vector<catalog::col_oid_t> all_col_oids;
      for (auto &it : dt_ver.column_map) {
        all_col_oids.emplace_back(it.first);
      }
      auto pair = InitializerForProjectedColumns(all_col_oids, max_tuples, layout_version_t(static_cast<uint32_t>(i)));
      auto pr_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedColumnsSize());
      storage::ProjectedColumns *read = pair.first.Initialize(pr_buffer);

      if ((!start_version) == i) {
        DataTable::SlotIterator dt_slot = start_pos->GetDataTableSlotIterator();
        dt_ver.data_table->Scan(txn, &dt_slot, read);
      } else {
        DataTable::SlotIterator begin = dt_ver.data_table->begin();
        dt_ver.data_table->Scan(txn, &begin, read);
      }

      uint32_t filled = 0;
      // Copy from ProjectedColumns into ProjectedColumns of the new version
      while (filled < read->NumTuples() && total_filled < max_tuples) {
        // TODO(yangjuns): if it's the most current version, we don't have to copy. We can directly write into
        // out_buffer
        ProjectedColumns::RowView from = read->InterpretAsRow(dt_ver.layout, filled);
        ProjectedColumns::RowView to = out_buffer->InterpretAsRow(tables_[!version_num].layout, total_filled);
        StorageUtil::CopyProjectionIntoProjection(from, pair.second, dt_ver.layout, &to, pr_map);
        filled++;
        total_filled++;
      }
      delete[] pr_buffer;
    }
    out_buffer->SetNumTuples(total_filled);
  }

  /**
   * @return the first tuple slot contained in the data table
   */
  SlotIterator begin() const {
    common::SpinLatch::ScopedSpinLatch guard(&tables_latch_);
    return {tables_.begin(), tables_.begin()->data_table->begin()};
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
    common::SpinLatch::ScopedSpinLatch guard(&tables_latch_);
    return {--tables_.end(), tables_[tables_.size() - 1].data_table->end()};
  }

  /**
   * Change the schema of the SqlTable. Only one transaction is allowed to change schema at a time.
   *
   * Note:
   *    1. tables_ is a vector of DataTableVersions which grows infinitely
   *    2. version_num is used to index DataTableVersion
   *
   * @param txn the calling transaction
   * @param schema the new schema
   */
  void ChangeSchema(transaction::TransactionContext *const txn, const catalog::Schema &schema) {
    common::SpinLatch::ScopedSpinLatch guard(&tables_latch_);
    schema_version_++;
    const auto layout_and_map = StorageUtil::BlockLayoutFromSchema(schema);
    DataTableVersion new_dt_version = {
        new DataTable(block_store_, layout_and_map.first, layout_version_t(schema_version_)), layout_and_map.first,
        layout_and_map.second};
    tables_.emplace_back(new_dt_version);
    STORAGE_LOG_INFO("# of versions: {}", tables_.size());
    // TODO(yangjuns): update catalog
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
    ProjectedColumnsInitializer initializer(tables_[!version_num].layout, col_ids, max_tuples);
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
    ProjectedRowInitializer initializer(tables_[!version_num].layout, col_ids);
    auto projection_map = ProjectionMapForInitializer<ProjectedRowInitializer>(initializer, version_num);
    TERRIER_ASSERT(projection_map.size() == col_oids.size(),
                   "ProjectionMap be the same number of columns as requested col_oids.");
    return {initializer, projection_map};
  }

 private:
  BlockStore *const block_store_;
  const catalog::table_oid_t oid_;
  std::atomic<uint32_t> schema_version_;

  mutable common::SpinLatch tables_latch_;
  std::vector<DataTableVersion> tables_;

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
};
}  // namespace terrier::storage
