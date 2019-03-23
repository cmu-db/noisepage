#include "storage/sql_table.h"
#include <set>
#include <utility>
#include <vector>
#include "common/macros.h"

namespace terrier::storage {

SqlTable::SqlTable(BlockStore *const store, const catalog::Schema &schema, const catalog::table_oid_t oid)
    : block_store_(store), oid_(oid) {
  UpdateSchema(schema);
}

SqlTable::~SqlTable() {
  while (tables_.cbegin() != tables_.cend()) {
    auto pair = *(tables_.cbegin());
    delete (pair.second.data_table);  // Delete the data_table object on the heap
    tables_.erase(pair.first);
  }
}

void SqlTable::UpdateSchema(const catalog::Schema &schema) {
  STORAGE_LOG_INFO("Update schema version: {}", uint32_t(schema.GetVersion()));
  TERRIER_ASSERT(tables_.find(schema.GetVersion()) == tables_.end(), "schema versions for an SQL table must be unique");
  const auto layout_and_map = StorageUtil::BlockLayoutFromSchema(schema);
  tables_[schema.GetVersion()] = {new DataTable(block_store_, layout_and_map.first, schema.GetVersion()),
                                  layout_and_map.first, layout_and_map.second};
  STORAGE_LOG_INFO("# of versions: {}", tables_.size());
}

bool SqlTable::Select(transaction::TransactionContext *const txn, const TupleSlot slot, ProjectedRow *const out_buffer,
                      const ProjectionMap &pr_map, layout_version_t version_num) const {
  STORAGE_LOG_INFO("slot version: {}, current version: {}", !slot.GetBlock()->layout_version_, !version_num);

  layout_version_t old_version_num = slot.GetBlock()->layout_version_;
  auto curr_dt_version = tables_.at(version_num);

  TERRIER_ASSERT(out_buffer->NumColumns() <= curr_dt_version.data_table->accessor_.GetBlockLayout().NumColumns() - NUM_RESERVED_COLUMNS,
                 "The output buffer never returns the version pointer columns, so it should have "
                 "fewer attributes.");

  // The version of the current slot is the same as the version num
  if (old_version_num == version_num) {
    return tables_.at(version_num).data_table->Select(txn, slot, out_buffer);
  }

  //The slot version is not the same as the version_num
  // 1. Copy the old header (excluding bitmap)
  auto initial_header_buffer = common::AllocationUtil::AllocateAligned(out_buffer->HeaderWithoutBitmapSize());
  std::memcpy(initial_header_buffer, out_buffer, out_buffer->HeaderWithoutBitmapSize());

  // 2. For each column present in the old version, change the column id to the col id of that version
  //    For each column not present in the old version, change the column id to the sentinel value VERSION_POINTER_COLUMN_ID
  auto old_dt_version = tables_.at(old_version_num);
  std::vector<catalog::col_oid_t> col_oids;
  for (uint16_t i = 0; i < out_buffer->NumColumns(); i++) {
    TERRIER_ASSERT(out_buffer->ColumnIds()[i] != VERSION_POINTER_COLUMN_ID,
                   "Output buffer should not read the version pointer column.");
    catalog::col_oid_t col_oid = curr_dt_version.inverse_column_map.at(out_buffer->ColumnIds()[i]);
    if(old_dt_version.column_map.count(col_oid) > 0){
      out_buffer->ColumnIds()[i] = old_dt_version.column_map.at(col_oid);
    } else {
      //TODO (Yashwanth) consider renaming VERSION_POINTER_COLUMN_ID, since we're using it for more than just that now
      out_buffer->ColumnIds()[i] = VERSION_POINTER_COLUMN_ID;
    }
  }

  //3. Get the result and copy back the old header
  bool result = old_dt_version.data_table->Select(txn, slot, out_buffer);
  std::memcpy(out_buffer, initial_header_buffer, out_buffer->HeaderWithoutBitmapSize());

  delete[] initial_header_buffer;

  //TODO (Yashwanth) handle default values
  return result;
}

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
std::pair<bool, storage::TupleSlot> SqlTable::Update(transaction::TransactionContext *const txn, const TupleSlot slot,
                                                     const ProjectedRow &redo, const ProjectionMap &map,
                                                     layout_version_t version_num) {
  // TODO(Matt): check constraints? Discuss if that happens in execution layer or not
  // TODO(Matt): update indexes
  STORAGE_LOG_INFO("Update slot version : {}, current version: {}", !slot.GetBlock()->layout_version_, !version_num);

  layout_version_t old_version = slot.GetBlock()->layout_version_;

  // The version of the current slot is the same as the version num
  if (old_version == version_num) {
    return std::make_pair(tables_.at(version_num).data_table->Update(txn, slot, redo), slot);
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
    if (tables_.at(old_version).column_map.count(it.first) == 0) {
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
    StorageUtil::CopyProjectionIntoProjection(redo, map, tables_.at(version_num).layout, pr, old_pair.second);

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
    for (auto &it : tables_.at(old_version).column_map) {
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
    for (auto &it : tables_.at(version_num).column_map) new_col_oids.emplace_back(it.first);
    auto new_pair = InitializerForProjectedRow(new_col_oids, version_num);
    auto new_buffer = common::AllocationUtil::AllocateAligned(new_pair.first.ProjectedRowSize());
    ProjectedRow *new_pr = new_pair.first.InitializeRow(new_buffer);
    StorageUtil::CopyProjectionIntoProjection(*old_pr, old_pair.second, tables_.at(old_version).layout, new_pr,
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

void SqlTable::Scan(transaction::TransactionContext *const txn, SqlTable::SlotIterator *const start_pos,
                    ProjectedColumns *const out_buffer, const ProjectionMap &pr_map,
                    layout_version_t version_num) const {
  uint32_t max_tuples = out_buffer->MaxTuples();
  layout_version_t start_version = start_pos->operator*().GetBlock()->layout_version_;

  uint32_t total_filled = 0;
  // For each DataTable
  for (auto iter : tables_) {
    if (start_version > iter.first) continue;

    DataTableVersion dt_ver = iter.second;

    // Scan the DataTable
    std::vector<catalog::col_oid_t> all_col_oids;
    for (auto &it : dt_ver.column_map) {
      all_col_oids.emplace_back(it.first);
    }
    auto pair =
        InitializerForProjectedColumns(all_col_oids, max_tuples, layout_version_t(static_cast<uint32_t>(iter.first)));
    auto pr_buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedColumnsSize());
    storage::ProjectedColumns *read = pair.first.Initialize(pr_buffer);

    if (start_version == iter.first) {
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
      ProjectedColumns::RowView to = out_buffer->InterpretAsRow(tables_.at(version_num).layout, total_filled);
      StorageUtil::CopyProjectionIntoProjection(from, pair.second, dt_ver.layout, &to, pr_map);
      filled++;
      total_filled++;
    }
    delete[] pr_buffer;
  }
  out_buffer->SetNumTuples(total_filled);
}

std::vector<col_id_t> SqlTable::ColIdsForOids(const std::vector<catalog::col_oid_t> &col_oids,
                                              layout_version_t version) const {
  TERRIER_ASSERT(!col_oids.empty(), "Should be used to access at least one column.");
  std::vector<col_id_t> col_ids;

  // Build the input to the initializer constructor
  for (const catalog::col_oid_t col_oid : col_oids) {
    TERRIER_ASSERT(tables_.at(version).column_map.count(col_oid) > 0, "Provided col_oid does not exist in the table.");
    const col_id_t col_id = tables_.at(version).column_map.at(col_oid);
    col_ids.push_back(col_id);
  }

  return col_ids;
}

template <class ProjectionInitializerType>
ProjectionMap SqlTable::ProjectionMapForInitializer(const ProjectionInitializerType &initializer,
                                                    layout_version_t version) const {
  ProjectionMap projection_map;
  // for every attribute in the initializer
  for (uint16_t i = 0; i < initializer.NumColumns(); i++) {
    // extract the underlying col_id it refers to
    const col_id_t col_id_at_offset = initializer.ColId(i);
    // find the key (col_oid) in the table's map corresponding to the value (col_id)
    const auto oid_to_id =
        std::find_if(tables_.at(version).column_map.cbegin(), tables_.at(version).column_map.cend(),
                     [&](const auto &oid_to_id) -> bool { return oid_to_id.second == col_id_at_offset; });
    // insert the mapping from col_oid to projection offset
    projection_map[oid_to_id->first] = i;
  }

  return projection_map;
}

template ProjectionMap SqlTable::ProjectionMapForInitializer<ProjectedColumnsInitializer>(
    const ProjectedColumnsInitializer &initializer, layout_version_t version) const;
template ProjectionMap SqlTable::ProjectionMapForInitializer<ProjectedRowInitializer>(
    const ProjectedRowInitializer &initializer, layout_version_t version) const;

}  // namespace terrier::storage
