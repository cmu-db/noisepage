#include "storage/sql_table.h"
#include <set>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "storage/storage_util.h"

namespace terrier::storage {

SqlTable::SqlTable(BlockStore *const store, const catalog::Schema &schema, const catalog::table_oid_t oid)
    : block_store_(store), oid_(oid) {
  UpdateSchema(schema);
}

SqlTable::~SqlTable() {
  while (tables_.CBegin() != tables_.CEnd()) {
    auto pair = *(tables_.CBegin());
    delete (pair.second.data_table);  // Delete the data_table object on the heap
    tables_.UnsafeErase(pair.first);
  }
}

void SqlTable::UpdateSchema(const catalog::Schema &schema) {
  STORAGE_LOG_DEBUG("Update schema version: {}", uint32_t(schema.GetVersion()));
  TERRIER_ASSERT(tables_.Find(schema.GetVersion()) == tables_.End(), "schema versions for an SQL table must be unique");

  // Begin with the NUM_RESERVED_COLUMNS in the attr_sizes
  std::vector<uint8_t> attr_sizes;
  attr_sizes.reserve(NUM_RESERVED_COLUMNS + schema.GetColumns().size());

  for (uint8_t i = 0; i < NUM_RESERVED_COLUMNS; i++) {
    attr_sizes.emplace_back(8);
  }

  TERRIER_ASSERT(attr_sizes.size() == NUM_RESERVED_COLUMNS,
                 "attr_sizes should be initialized with NUM_RESERVED_COLUMNS elements.");

  // First pass through to accumulate the counts of each attr_size
  for (const auto &column : schema.GetColumns()) {
    attr_sizes.push_back(column.GetAttrSize());
  }

  auto offsets = storage::StorageUtil::ComputeBaseAttributeOffsets(attr_sizes, NUM_RESERVED_COLUMNS);

  ColumnMap col_map;
  InverseColumnMap inv_col_map;

  // Build the maps between Schema column OIDs and underlying column IDs
  for (const auto &column : schema.GetColumns()) {
    switch (column.GetAttrSize()) {
      case VARLEN_COLUMN:
        inv_col_map[col_id_t(offsets[0])] = column.GetOid();
        col_map[column.GetOid()] = col_id_t(offsets[0]++);
        break;
      case 8:
        inv_col_map[col_id_t(offsets[1])] = column.GetOid();
        col_map[column.GetOid()] = col_id_t(offsets[1]++);
        break;
      case 4:
        inv_col_map[col_id_t(offsets[2])] = column.GetOid();
        col_map[column.GetOid()] = col_id_t(offsets[2]++);
        break;
      case 2:
        inv_col_map[col_id_t(offsets[3])] = column.GetOid();
        col_map[column.GetOid()] = col_id_t(offsets[3]++);
        break;
      case 1:
        inv_col_map[col_id_t(offsets[4])] = column.GetOid();
        col_map[column.GetOid()] = col_id_t(offsets[4]++);
        break;
      default:
        throw std::runtime_error("unexpected switch case value");
    }
  }

  // Populate the default value map
  for (const auto &column : schema.GetColumns()) {
    auto col_oid = column.GetOid();
    byte *default_value = column.GetDefault();
    // Only populate the default values of the columns which are new and have a default value
    if (default_value_map_.Find(col_oid) == default_value_map_.End()) {
      uint8_t attr_size = column.GetAttrSize();
      default_value_map_.Insert(column.GetOid(), {default_value, attr_size});
    }
  }

  auto layout = BlockLayout(attr_sizes);

  auto dt = new DataTable(block_store_, layout, schema.GetVersion());
  // clang's memory analysis has a false positive on this allocation.  The TERRIER_ASSERT on the second line of this
  // function prevents the insert below from failing (can only fail when key is not unique).  The write-lock on the
  // catalog prevents any other transaction from being in a race condition with this one.  The corresponding delete
  // for this allocation is in the destructor for SqlTable.  clang-analyzer-cplusplus.NewDeleteLeaks identifies this
  // as a potential leak and throws an error incorrectly.
  // NOLINTNEXTLINE
  tables_.Insert(schema.GetVersion(), {dt, layout, col_map, inv_col_map});
}

bool SqlTable::Select(transaction::TransactionContext *const txn, const TupleSlot slot, ProjectedRow *const out_buffer,
                      const ProjectionMap &pr_map, layout_version_t version_num) const {
  TERRIER_ASSERT(slot.GetBlock() != nullptr, "Slot does not exist");
  STORAGE_LOG_DEBUG("slot version: {}, current version: {}", !slot.GetBlock()->layout_version_, !version_num);

  layout_version_t old_version_num = slot.GetBlock()->layout_version_;

  TERRIER_ASSERT(out_buffer->NumColumns() <= tables_.Find(version_num)->second.column_map.size(),
                 "The output buffer never returns the version pointer columns, so it should have "
                 "fewer attributes.");

  // The version of the current slot is the same as the version num
  if (old_version_num == version_num) {
    return tables_.Find(version_num)->second.data_table->Select(txn, slot, out_buffer);
  }

  auto old_dt_version = tables_.Find(old_version_num)->second;
  auto curr_dt_version = tables_.Find(version_num)->second;

  // The slot version is not the same as the version_num
  col_id_t original_column_ids[out_buffer->NumColumns()];
  ModifyProjectionHeaderForVersion(out_buffer, tables_.Find(version_num)->second, old_dt_version, original_column_ids);

  // Get the result and copy back the old header
  bool result = old_dt_version.data_table->Select(txn, slot, out_buffer);
  std::memcpy(out_buffer->ColumnIds(), original_column_ids, sizeof(col_id_t) * out_buffer->NumColumns());

  // Default values should only be filled into the columns that are missing in the old_dt_version
  // Do not fill default values for the columns which already exist in the old_dt_version

  // Calculate the col_oids of out_buffer which are missing from the old version of datatable
  std::unordered_set<catalog::col_oid_t> missing_col_oids = GetMissingColumnOidsForVersion(pr_map, old_dt_version);

  // Fill in the default values for the missing columns
  for (catalog::col_oid_t col_oid : missing_col_oids) {
    FillDefaultValue(out_buffer, col_oid, pr_map);
  }

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
                                                     ProjectedRow *redo, const ProjectionMap &map,
                                                     layout_version_t version_num) {
  // TODO(Matt): check constraints? Discuss if that happens in execution layer or not
  // TODO(Matt): update indexes
  STORAGE_LOG_DEBUG("Update slot version : {}, current version: {}", !slot.GetBlock()->layout_version_, !version_num);

  layout_version_t old_version = slot.GetBlock()->layout_version_;

  // The version of the current slot is the same as the version num
  TERRIER_ASSERT(old_version <= version_num, "Transaction should not be seeing this tuple");
  if (old_version == version_num) {
    return {tables_.Find(version_num)->second.data_table->Update(txn, slot, *redo), slot};
  }

  // The versions are different
  // 1. Check if we can just update the old version
  // 2. If Yes:
  //    2.a) Convert ProjectedRow into old ProjectedRow
  //    2.b) Update the old DataTable using the old ProjectedRow
  // 3. Else:
  //    3.a) Get the old row
  //    3.b) Convert it into new row
  //    3.c) Delete old row
  //    3.d) Update the new row before insert
  //    3.e) Insert new row into new table

  // Check if the Redo's attributes are a subset of old schema so that we can update old version in place
  bool is_subset = true;

  std::vector<catalog::col_oid_t> redo_col_oids;  // the set of col oids the redo touches
  for (auto &it : map) {
    redo_col_oids.emplace_back(it.first);
    // check if the col_oid exists in the old schema
    if (tables_.Find(old_version)->second.column_map.count(it.first) == 0) {
      is_subset = false;
      break;
    }
  }

  storage::TupleSlot ret_slot;
  if (is_subset) {
    // we can update in place
    auto old_dt_version = tables_.Find(old_version)->second;

    // The slot version is not the same as the version_num
    col_id_t original_column_ids[redo->NumColumns()];
    ModifyProjectionHeaderForVersion(redo, tables_.Find(version_num)->second, old_dt_version, original_column_ids);

    // Get the result and copy back the old header
    bool result = old_dt_version.data_table->Update(txn, slot, *redo);

    // We should create a buffer of old Projected Row and update in place. We can't just
    // directly erase the data without creating a redo and update the chain.

    // auto old_pair = InitializerForProjectedRow(redo_col_oids, old_version);

    // // 1. Create a ProjectedRow Buffer for the old version
    // byte *buffer = common::AllocationUtil::AllocateAligned(old_pair.first.ProjectedRowSize());
    // storage::ProjectedRow *pr = old_pair.first.InitializeRow(buffer);

    // // 2. Copy from new ProjectedRow to old ProjectedRow
    // StorageUtil::CopyProjectionIntoProjection(redo, map, tables_.Find(old_version)->second.layout, pr,
    // old_pair.second);

    // // 3. Update the old data-table
    // bool result = tables_.Find(old_version)->second.data_table->Update(txn, slot, *pr);
    // delete[] buffer;
    if (!result) {
      return {false, slot};
    }
    ret_slot = slot;
  } else {
    STORAGE_LOG_DEBUG("have to delete and insert ... ");

    // need to create a new ProjectedRow of all columns
    // 1. Get the old row
    // 2. Convert it into new row
    // 3. Delete old row
    // 4. Update the new row before insert
    // 5. Insert new row into new table

    // 1. Get old row
    // 2. Convert it into new row
    std::vector<catalog::col_oid_t> new_col_oids;  // the set of col oids which the new schema has
    for (auto &it : tables_.Find(version_num)->second.column_map) new_col_oids.emplace_back(it.first);
    auto new_pair = InitializerForProjectedRow(new_col_oids, version_num);
    auto new_buffer = common::AllocationUtil::AllocateAligned(new_pair.first.ProjectedRowSize());
    ProjectedRow *new_pr = new_pair.first.InitializeRow(new_buffer);
    bool valid = Select(txn, slot, new_pr, new_pair.second, version_num);
    if (!valid) {
      delete[] new_buffer;
      return {false, slot};
    }
    // 3. Delete the old row
    bool succ = tables_.Find(old_version)->second.data_table->Delete(txn, slot);

    // 4. Update the new row before insert
    StorageUtil::CopyProjectionIntoProjection(*redo, map, tables_.Find(version_num)->second.layout, new_pr,
                                              new_pair.second);

    // 5. Insert the row into new table
    storage::TupleSlot new_slot;
    if (succ) {
      new_slot = tables_.Find(version_num)->second.data_table->Insert(txn, *new_pr);
    } else {
      // someone else deleted the old row, write-write conflict
      delete[] new_buffer;
      return {false, slot};
    }

    delete[] new_buffer;

    ret_slot = new_slot;
  }
  return {true, ret_slot};
}

void SqlTable::Scan(transaction::TransactionContext *const txn, SqlTable::SlotIterator *start_pos,
                    ProjectedColumns *const out_buffer, const ProjectionMap &pr_map,
                    layout_version_t version_num) const {
  layout_version_t old_version_num = start_pos->curr_version_;

  TERRIER_ASSERT(out_buffer->NumColumns() <= tables_.Find(version_num)->second.column_map.size(),
                 "The output buffer never returns the version pointer columns, so it should have "
                 "fewer attributes.");

  DataTable::SlotIterator *dt_slot = start_pos->GetDataTableSlotIterator();

  // Check for version match
  if (old_version_num == version_num) {
    tables_.Find(version_num)->second.data_table->Scan(txn, dt_slot, out_buffer);
    start_pos->AdvanceOnEndOfDatatable_();
    return;
  }

  col_id_t original_column_ids[out_buffer->NumColumns()];
  ModifyProjectionHeaderForVersion(out_buffer, tables_.Find(version_num)->second, tables_.Find(old_version_num)->second,
                                   original_column_ids);

  tables_.Find(old_version_num)->second.data_table->Scan(txn, dt_slot, out_buffer);
  start_pos->AdvanceOnEndOfDatatable_();

  uint32_t filled = out_buffer->NumTuples();
  std::memcpy(out_buffer->ColumnIds(), original_column_ids, sizeof(col_id_t) * out_buffer->NumColumns());
  out_buffer->SetNumTuples(filled);

  auto old_dt_version = tables_.Find(old_version_num)->second;
  auto curr_dt_version = tables_.Find(version_num)->second;

  // Populate the default values to all the scanned tuples
  if (filled > 0) {
    // Calculate the col_oids of out_buffer which are missing from the old version of datatable
    std::unordered_set<catalog::col_oid_t> missing_col_oids = GetMissingColumnOidsForVersion(pr_map, old_dt_version);

    // TODO(Sai): The default values can be populated directly into the ProjectedColumns, making it faster than the
    // case of column being present. Need to handle the bitmask operations correctly for null default values.
    // Fill in the default values for the missing columns
    for (uint32_t row_idx = 0; row_idx < filled; row_idx++) {
      ProjectedColumns::RowView row = out_buffer->InterpretAsRow(row_idx);
      for (catalog::col_oid_t col_oid : missing_col_oids) {
        FillDefaultValue(&row, col_oid, pr_map);
      }
    }
  }
}

std::vector<col_id_t> SqlTable::ColIdsForOids(const std::vector<catalog::col_oid_t> &col_oids,
                                              layout_version_t version) const {
  TERRIER_ASSERT(!col_oids.empty(), "Should be used to access at least one column.");
  std::vector<col_id_t> col_ids;

  // Build the input to the initializer constructor
  for (const catalog::col_oid_t col_oid : col_oids) {
    TERRIER_ASSERT(tables_.Find(version) != tables_.CEnd(), "Table version must exist before insert");
    TERRIER_ASSERT(tables_.Find(version)->second.column_map.count(col_oid) > 0,
                   "Provided col_oid does not exist in the table.");
    const col_id_t col_id = tables_.Find(version)->second.column_map.at(col_oid);
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

    TERRIER_ASSERT(tables_.Find(version) != tables_.CEnd(), "Table version must exist");
    const auto oid_to_id =
        std::find_if(tables_.Find(version)->second.column_map.cbegin(), tables_.Find(version)->second.column_map.cend(),
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

// TODO(Yashwanth): don't copy the entire header, no need for template only take in ColumnIds() and then just modify
// that when resetting header only have memc py ColumnIds()
template <class RowType>
void SqlTable::ModifyProjectionHeaderForVersion(RowType *out_buffer, const DataTableVersion &curr_dt_version,
                                                const DataTableVersion &old_dt_version,
                                                col_id_t *original_col_id_store) const {
  // The slot version is not the same as the version_num
  // 1. Copy the old header (excluding bitmap)
  std::memcpy(original_col_id_store, out_buffer->ColumnIds(), sizeof(col_id_t) * out_buffer->NumColumns());

  // 2. For each column present in the old version, change the column id to the col id of that version
  //    For each column not present in the old version, change the column id to the sentinel value
  //    VERSION_POINTER_COLUMN_ID
  for (uint16_t i = 0; i < out_buffer->NumColumns(); i++) {
    TERRIER_ASSERT(out_buffer->ColumnIds()[i] != VERSION_POINTER_COLUMN_ID,
                   "Output buffer should not read the version pointer column.");
    catalog::col_oid_t col_oid = curr_dt_version.inverse_column_map.at(out_buffer->ColumnIds()[i]);
    if (old_dt_version.column_map.count(col_oid) > 0) {
      out_buffer->ColumnIds()[i] = old_dt_version.column_map.at(col_oid);
    } else {
      // TODO(Yashwanth): consider renaming VERSION_POINTER_COLUMN_ID, since we're using it for more than just that now
      out_buffer->ColumnIds()[i] = VERSION_POINTER_COLUMN_ID;
    }
  }
}

template void SqlTable::ModifyProjectionHeaderForVersion<ProjectedRow>(ProjectedRow *out_buffer,
                                                                       const DataTableVersion &curr_dt_version,
                                                                       const DataTableVersion &old_dt_version,
                                                                       col_id_t *original_col_id_store) const;
template void SqlTable::ModifyProjectionHeaderForVersion<ProjectedColumns>(ProjectedColumns *out_buffer,
                                                                           const DataTableVersion &curr_dt_version,
                                                                           const DataTableVersion &old_dt_version,
                                                                           col_id_t *original_col_id_store) const;

std::unordered_set<catalog::col_oid_t> SqlTable::GetMissingColumnOidsForVersion(
    const ProjectionMap &pr_map, const DataTableVersion &old_dt_version) const {
  // Calculate the col_oids of out_buffer which are missing from the old version of datatable
  std::unordered_set<catalog::col_oid_t> missing_col_oids;
  for (const auto &it : pr_map) {
    missing_col_oids.emplace(it.first);
  }
  for (const auto &it : old_dt_version.column_map) {
    // Remove the col_oid from the missing_col_oids
    missing_col_oids.erase(it.first);
  }
  return missing_col_oids;
}

template <class RowType>
void SqlTable::FillDefaultValue(RowType *out_buffer, const catalog::col_oid_t col_oid,
                                const ProjectionMap &pr_map) const {
  TERRIER_ASSERT(default_value_map_.Find(col_oid) != default_value_map_.CEnd(),
                 "Every column in schema must exist in default_value_map");
  auto pair = default_value_map_.Find(col_oid)->second;
  auto default_value = pair.first;
  auto attr_size = pair.second;
  // TODO(Sai): If this becomes a performance bottleneck, we can move this logic to ModifyProjectionHeaderForVersion
  storage::StorageUtil::CopyWithNullCheck(default_value, out_buffer, attr_size, pr_map.at(col_oid));
}

template void SqlTable::FillDefaultValue<ProjectedRow>(ProjectedRow *, const catalog::col_oid_t,
                                                       const ProjectionMap &) const;
template void SqlTable::FillDefaultValue<ProjectedColumns::RowView>(ProjectedColumns::RowView *,
                                                                    const catalog::col_oid_t,
                                                                    const ProjectionMap &) const;

}  // namespace terrier::storage
