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
  STORAGE_LOG_DEBUG("Update schema version: {}", uint32_t(schema.GetVersion()));
  TERRIER_ASSERT(tables_.find(schema.GetVersion()) == tables_.end(), "schema versions for an SQL table must be unique");

  // Calculate the BlockLayout for the schema based off attribute sizes
  uint16_t num_8_byte_attrs = 0;
  uint16_t num_4_byte_attrs = 0;
  uint16_t num_2_byte_attrs = 0;
  uint16_t num_1_byte_attrs = 0;
  uint16_t num_varlen_byte_attrs = 0;

  // Begin with the NUM_RESERVED_COLUMNS in the attr_sizes
  std::vector<uint8_t> attr_sizes;
  attr_sizes.reserve(NUM_RESERVED_COLUMNS + schema.GetColumns().size());

  for (uint8_t i = 0; i < NUM_RESERVED_COLUMNS; i++) {
    attr_sizes.emplace_back(8);
    num_8_byte_attrs++;
  }

  TERRIER_ASSERT(attr_sizes.size() == NUM_RESERVED_COLUMNS,
                 "attr_sizes should be initialized with NUM_RESERVED_COLUMNS elements.");

  // First pass through to accumulate the counts of each attr_size
  for (const auto &column : schema.GetColumns()) {
    attr_sizes.push_back(column.GetAttrSize());
    switch (column.GetAttrSize()) {
      case 8:
        num_8_byte_attrs++;
        break;
      case 4:
        num_4_byte_attrs++;
        break;
      case 2:
        num_2_byte_attrs++;
        break;
      case 1:
        num_1_byte_attrs++;
        break;
      case VARLEN_COLUMN:
        num_varlen_byte_attrs++;
      default:
        break;
    }
  }

  TERRIER_ASSERT(static_cast<uint16_t>(attr_sizes.size()) ==
                     num_8_byte_attrs + num_4_byte_attrs + num_2_byte_attrs + num_1_byte_attrs + num_varlen_byte_attrs,
                 "Number of attr_sizes does not match the sum of attr counts.");

  // Initialize the offsets for each attr_size
  auto offset_varlen_byte_attrs = static_cast<uint16_t>(NUM_RESERVED_COLUMNS);
  auto offset_8_byte_attrs = static_cast<uint16_t>(offset_varlen_byte_attrs + num_varlen_byte_attrs);
  auto offset_4_byte_attrs = static_cast<uint16_t>(offset_8_byte_attrs + (num_8_byte_attrs - NUM_RESERVED_COLUMNS));
  auto offset_2_byte_attrs = static_cast<uint16_t>(offset_4_byte_attrs + num_4_byte_attrs);
  auto offset_1_byte_attrs = static_cast<uint16_t>(offset_2_byte_attrs + num_2_byte_attrs);

  ColumnMap col_map;
  InverseColumnMap inv_col_map;

  // Build the maps between Schema column OIDs and underlying column IDs
  for (const auto &column : schema.GetColumns()) {
    switch (column.GetAttrSize()) {
      case 8:
        inv_col_map[col_id_t(offset_8_byte_attrs)] = column.GetOid();
        col_map[column.GetOid()] = col_id_t(offset_8_byte_attrs++);
        break;
      case 4:
        inv_col_map[col_id_t(offset_4_byte_attrs)] = column.GetOid();
        col_map[column.GetOid()] = col_id_t(offset_4_byte_attrs++);
        break;
      case 2:
        inv_col_map[col_id_t(offset_2_byte_attrs)] = column.GetOid();
        col_map[column.GetOid()] = col_id_t(offset_2_byte_attrs++);
        break;
      case 1:
        inv_col_map[col_id_t(offset_1_byte_attrs)] = column.GetOid();
        col_map[column.GetOid()] = col_id_t(offset_1_byte_attrs++);
        break;
      case VARLEN_COLUMN:
        inv_col_map[col_id_t(offset_varlen_byte_attrs)] = column.GetOid();
        col_map[column.GetOid()] = col_id_t(offset_varlen_byte_attrs++);
        break;
      default:
        throw std::runtime_error("unexpected switch case value");
    }
  }

  BlockLayout layout = storage::BlockLayout(attr_sizes);
  tables_[schema.GetVersion()] = {new DataTable(block_store_, layout, schema.GetVersion()), layout, col_map,
                                  inv_col_map};
  STORAGE_LOG_DEBUG("# of versions: {}", tables_.size());
}

bool SqlTable::Select(transaction::TransactionContext *const txn, const TupleSlot slot, ProjectedRow *const out_buffer,
                      const ProjectionMap &pr_map, layout_version_t version_num) const {
  STORAGE_LOG_DEBUG("slot version: {}, current version: {}", !slot.GetBlock()->layout_version_, !version_num);

  layout_version_t old_version_num = slot.GetBlock()->layout_version_;

  TERRIER_ASSERT(out_buffer->NumColumns() <= tables_.at(version_num).column_map.size(),
                 "The output buffer never returns the version pointer columns, so it should have "
                 "fewer attributes.");

  // The version of the current slot is the same as the version num
  if (old_version_num == version_num) {
    return tables_.at(version_num).data_table->Select(txn, slot, out_buffer);
  }

  auto old_dt_version = tables_.at(old_version_num);

  // The slot version is not the same as the version_num
  col_id_t original_column_ids[out_buffer->NumColumns()];
  ModifyProjectionHeaderForVersion(out_buffer, tables_.at(version_num), old_dt_version, original_column_ids);

  // Get the result and copy back the old header
  bool result = old_dt_version.data_table->Select(txn, slot, out_buffer);
  std::memcpy(out_buffer->ColumnIds(), original_column_ids, sizeof(col_id_t) * out_buffer->NumColumns());

  // TODO(Yashwanth): handle default values
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
  STORAGE_LOG_DEBUG("Update slot version : {}, current version: {}", !slot.GetBlock()->layout_version_, !version_num);

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
    STORAGE_LOG_DEBUG("have to insert and delete ... ");

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

void SqlTable::Scan(transaction::TransactionContext *const txn, SqlTable::SlotIterator *start_pos,
                    ProjectedColumns *const out_buffer, const ProjectionMap &pr_map,
                    layout_version_t version_num) const {
  layout_version_t dt_version_num = start_pos->operator*().GetBlock()->layout_version_;

  TERRIER_ASSERT(out_buffer->NumColumns() <= tables_.at(version_num).column_map.size(),
                 "The output buffer never returns the version pointer columns, so it should have "
                 "fewer attributes.");
  col_id_t original_column_ids[out_buffer->NumColumns()];
  ModifyProjectionHeaderForVersion(out_buffer, tables_.at(version_num), tables_.at(dt_version_num),
                                   original_column_ids);

  DataTable::SlotIterator dt_slot = start_pos->GetDataTableSlotIterator();
  tables_.at(dt_version_num).data_table->Scan(txn, &dt_slot, out_buffer);
  if (dt_slot == tables_.at(dt_version_num).data_table->end()) {
    if ((start_pos->dt_version_)->first != version_num) {
      ++(*start_pos);
    }
  }

  uint32_t filled = out_buffer->NumTuples();
  std::memcpy(out_buffer->ColumnIds(), original_column_ids, sizeof(col_id_t) * out_buffer->NumColumns());
  out_buffer->SetNumTuples(filled);
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

}  // namespace terrier::storage
