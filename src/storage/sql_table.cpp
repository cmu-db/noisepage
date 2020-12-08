#include "storage/sql_table.h"

#include <map>
#include <string>
#include <vector>

#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/storage_util.h"

namespace noisepage::storage {

SqlTable::SqlTable(const common::ManagedPointer<BlockStore> store, const catalog::Schema &schema) {
  // Begin with the NUM_RESERVED_COLUMNS in the attr_sizes
  std::vector<uint16_t> attr_sizes;
  attr_sizes.reserve(NUM_RESERVED_COLUMNS + schema.GetColumns().size());

  for (uint8_t i = 0; i < NUM_RESERVED_COLUMNS; i++) {
    attr_sizes.emplace_back(8);
  }

  NOISEPAGE_ASSERT(attr_sizes.size() == NUM_RESERVED_COLUMNS,
                   "attr_sizes should be initialized with NUM_RESERVED_COLUMNS elements.");

  for (const auto &column : schema.GetColumns()) {
    attr_sizes.push_back(column.AttributeLength());
  }

  auto offsets = storage::StorageUtil::ComputeBaseAttributeOffsets(attr_sizes, NUM_RESERVED_COLUMNS);

  ColumnMap col_map;
  // Build the map from Schema columns to underlying columns
  for (const auto &column : schema.GetColumns()) {
    switch (column.AttributeLength()) {
      case VARLEN_COLUMN:
        col_map[column.Oid()] = {col_id_t(offsets[0]++), column.Type()};
        break;
      case 8:
        col_map[column.Oid()] = {col_id_t(offsets[1]++), column.Type()};
        break;
      case 4:
        col_map[column.Oid()] = {col_id_t(offsets[2]++), column.Type()};
        break;
      case 2:
        col_map[column.Oid()] = {col_id_t(offsets[3]++), column.Type()};
        break;
      case 1:
        col_map[column.Oid()] = {col_id_t(offsets[4]++), column.Type()};
        break;
      default:
        throw std::runtime_error("unexpected switch case value");
    }
  }

  auto layout = storage::BlockLayout(attr_sizes);
  table_ = {new DataTable(store, layout, layout_version_t(0)), layout, col_map};
}

std::vector<col_id_t> SqlTable::ColIdsForOids(const std::vector<catalog::col_oid_t> &col_oids) const {
  NOISEPAGE_ASSERT(!col_oids.empty(), "Should be used to access at least one column.");
  std::vector<col_id_t> col_ids;

  // Build the input to the initializer constructor
  for (const catalog::col_oid_t col_oid : col_oids) {
    NOISEPAGE_ASSERT(table_.column_map_.count(col_oid) > 0, "Provided col_oid does not exist in the table.");
    const col_id_t col_id = table_.column_map_.at(col_oid).col_id_;
    col_ids.push_back(col_id);
  }

  return col_ids;
}

ProjectionMap SqlTable::ProjectionMapForOids(const std::vector<catalog::col_oid_t> &col_oids) {
  // Resolve OIDs to storage IDs
  auto col_ids = ColIdsForOids(col_oids);

  // Use std::map to effectively sort OIDs by their corresponding ID
  std::map<col_id_t, catalog::col_oid_t> inverse_map;
  for (uint16_t i = 0; i < col_oids.size(); i++) inverse_map[col_ids[i]] = col_oids[i];

  // Populate the projection map using the in-order iterator on std::map
  ProjectionMap projection_map;
  uint16_t i = 0;
  for (auto &iter : inverse_map) projection_map[iter.second] = i++;

  return projection_map;
}

catalog::col_oid_t SqlTable::OidForColId(const col_id_t col_id) const {
  const auto oid_to_id =
      std::find_if(table_.column_map_.cbegin(), table_.column_map_.cend(),
                   [&](const auto &oid_to_id) -> bool { return oid_to_id.second.col_id_ == col_id; });
  return oid_to_id->first;
}

}  // namespace noisepage::storage
