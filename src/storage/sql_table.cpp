#include "storage/sql_table.h"
#include <set>
#include <vector>
#include "common/macros.h"
#include "storage/storage_util.h"

namespace terrier::storage {

SqlTable::SqlTable(BlockStore *const store, const catalog::Schema &schema) : block_store_(store) {
  // Begin with the NUM_RESERVED_COLUMNS in the attr_sizes
  std::vector<uint8_t> attr_sizes;
  attr_sizes.reserve(NUM_RESERVED_COLUMNS + schema.GetColumns().size());

  for (uint8_t i = 0; i < NUM_RESERVED_COLUMNS; i++) {
    attr_sizes.emplace_back(8);
  }

  TERRIER_ASSERT(attr_sizes.size() == NUM_RESERVED_COLUMNS,
                 "attr_sizes should be initialized with NUM_RESERVED_COLUMNS elements.");

  for (const auto &column : schema.GetColumns()) {
    attr_sizes.push_back(column.AttrSize());
  }

  auto offsets = storage::StorageUtil::ComputeBaseAttributeOffsets(attr_sizes, NUM_RESERVED_COLUMNS);

  ColumnMap col_oid_to_id;
  // Build the map from Schema columns to underlying columns
  for (const auto &column : schema.GetColumns()) {
    switch (column.AttrSize()) {
      case VARLEN_COLUMN:
        col_oid_to_id[column.Oid()] = col_id_t(offsets[0]++);
        break;
      case 8:
        col_oid_to_id[column.Oid()] = col_id_t(offsets[1]++);
        break;
      case 4:
        col_oid_to_id[column.Oid()] = col_id_t(offsets[2]++);
        break;
      case 2:
        col_oid_to_id[column.Oid()] = col_id_t(offsets[3]++);
        break;
      case 1:
        col_oid_to_id[column.Oid()] = col_id_t(offsets[4]++);
        break;
      default:
        throw std::runtime_error("unexpected switch case value");
    }
  }

  auto layout = storage::BlockLayout(attr_sizes);
  table_ = {new DataTable(block_store_, layout, layout_version_t(0)), layout, col_oid_to_id};
}

std::vector<col_id_t> SqlTable::ColIdsForOids(const std::vector<catalog::col_oid_t> &col_oids) const {
  TERRIER_ASSERT(!col_oids.empty(), "Should be used to access at least one column.");
  std::vector<col_id_t> col_ids;

  // Build the input to the initializer constructor
  for (const catalog::col_oid_t col_oid : col_oids) {
    TERRIER_ASSERT(table_.column_map.count(col_oid) > 0, "Provided col_oid does not exist in the table.");
    const col_id_t col_id = table_.column_map.at(col_oid);
    col_ids.push_back(col_id);
  }

  return col_ids;
}

template <class ProjectionInitializerType>
ProjectionMap SqlTable::ProjectionMapForInitializer(const ProjectionInitializerType &initializer) const {
  ProjectionMap projection_map;
  // for every attribute in the initializer
  for (uint16_t i = 0; i < initializer.NumColumns(); i++) {
    // extract the underlying col_id it refers to
    const col_id_t col_id_at_offset = initializer.ColId(i);
    // find the key (col_oid) in the table's map corresponding to the value (col_id)
    const auto oid_to_id =
        std::find_if(table_.column_map.cbegin(), table_.column_map.cend(),
                     [&](const auto &oid_to_id) -> bool { return oid_to_id.second == col_id_at_offset; });
    // insert the mapping from col_oid to projection offset
    projection_map[oid_to_id->first] = i;
  }

  return projection_map;
}

template ProjectionMap SqlTable::ProjectionMapForInitializer<ProjectedColumnsInitializer>(
    const ProjectedColumnsInitializer &initializer) const;
template ProjectionMap SqlTable::ProjectionMapForInitializer<ProjectedRowInitializer>(
    const ProjectedRowInitializer &initializer) const;

}  // namespace terrier::storage
