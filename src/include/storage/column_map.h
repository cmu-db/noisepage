#pragma once

#include <unordered_map>

#include "catalog/catalog_defs.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"

namespace terrier::storage {

/** ColumnMapInfo maps between col_oids in Schema and useful information that we need about a Column in SqlTable. */
struct ColumnMapInfo {
  /** col_id in BlockLayout. */
  col_id_t col_id_;
  /** SQL type of the column. */
  type::TypeId col_type_;
};

/**
 * Used by SqlTable to map between col_oids in Schema and useful necessary information.
 */
using ColumnMap = std::unordered_map<catalog::col_oid_t, ColumnMapInfo>;

}  // namespace terrier::storage
