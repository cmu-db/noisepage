#pragma once

#include <unordered_map>

#include "catalog/catalog_defs.h"

namespace terrier::storage {
/**
 * Used by execution and storage layers to map between col_oids and offsets within a ProjectedRow
 */
using ProjectionMap = std::unordered_map<catalog::col_oid_t, uint16_t>;
}  // namespace terrier::storage
