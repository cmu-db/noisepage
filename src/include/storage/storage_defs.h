#pragma once

#include "common/strong_typedef.h"

namespace terrier::storage {

// Internally we use the sign bit to represent if a column is varlen or not. Down to the implementation detail though,
// we always allocate 16 bytes for a varlen entry, with the first 8 bytes being the pointer to the value and following
// 4 bytes be the size of the varlen. There are 4 bytes of padding for alignment purposes.
constexpr uint16_t VARLEN_COLUMN = static_cast<uint16_t>(0x8010);  // 16 with the first (most significant) bit set to 1

// In type_util.h there are a total of 5 possible inlined attribute sizes:
// 1, 2, 4, 8, and 16-bytes (16 byte is the structure portion of varlen).
// Since we pack these attributes in descending size order, we can infer a
// columns size by tracking the locations of the attribute size boundaries.
// Therefore, we only need to track 4 locations because the exterior bounds
// are implicit.
constexpr uint8_t NUM_ATTR_BOUNDARIES = 4;

STRONG_TYPEDEF_HEADER(col_id_t, uint16_t);
STRONG_TYPEDEF_HEADER(layout_version_t, uint16_t);

// All tuples potentially visible to txns should have a non-null attribute of version vector.
// This is not to be confused with a non-null version vector that has value nullptr (0).

constexpr col_id_t VERSION_POINTER_COLUMN_ID = col_id_t(0);
constexpr uint8_t NUM_RESERVED_COLUMNS = 1;

/**
 * Denote whether a record modifies the logical delete column, used when DataTable inspects deltas
 */
enum class DeltaRecordType : uint8_t { UPDATE = 0, INSERT, DELETE };

/**
 * Types of LogRecords
 */
enum class LogRecordType : uint8_t { REDO = 1, DELETE, COMMIT, ABORT };

}  // namespace terrier::storage
