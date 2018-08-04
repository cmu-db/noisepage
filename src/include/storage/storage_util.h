#pragma once
#include <unordered_map>
#include "common/macros.h"
#include "common/typedefs.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"

namespace terrier::storage {
/**
 * Static utility class for common functions in storage
 */
class StorageUtil {
 public:
  StorageUtil() = delete;

  /**
   * Write specified number of bytes to position and interpret the bytes as
   * an integer of given size. (Thus only 1, 2, 4, 8 are allowed)
   *
   * @param attr_size the number of bytes to write. (one of {1, 2, 4, 8})
   * @param val the byte value to write. Truncated if neccessary.
   * @param pos the location to write to.
   */
  static void WriteBytes(uint8_t attr_size, uint64_t val, byte *pos) {
    switch (attr_size) {
      case sizeof(uint8_t):
        *reinterpret_cast<uint8_t *>(pos) = static_cast<uint8_t>(val);
        break;
      case sizeof(uint16_t):
        *reinterpret_cast<uint16_t *>(pos) = static_cast<uint16_t>(val);
        break;
      case sizeof(uint32_t):
        *reinterpret_cast<uint32_t *>(pos) = static_cast<uint32_t>(val);
        break;
      case sizeof(uint64_t):
        *reinterpret_cast<uint64_t *>(pos) = static_cast<uint64_t>(val);
        break;
      default:
        // Invalid attr size
        throw std::runtime_error("Invalid byte write value");
    }
  }

  /**
   * Read specified number of bytes from position and interpret the bytes as
   * an integer of given size. (Thus only 1, 2, 4, 8 are allowed)
   *
   * @param attr_size attr_size the number of bytes to write. (one of {1, 2, 4, 8})
   * @param pos the location to read from.
   * @return the byte value at position, padded up to 8 bytes.
   */
  static uint64_t ReadBytes(uint8_t attr_size, const byte *pos) {
    switch (attr_size) {
      case sizeof(uint8_t):
        return *reinterpret_cast<const uint8_t *>(pos);
      case sizeof(uint16_t):
        return *reinterpret_cast<const uint16_t *>(pos);
      case sizeof(uint32_t):
        return *reinterpret_cast<const uint32_t *>(pos);
      case sizeof(uint64_t):
        return *reinterpret_cast<const uint64_t *>(pos);
      default:
        // Invalid attr size
        throw std::runtime_error("Invalid byte write value");
    }
  }

  /**
   * Copy from pointer location into projected row at given column id. If the pointer location is null,
   * set the null bit on attribute.
   * @param from pointer location to copy fro, or nullptr
   * @param to ProjectedRow to copy into
   * @param size size of the attribute
   * @param col_id column id to copy into
   */
  static void CopyWithNullCheck(const byte *from, ProjectedRow *to, uint8_t size, uint16_t col_id) {
    if (from == nullptr)
      to->SetNull(col_id);
    else
      WriteBytes(size, ReadBytes(size, from), to->AccessForceNotNull(col_id));
  }

  /**
   * Copy from pointer location into the tuple slot at given column id. If the pointer location is null,
   * set the null bit on attribute.
   * @param from pointer location to copy fro, or nullptr
   * @param to ProjectedRow to copy into
   * @param accessor TupleAccessStrategy used to interact with the given block.
   * @param to tuple slot to copy into
   * @param col_id col_id to copy into
   */
  static void CopyWithNullCheck(const byte *from, const TupleAccessStrategy &accessor, TupleSlot to, uint16_t col_id) {
    if (from == nullptr) {
      accessor.SetNull(to, col_id);
    } else {
      uint8_t size = accessor.GetBlockLayout().attr_sizes_[col_id];
      WriteBytes(size, ReadBytes(size, from), accessor.AccessForceNotNull(to, col_id));
    }
  }

  /**
   * Copy an attribute from a block into a ProjectedRow.
   * @param accessor TupleAccessStrategy used to interact with the given block.
   * @param from tuple slot to copy from
   * @param to projected row to copy into
   * @param projection_list_offset The projection_list index to copy to on the projected row.
   */
  static void CopyAttrIntoProjection(const TupleAccessStrategy &accessor, TupleSlot from, ProjectedRow *to,
                                     uint16_t projection_list_offset) {
    uint16_t col_id = to->ColumnIds()[projection_list_offset];
    uint8_t attr_size = accessor.GetBlockLayout().attr_sizes_[col_id];
    byte *stored_attr = accessor.AccessWithNullCheck(from, col_id);
    CopyWithNullCheck(stored_attr, to, attr_size, projection_list_offset);
  }

  /**
   * Copy an attribute from a ProjectedRow into a block.
   * @param accessor TupleAccessStrategy used to interact with the given block.
   * @param to tuple slot to copy to
   * @param from projected row to copy from
   * @param projection_list_offset The projection_list index to copy from on the projected row.
   */
  static void CopyAttrFromProjection(const TupleAccessStrategy &accessor, TupleSlot to, const ProjectedRow &from,
                                     uint16_t projection_list_offset) {
    uint16_t col_id = from.ColumnIds()[projection_list_offset];
    const byte *stored_attr = from.AccessWithNullCheck(projection_list_offset);
    CopyWithNullCheck(stored_attr, accessor, to, col_id);
  }

  /**
   * TODO(Tianyu): Write
   * @param layout
   * @param delta
   * @param buffer
   * @param col_to_index
   */
  static void ApplyDelta(const BlockLayout &layout, const ProjectedRow &delta, ProjectedRow *buffer,
                         const std::unordered_map<uint16_t, uint16_t> &col_to_index) {
    for (uint16_t i = 0; i < delta.NumColumns(); i++) {
      uint16_t delta_col_id = delta.ColumnIds()[i];
      auto it = col_to_index.find(delta_col_id);
      if (it != col_to_index.end()) {
        uint16_t col_id = it->first, buffer_offset = it->second;
        uint8_t attr_size = layout.attr_sizes_[col_id];
        StorageUtil::CopyWithNullCheck(delta.AccessWithNullCheck(i), buffer, attr_size, buffer_offset);
      }
    }
  }
};
}  // namespace terrier::storage
