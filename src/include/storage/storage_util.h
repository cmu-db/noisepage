#pragma once
#include <unordered_map>
#include "common/macros.h"
#include "common/typedefs.h"
#include "storage/storage_defs.h"

namespace terrier::storage {
class ProjectedRow;
class TupleAccessStrategy;
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
  static void WriteBytes(uint8_t attr_size, uint64_t val, byte *pos);

  /**
   * Read specified number of bytes from position and interpret the bytes as
   * an integer of given size. (Thus only 1, 2, 4, 8 are allowed)
   *
   * @param attr_size attr_size the number of bytes to write. (one of {1, 2, 4, 8})
   * @param pos the location to read from.
   * @return the byte value at position, padded up to 8 bytes.
   */
  static uint64_t ReadBytes(uint8_t attr_size, const byte *pos);

  /**
   * Copy from pointer location into projected row at given column id. If the pointer location is null,
   * set the null bit on attribute.
   * @param from pointer location to copy fro, or nullptr
   * @param to ProjectedRow to copy into
   * @param size size of the attribute
   * @param col_id column id to copy into
   */
  static void CopyWithNullCheck(const byte *from, ProjectedRow *to, uint8_t size, uint16_t col_id);

  /**
   * Copy from pointer location into the tuple slot at given column id. If the pointer location is null,
   * set the null bit on attribute.
   * @param from pointer location to copy fro, or nullptr
   * @param to ProjectedRow to copy into
   * @param accessor TupleAccessStrategy used to interact with the given block.
   * @param to tuple slot to copy into
   * @param col_id col_id to copy into
   */
  static void CopyWithNullCheck(const byte *from, const TupleAccessStrategy &accessor, TupleSlot to, uint16_t col_id);

  /**
   * Copy an attribute from a block into a ProjectedRow.
   * @param accessor TupleAccessStrategy used to interact with the given block.
   * @param from tuple slot to copy from
   * @param to projected row to copy into
   * @param projection_list_offset The projection_list index to copy to on the projected row.
   */
  static void CopyAttrIntoProjection(const TupleAccessStrategy &accessor, TupleSlot from, ProjectedRow *to,
                                     uint16_t projection_list_offset);

  /**
   * Copy an attribute from a ProjectedRow into a block.
   * @param accessor TupleAccessStrategy used to interact with the given block.
   * @param to tuple slot to copy to
   * @param from projected row to copy from
   * @param projection_list_offset The projection_list index to copy from on the projected row.
   */
  static void CopyAttrFromProjection(const TupleAccessStrategy &accessor, TupleSlot to, const ProjectedRow &from,
                                     uint16_t projection_list_offset);

  /**
   * Applies delta into the given buffer.
   *
   * Specifically, columns present in the delta will have their value (or lack of value, in the case of null) copied
   * into the same column in the buffer. It is expected that the buffer's columns is a super set of the delta. If not,
   * behavior is not defined.
   * @param layout layout used for the projected row
   * @param delta delta to apply
   * @param buffer buffer to apply delta into
   * @param col_to_index a mapping between column id and projection list index for the buffer to apply delta to. This
   *                     speeds up operation if multiple deltas are expected to be applied to the same buffer.
   */
  static void ApplyDelta(const BlockLayout &layout, const ProjectedRow &delta, ProjectedRow *buffer,
                         const std::unordered_map<uint16_t, uint16_t> &col_to_index);

  /**
   * Applies delta into the given buffer.
   *
   * Specifically, columns present in the delta will have their value (or lack of value, in the case of null) copied
   * into the same column in the buffer. It is expected that the buffer's columns is a super set of the delta. If not,
   * behavior is not defined.
   *
   * @warning This version of the function is slow if you expect to apply multiple deltas into the same buffer, because
   * every call will construct their own maps from column id to projection list index. If that is your use case, call
   * the other version of this function that takes in a map that can be reused across different calls.
   *
   * @param layout layout used for the projected row
   * @param delta delta to apply
   * @param buffer buffer to apply delta into
   */
  static void ApplyDelta(const BlockLayout &layout, const ProjectedRow &delta, ProjectedRow *buffer);
  /**
   * Given an address offset, aligns it to the word_size
   * @param word_size size in bytes to align offset to
   * @param offset address to be aligned
   * @return modified version of address padded to align to word_size
   */
  static uint32_t PadOffsetToSize(uint8_t word_size, uint32_t offset);
};
}  // namespace terrier::storage
