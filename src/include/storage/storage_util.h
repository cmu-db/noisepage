#pragma once
#include <utility>
#include <unordered_map>
#include "common/macros.h"
#include "common/typedefs.h"
#include "storage/block_layout.h"
#include "storage/storage_defs.h"

namespace terrier::catalog {
class Schema;
}

namespace terrier::storage {
class ProjectedRow;
class TupleAccessStrategy;
class UndoRecord;
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
   * @param projection_list_index the projection_list_index to copy to
   */
  template <class RowType>
  static void CopyWithNullCheck(const byte *from, RowType *to, uint8_t size, uint16_t projection_list_index);

  /**
   * Copy from pointer location into the tuple slot at given column id. If the pointer location is null,
   * set the null bit on attribute.
   * @param from pointer location to copy fro, or nullptr
   * @param to ProjectedRow to copy into
   * @param accessor TupleAccessStrategy used to interact with the given block.
   * @param to tuple slot to copy into
   * @param col_id the col_id to copy into
   */
  static void CopyWithNullCheck(const byte *from, const TupleAccessStrategy &accessor, TupleSlot to, col_id_t col_id);

  /**
   * Copy an attribute from a block into a ProjectedRow.
   * @param accessor TupleAccessStrategy used to interact with the given block.
   * @param from tuple slot to copy from
   * @param to projected row to copy into
   * @param projection_list_offset The projection_list index to copy to on the projected row.
   */
  template <class RowType>
  static void CopyAttrIntoProjection(const TupleAccessStrategy &accessor, TupleSlot from, RowType *to,
                                     uint16_t projection_list_offset);

  /**
   * Copy an attribute from a ProjectedRow into a block.
   * @param accessor TupleAccessStrategy used to interact with the given block.
   * @param to tuple slot to copy to
   * @param from projected row to copy from
   * @param projection_list_offset The projection_list index to copy from on the projected row.
   */
  template <class RowType>
  static void CopyAttrFromProjection(const TupleAccessStrategy &accessor, TupleSlot to, const RowType &from,
                                     uint16_t projection_list_offset);

  /**
   * Applies delta into the given buffer.
   *
   * Specifically, columns present in the delta will have their value (or lack of value, in the case of null) copied
   * into the same column in the buffer. It is expected that the buffer's columns is a super set of the delta. If not,
   * behavior is not defined.
   *
   * @param layout layout used for the projected row
   * @param delta delta to apply
   * @param buffer buffer to apply delta into
   */
  template <class RowType>
  static void ApplyDelta(const BlockLayout &layout, const ProjectedRow &delta, RowType *buffer);

  /**
   * Given an address offset, aligns it to the word_size
   * @param word_size size in bytes to align offset to
   * @param offset address to be aligned
   * @return modified version of address padded to align to word_size
   */
  static uint32_t PadUpToSize(uint8_t word_size, uint32_t offset);

  /**
   * Given a pointer, pad the pointer so that the pointer aligns to the given size.
   * @param size the size to pad up to
   * @param ptr the pointer to pad
   * @return padded pointer
   */
  // This const qualifier on ptr lies. Use this really only for pointer arithmetic.
  static byte *AlignedPtr(const uint8_t size, const void *ptr) {
    auto ptr_value = reinterpret_cast<uintptr_t>(ptr);
    uint64_t remainder = ptr_value % size;
    return remainder == 0 ? reinterpret_cast<byte *>(ptr_value)
                          : reinterpret_cast<byte *>(ptr_value + size - remainder);
  }

  /**
   * Given a pointer, pad the pointer so that the pointer aligns to the size of A.
   * @tparam A type of value to pad up to
   * @param ptr the pointer to pad
   * @return padded pointer
   */
  template <class A>
  static A *AlignedPtr(const void *ptr) {
    return reinterpret_cast<A *>(AlignedPtr(sizeof(A), ptr));
  }

  /**
   * Given a schema, returns both a BlockLayout for the storage layer, and a mapping between each column's oid and the
   * corresponding column id in the storage layer/BlockLayout
   * @param schema Schema to generate a BlockLayout from. Columns should all have unique oids
   * @return pair of BlockLayout and a map between col_oid_t and col_id
   */
  static std::pair<BlockLayout, ColumnMap> BlockLayoutFromSchema(const catalog::Schema &schema);
};
}  // namespace terrier::storage
