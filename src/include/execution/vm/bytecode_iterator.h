#pragma once

#include <vector>

#include "execution/util/common.h"
#include "execution/vm/bytecodes.h"

namespace terrier::vm {

class LocalVar;

/**
 * An iterator over a section of bytecode. Use as follows:
 *
 * @code
 * for (auto iter = ...; !iter.Done(); iter.Advance() {
 *   // body
 * }
 * @endcode
 *
 * At any point, the iterator always points to the start of a bytecode
 * instruction. Users can query the current bytecode along with all its
 * operands through the various operand getter functions.
 *
 * BytecodeIterators support random access of the bytecode through explicit
 * calls to @a SetPosition(). It is the user's responsibility to ensure these
 * points mark the beginning of a bytecode instruction.
 */
class BytecodeIterator {
 public:
  /**
   * Construct an iterator over the bytecode, but only the within the range
   * [start,end]
   * @param bytecode The underlying bytecode
   * @param start The start position
   * @param end The end position
   */
  BytecodeIterator(const std::vector<u8> &bytecode, std::size_t start, std::size_t end);

  /**
   * Get the bytecode instruction the iterator is currently pointing to
   * @return The current bytecode instruction
   */
  Bytecode CurrentBytecode() const;

  /**
   * Has the iterator reached the end
   * @return True if iteration is complete; false otherwise
   */
  bool Done() const;

  /**
   * Advance the iterator to the next bytecode instruction. It's expected that
   * the user has verified there are more instructions with a preceding call to
   * @em Done()
   * @see Done()
   */
  void Advance();

  /**
   * Read the operand at index @a operand_index for the current bytecode as a
   * signed immediate value
   * @param operand_index The index of operand to read
   * @return The immediate value, up-casted to a signed 64-bit integer
   */
  i64 GetImmediateOperand(u32 operand_index) const;

  /**
   * Read the operand at index @a operand_index for the current bytecode as a
   * floating point immediate value
   * @param operand_index The index of operand to read
   * @return The immediate value, up-casted to a double
   */
  f64 GetFloatImmediateOperand(u32 operand_index) const;

  /**
   * Read the operand at index @a operand_index for the current bytecode as an
   * unsigned immediate value
   * @param operand_index The index of the operand to read
   * @return The immediate value, up-casted to an unsigned 64-bit integer
   */
  u64 GetUnsignedImmediateOperand(u32 operand_index) const;

  /**
   * Read the operand at index @a operand_index for the current bytecode as a
   * jump offset as part of either a conditional or unconditional jump
   * @param operand_index The index of the operand to read
   * @return The jump offset at the given index
   */
  i32 GetJumpOffsetOperand(u32 operand_index) const;

  /**
   * Read the operand at index @a operand_index for the current bytecode as a
   * local variable
   * @param operand_index The index of the operand to read
   * @return The operand at the given operand index
   */
  LocalVar GetLocalOperand(u32 operand_index) const;

  /**
   * Read the operand at @a operand_index for the current bytecode as a count
   * of local variables, and read each such local variable into the output
   * vector @a locals.
   * @param operand_index The index of the operand to read
   * @param[out] locals output vector of locals
   * @return The number of operands
   */
  u16 GetLocalCountOperand(u32 operand_index, std::vector<LocalVar> *locals) const;

  /**
   * Read the operand at @a operand_index for the current bytecode as a count
   * of local variables
   * @param operand_index The index of the operand to read
   * @return The number of operands
   */
  u16 GetLocalCountOperand(u32 operand_index) const {
    std::vector<LocalVar> locals;
    return GetLocalCountOperand(operand_index, &locals);
  }

  /**
   * Get the operand at @a operand_index for the current bytecode as the ID of
   * a function defined in the module
   * @param operand_index The index of the operand to read
   * @return The encoded function ID
   */
  u16 GetFunctionIdOperand(u32 operand_index) const;

  /**
   * Return the total size in bytes of the bytecode instruction the iterator is
   * currently pointing to. This size includes variable length arguments.
   * @return The size, in bytes, of the bytecode this iterator is pointing to
   */
  u32 CurrentBytecodeSize() const;

  /**
   * Get the current position of the iterator
   * @return The position of the iterator from the start
   */
  std::size_t GetPosition() const { return curr_offset_ - start_offset_; }

  /**
   * Set the position of the iterator
   * @param pos The position to shift the iterator to
   */
  void SetPosition(std::size_t pos) {
    // TPL_ASSERT(offset < (end_offset() - start_offset()), "Invalid offset");
    curr_offset_ = start_offset_ + pos;
  }

 private:
  // ALL the bytecode instructions for a TPL compilation unit
  const std::vector<u8> &bytecodes_;
  // The range of bytecodes the iterator uses
  const std::size_t start_offset_;
  const std::size_t end_offset_;
  // The current offset in the bytecode. This member is always within the
  // range [start_offset, end_offset].
  std::size_t curr_offset_;
};

}  // namespace terrier::vm
