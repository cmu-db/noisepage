#pragma once

#include <vector>

#include "execution/vm/bytecodes.h"

namespace noisepage::execution::vm {

class LocalVar;

/**
 * An iterator over a section of bytecode. Use as follows:
 *
 * @code
 * for (auto iter = ...; !iter.Done(); iter.Advance()) {
 *   auto bytecode = iter.CurrentBytecode();
 *   // body
 * }
 * @endcode
 *
 * At any point, the iterator always points to the <b>start</b> of a bytecode instruction. Users can
 * query the current bytecode (through BytecodeIterator::CurrentBytecode()) along with all its
 * operands through the various operand getter functions.
 *
 * BytecodeIterators support random access of the bytecode through explicit calls to
 * BytecodeIterator::SetPosition(). It is the user's responsibility to ensure these points mark the
 * beginning of a bytecode instruction.
 */
class BytecodeIterator {
 public:
  /**
   * Construct an iterator over the bytecode, but only the within the range [start,end).
   * @param bytecode The underlying bytecode.
   * @param start The start position.
   * @param end The end position.
   */
  BytecodeIterator(const std::vector<uint8_t> &bytecode, std::size_t start, std::size_t end);

  /**
   * Construct an iterator over all the bytecode container in the input bytecode vector.
   * @param bytecode The underlying bytecode.
   */
  explicit BytecodeIterator(const std::vector<uint8_t> &bytecode);

  /**
   * @return The current bytecode instruction.
   */
  Bytecode CurrentBytecode() const;

  /**
   * @return True if iteration is complete; false otherwise.
   */
  bool Done() const;

  /**
   * Advance the iterator to the next bytecode instruction. It's expected that the user has verified
   * there are more instructions with a preceding call to BytecodeIterator::Done().
   */
  void Advance();

  /**
   * Read the operand at index @em operand_index for the current bytecode as a signed integer value.
   * Supports reading 8-, 16-, 32-, and 64-bit signed integer immediates.
   * @param operand_index The index of the operand to read.
   * @return The immediate value, up-casted to a signed 64-bit integer.
   */
  int64_t GetImmediateIntegerOperand(uint32_t operand_index) const;

  /**
   * Read the operand at index @em operand_index for the current bytecode as a floating point value.
   * Supports both single- and double-precision floating point immediates.
   * @param operand_index The index of the operand to read.
   * @return The immediate value, up-casted to a double-precision floating point value.
   */
  double GetImmediateFloatOperand(uint32_t operand_index) const;

  /**
   * Read the operand at index @em operand_index for the current bytecode as an unsigned integer
   * value. Supports 8-, 16-, 32-, and 64-bit unsigned integer immediates.
   * @param operand_index The index of the operand to read.
   * @return The immediate value, up-casted to an unsigned 64-bit integer.
   */
  uint64_t GetUnsignedImmediateIntegerOperand(uint32_t operand_index) const;

  /**
   * Read the operand at index @em operand_index for the current bytecode as a jump offset. Jump
   * offsets are signed (to support backwards and forward jumps) and relative to the current
   * bytecode position.
   * @param operand_index The index of the operand to read.
   * @return The jump offset at the given index.
   */
  int32_t GetJumpOffsetOperand(uint32_t operand_index) const;

  /**
   * Read the operand at index @em operand_index for the current bytecode as a local variable.
   * @param operand_index The index of the operand to read.
   * @return The operand at the given operand index.
   */
  LocalVar GetLocalOperand(uint32_t operand_index) const;

  /**
   * Read the operand at index @em operand_index for the current bytecode as a reference to a static
   * local stored in the TBC unit's data section. This is an absolute offset of an element in the
   * data section.
   * @param operand_index The index of the operand to read.
   * @return The static local at the given operand index.
   */
  LocalVar GetStaticLocalOperand(uint32_t operand_index) const;

  /**
   * Read the operand at @em operand_index for the current bytecode as a count of local variables,
   * and read each such local variable into the output vector @em locals.
   * @param operand_index The index of the operand to read.
   * @param locals The output vector.
   * @return The number of operands.
   */
  uint16_t GetLocalCountOperand(uint32_t operand_index, std::vector<LocalVar> *locals) const;

  /**
   * Read the operand at @em operand_index for the current bytecode as a count of local variables
   * that appear after this operand in the instruction.
   * @param operand_index The index of the operand to read.
   * @return The number of operands.
   */
  uint16_t GetLocalCountOperand(uint32_t operand_index) const {
    std::vector<LocalVar> locals;
    return GetLocalCountOperand(operand_index, &locals);
  }

  /**
   * Get the operand at @em operand_index for the current bytecode as the ID of a function defined
   * in the module.
   * @param operand_index The index of the operand to read.
   * @return The encoded function ID.
   */
  uint16_t GetFunctionIdOperand(uint32_t operand_index) const;

  /**
   * Return the total size in bytes of the bytecode instruction the iterator is currently pointing
   * to. This size includes variable length arguments.
   * @return The size, in bytes, of the bytecode this iterator is pointing to.
   */
  uint32_t CurrentBytecodeSize() const;

  /**
   * @return The position of the iterator from the start.
   */
  std::size_t GetPosition() const { return curr_offset_ - start_offset_; }

  /**
   * Set the position of the iterator.
   * @param pos The position to shift the iterator to
   */
  void SetPosition(std::size_t pos) {
    // TPL_ASSERT(offset < (end_offset() - start_offset()), "Invalid offset");
    curr_offset_ = start_offset_ + pos;
  }

 private:
  // All the bytecode instructions for a TPL compilation unit
  const std::vector<uint8_t> &bytecodes_;

  // The range of bytecodes the iterator uses as index offsets in the bytecode array
  const std::size_t start_offset_;
  const std::size_t end_offset_;

  // The current absolute offset in the bytecode, always in the range [start_offset, end_offset)
  std::size_t curr_offset_;
};

}  // namespace noisepage::execution::vm
