#pragma once

#include "execution/vm/bytecode_operands.h"
#include "execution/vm/bytecodes.h"

namespace tpl::vm {

/**
 * Properties of a generic operand type
 */
template <OperandType>
struct OperandTypeTraits {
  /**
   * Whether the operand is signed or unsigned
   */
  static constexpr bool kIsSigned = false;

  /**
   * Size of the operand
   */
  static constexpr OperandSize kOperandSize = OperandSize::None;

  /**
   * Also the size of the operand, but as a raw u32
   */
  static constexpr u32 kSize = static_cast<u32>(kOperandSize);
};

// Generate traits for each operand
#define DECLARE_OPERAND_TYPE(Name, IsSigned, BaseSize)           \
  template <>                                                    \
  struct OperandTypeTraits<OperandType::Name> {                  \
    static constexpr bool kIsSigned = IsSigned;                  \
    static constexpr OperandSize kOperandSize = BaseSize;        \
    static constexpr u32 kSize = static_cast<u32>(kOperandSize); \
  };
OPERAND_TYPE_LIST(DECLARE_OPERAND_TYPE)
#undef DECLARE_OPERAND_TYPE

/**
 * Properties of a bytecode.
 * @tparam operands of the bytecode .
 */
template <OperandType... operands>
struct BytecodeTraits {
  /**
   * Number of operands
   */
  static constexpr const u32 kOperandCount = sizeof...(operands);

  /**
   * Total size of the operand size
   */
  static constexpr const u32 kOperandsSize = (0u + ... + OperandTypeTraits<operands>::kSize);

  /**
   * List of operand types
   */
  static constexpr const OperandType kOperandTypes[] = {operands...};

  /**
   * List of operand sizes
   */
  static constexpr const OperandSize kOperandSizes[] = {OperandTypeTraits<operands>::kOperandSize...};

  /**
   * Total size of bytecode + operands.
   */
  static constexpr const u32 kSize = sizeof(std::underlying_type_t<Bytecode>) + kOperandsSize;
};

}  // namespace tpl::vm
