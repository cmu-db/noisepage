#pragma once

#include "execution/vm/bytecode_operands.h"
#include "execution/vm/bytecodes.h"

namespace terrier::execution::vm {

/**
 * Properties of a generic operand type
 */
template <OperandType>
struct OperandTypeTraits {
  /**
   * Whether the operand is signed or unsigned
   */
  static constexpr bool K_IS_SIGNED = false;

  /**
   * Size of the operand
   */
  static constexpr OperandSize K_OPERAND_SIZE = OperandSize::None;

  /**
   * Also the size of the operand, but as a raw uint32_t
   */
  static constexpr uint32_t K_SIZE = static_cast<uint32_t>(K_OPERAND_SIZE);
};

// Generate traits for each operand
#define DECLARE_OPERAND_TYPE(Name, IsSigned, BaseSize)                     \
  template <>                                                              \
  struct OperandTypeTraits<OperandType::Name> {                            \
    static constexpr bool kIsSigned = IsSigned;                            \
    static constexpr OperandSize kOperandSize = BaseSize;                  \
    static constexpr uint32_t kSize = static_cast<uint32_t>(kOperandSize); \
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
  static constexpr const uint32_t K_OPERAND_COUNT = sizeof...(operands);

  /**
   * Total size of the operand size
   */
  static constexpr const uint32_t K_OPERANDS_SIZE = (0u + ... + OperandTypeTraits<operands>::kSize);

  /**
   * List of operand types
   */
  static constexpr const OperandType k_operand_types[] = {operands...};

  /**
   * List of operand sizes_
   */
  static constexpr const OperandSize k_operand_sizes[] = {OperandTypeTraits<operands>::kOperandSize...};

  /**
   * Total size of bytecode + operands.
   */
  static constexpr const uint32_t K_SIZE = sizeof(std::underlying_type_t<Bytecode>) + K_OPERANDS_SIZE;
};

}  // namespace terrier::execution::vm
