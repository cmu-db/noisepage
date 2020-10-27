#pragma once

#include "execution/vm/bytecode_operands.h"
#include "execution/vm/bytecodes.h"

namespace noisepage::execution::vm {

/**
 * Properties of a generic operand type
 */
template <OperandType>
struct OperandTypeTraits {
  /** Whether the operand is signed or unsigned. */
  static constexpr bool IS_SIGNED = false;
  /** Size of the operand. */
  static constexpr OperandSize OPERAND_SIZE = OperandSize::None;
  /** Also the size of the operand, but as a raw uint32_t. */
  static constexpr uint32_t SIZE = static_cast<uint32_t>(OPERAND_SIZE);
};

#define DECLARE_OPERAND_TYPE(Name, IsSigned, BaseSize)                    \
  template <>                                                             \
  struct OperandTypeTraits<OperandType::Name> {                           \
    static constexpr bool IS_SIGNED = IsSigned;                           \
    static constexpr OperandSize OPERAND_SIZE = BaseSize;                 \
    static constexpr uint32_t SIZE = static_cast<uint32_t>(OPERAND_SIZE); \
  };
OPERAND_TYPE_LIST(DECLARE_OPERAND_TYPE)
#undef DECLARE_OPERAND_TYPE

/**
 * Properties of a bytecode.
 * @tparam operands of the bytecode.
 */
template <OperandType... operands>
struct BytecodeTraits {
  /** Number of operands. */
  static constexpr const uint32_t OPERAND_COUNT = sizeof...(operands);
  /** Total size of the operand size. */
  static constexpr const uint32_t OPERANDS_SIZE = (0u + ... + OperandTypeTraits<operands>::SIZE);
  /** List of operand types. */
  static constexpr const OperandType operand_types[] = {operands...};
  /** List of operand sizes. */
  static constexpr const OperandSize operand_sizes[] = {OperandTypeTraits<operands>::OPERAND_SIZE...};
  /** Total size of bytecode + operands. */
  static constexpr const uint32_t SIZE = sizeof(std::underlying_type_t<Bytecode>) + OPERANDS_SIZE;
};

}  // namespace noisepage::execution::vm
