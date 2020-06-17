#pragma once

#include "execution/vm/bytecode_operands.h"
#include "execution/vm/bytecodes.h"

namespace terrier::execution::vm {

template <OperandType>
struct OperandTypeTraits {
  static constexpr bool IS_SIGNED = false;
  static constexpr OperandSize OPERAND_SIZE = OperandSize::None;
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

template <OperandType... operands>
struct BytecodeTraits {
  static constexpr const uint32_t kOperandCount = sizeof...(operands);
  static constexpr const uint32_t kOperandsSize = (0u + ... + OperandTypeTraits<operands>::SIZE);
  static constexpr const OperandType operand_types[] = {operands...};
  static constexpr const OperandSize operand_sizes[] = {OperandTypeTraits<operands>::OPERAND_SIZE...};
  static constexpr const uint32_t SIZE = sizeof(std::underlying_type_t<Bytecode>) + kOperandsSize;
};

}  // namespace terrier::execution::vm
