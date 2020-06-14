#pragma once

#include "execution/vm/bytecode_operands.h"
#include "execution/vm/bytecodes.h"

namespace terrier::execution::vm {

template <OperandType>
struct OperandTypeTraits {
  static constexpr bool kIsSigned = false;
  static constexpr OperandSize kOperandSize = OperandSize::None;
  static constexpr uint32_t kSize = static_cast<uint32_t>(kOperandSize);
};

#define DECLARE_OPERAND_TYPE(Name, IsSigned, BaseSize)                     \
  template <>                                                              \
  struct OperandTypeTraits<OperandType::Name> {                            \
    static constexpr bool kIsSigned = IsSigned;                            \
    static constexpr OperandSize kOperandSize = BaseSize;                  \
    static constexpr uint32_t kSize = static_cast<uint32_t>(kOperandSize); \
  };
OPERAND_TYPE_LIST(DECLARE_OPERAND_TYPE)
#undef DECLARE_OPERAND_TYPE

template <OperandType... operands>
struct BytecodeTraits {
  static constexpr const uint32_t kOperandCount = sizeof...(operands);
  static constexpr const uint32_t kOperandsSize = (0u + ... + OperandTypeTraits<operands>::kSize);
  static constexpr const OperandType kOperandTypes[] = {operands...};
  static constexpr const OperandSize kOperandSizes[] = {OperandTypeTraits<operands>::kOperandSize...};
  static constexpr const uint32_t kSize = sizeof(std::underlying_type_t<Bytecode>) + kOperandsSize;
};

}  // namespace terrier::execution::vm
