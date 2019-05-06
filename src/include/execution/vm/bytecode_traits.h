#pragma once

#include "execution/vm/bytecode_operands.h"
#include "execution/vm/bytecodes.h"

namespace tpl::vm {

template <OperandType>
struct OperandTypeTraits {
  static constexpr bool kIsSigned = false;
  static constexpr OperandSize kOperandSize = OperandSize::None;
  static constexpr u32 kSize = static_cast<u32>(kOperandSize);
};

#define DECLARE_OPERAND_TYPE(Name, IsSigned, BaseSize)           \
  template <>                                                    \
  struct OperandTypeTraits<OperandType::Name> {                  \
    static constexpr bool kIsSigned = IsSigned;                  \
    static constexpr OperandSize kOperandSize = BaseSize;        \
    static constexpr u32 kSize = static_cast<u32>(kOperandSize); \
  };
OPERAND_TYPE_LIST(DECLARE_OPERAND_TYPE)
#undef DECLARE_OPERAND_TYPE

template <OperandType... operands>
struct BytecodeTraits {
  static constexpr const u32 kOperandCount = sizeof...(operands);
  static constexpr const u32 kOperandsSize =
      (0u + ... + OperandTypeTraits<operands>::kSize);
  static constexpr const OperandType kOperandTypes[] = {operands...};
  static constexpr const OperandSize kOperandSizes[] = {
      OperandTypeTraits<operands>::kOperandSize...};
  static constexpr const u32 kSize =
      sizeof(std::underlying_type_t<Bytecode>) + kOperandsSize;
};

}  // namespace tpl::vm
