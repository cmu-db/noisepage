#pragma once

#include "execution/util/common.h"

namespace terrier::vm {

/**
 * This enumeration lists all possible sizes of operands to any bytecode
 */
enum class OperandSize : u8 { None = 0, Byte = 1, Short = 2, Int = 4, Long = 8, Float = 4, Double = 8 };

/**
 * This macro list provides information about all possible operand types to a
 * bytecode operation. The format is: Name, IsSigned, BaseSize
 */
#define OPERAND_TYPE_LIST(V)               \
  V(None, false, OperandSize::None)        \
  V(Imm1, true, OperandSize::Byte)         \
  V(Imm2, true, OperandSize::Short)        \
  V(Imm4, true, OperandSize::Int)          \
  V(Imm8, true, OperandSize::Long)         \
  V(Imm4F, true, OperandSize::Float)       \
  V(Imm8F, true, OperandSize::Double)      \
  V(UImm2, false, OperandSize::Short)      \
  V(UImm4, false, OperandSize::Int)        \
  V(JumpOffset, true, OperandSize::Int)    \
  V(Local, false, OperandSize::Int)        \
  V(LocalCount, false, OperandSize::Short) \
  V(FunctionId, false, OperandSize::Short)

/**
 * This enumeration lists all possible types of operands to any bytecode
 */
enum class OperandType : u8 {
#define OP_TYPE(Name, ...) Name,
  OPERAND_TYPE_LIST(OP_TYPE)
#undef OP_TYPE
};

/**
 * Helper class to query operand types
 */
class OperandTypes {
 public:
  /**
   * @param operand_type operand to check
   * @return whether the operand is a signed immediate type
   */
  static constexpr bool IsSignedImmediate(OperandType operand_type) {
    return operand_type == OperandType::Imm1 || operand_type == OperandType::Imm2 ||
           operand_type == OperandType::Imm4 || operand_type == OperandType::Imm8;
  }

  /**
   * @param operand_type operand to check
   * @return whether the operand is a float immediate type
   */
  static constexpr bool IsFloatImmediate(OperandType operand_type) {
    return operand_type == OperandType::Imm4F || operand_type == OperandType::Imm8F;
  }

  /**
   * @param operand_type operand to check
   * @return whether the operand is an unsigned immediate type
   */
  static constexpr bool IsUnsignedImmediate(OperandType operand_type) {
    return operand_type == OperandType::UImm2 || operand_type == OperandType::UImm4;
  }

  /**
   * @param operand_type operand to check
   * @return whether the operand is a Local
   */
  static constexpr bool IsLocal(OperandType operand_type) { return operand_type == OperandType::Local; }

  /**
   * @param operand_type operand to check
   * @return whether the operand is a LocalCount
   */
  static constexpr bool IsLocalCount(OperandType operand_type) { return operand_type == OperandType::LocalCount; }

  /**
   * @return the maximum jump offset
   */
  static i32 MaxJumpOffset();
};

}  // namespace terrier::vm
