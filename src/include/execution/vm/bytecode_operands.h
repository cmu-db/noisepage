#pragma once

namespace noisepage::execution::vm {

/**
 * This enumeration lists all possible <b>sizes</b> of operands to any bytecode.
 */
enum class OperandSize : uint8_t { None = 0, Byte = 1, Short = 2, Int = 4, Long = 8 };

// This macro list provides information about all possible operand types to a bytecode operation.
// The format is: Name, IsSigned, BaseSize
#define OPERAND_TYPE_LIST(V)               \
  V(None, false, OperandSize::None)        \
  V(Imm1, true, OperandSize::Byte)         \
  V(Imm2, true, OperandSize::Short)        \
  V(Imm4, true, OperandSize::Int)          \
  V(Imm8, true, OperandSize::Long)         \
  V(Imm4F, true, OperandSize::Int)         \
  V(Imm8F, true, OperandSize::Long)        \
  V(UImm2, false, OperandSize::Short)      \
  V(UImm4, false, OperandSize::Int)        \
  V(JumpOffset, true, OperandSize::Int)    \
  V(Local, false, OperandSize::Int)        \
  V(StaticLocal, false, OperandSize::Int)  \
  V(LocalCount, false, OperandSize::Short) \
  V(FunctionId, false, OperandSize::Short)

/**
 * This enumeration lists all possible <b>types</b> of operands to any bytecode.
 */
enum class OperandType : uint8_t {
#define OP_TYPE(Name, ...) Name,
  OPERAND_TYPE_LIST(OP_TYPE)
#undef OP_TYPE
};

/**
 * Helper class to query operand types.
 */
class OperandTypes {
 public:
  /**
   * @return True if @em operand_type is a signed integer immediate operand; false otherwise.
   */
  static constexpr bool IsSignedIntegerImmediate(OperandType operand_type) {
    return operand_type == OperandType::Imm1 || operand_type == OperandType::Imm2 ||
           operand_type == OperandType::Imm4 || operand_type == OperandType::Imm8;
  }

  /**
   * @return True if @em operand_type is an unsigned integer immediate operand; false otherwise.
   */
  static constexpr bool IsUnsignedIntegerImmediate(OperandType operand_type) {
    return operand_type == OperandType::UImm2 || operand_type == OperandType::UImm4;
  }

  /**
   * @return True if @em operand_type is a floating pointer immediate operand; false otherwise.
   */
  static constexpr bool IsFloatImmediate(OperandType operand_type) {
    return operand_type == OperandType::Imm4F || operand_type == OperandType::Imm8F;
  }

  /**
   * @return True if @em operand_type is a local-reference operand; false otherwise.
   */
  static constexpr bool IsLocal(OperandType operand_type) { return operand_type == OperandType::Local; }

  /**
   * @return True if @em operand_type is a static-local reference operand; false otherwise.
   */
  static constexpr bool IsStaticLocal(OperandType operand_type) { return operand_type == OperandType::StaticLocal; }

  /**
   * @return True if @em operand_type is a count of locals following this operand; false otherwise.
   */
  static constexpr bool IsLocalCount(OperandType operand_type) { return operand_type == OperandType::LocalCount; }

  /** @return The maximum jump offset. */
  static int32_t MaxJumpOffset();
};

}  // namespace noisepage::execution::vm
