#include "execution/vm/bytecode_iterator.h"

#include <vector>

#include "execution/vm/bytecode_function_info.h"
#include "execution/vm/bytecode_traits.h"

namespace tpl::vm {

BytecodeIterator::BytecodeIterator(const std::vector<u8> &bytecode, std::size_t start, std::size_t end)
    : bytecodes_(bytecode), start_offset_(start), end_offset_(end), curr_offset_(start) {}

Bytecode BytecodeIterator::CurrentBytecode() const {
  auto raw_code = *reinterpret_cast<const std::underlying_type_t<Bytecode> *>(&bytecodes_[curr_offset_]);
  return Bytecodes::FromByte(raw_code);
}

bool BytecodeIterator::Done() const { return curr_offset_ >= end_offset_; }

void BytecodeIterator::Advance() { curr_offset_ += CurrentBytecodeSize(); }

i64 BytecodeIterator::GetImmediateOperand(u32 operand_index) const {
  OperandType operand_type = Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index);
  TPL_ASSERT(OperandTypes::IsSignedImmediate(operand_type), "Operand type is not a signed immediate");

  const u8 *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  switch (operand_type) {
    case OperandType::Imm1: {
      return *reinterpret_cast<const i8 *>(operand_address);
    }
    case OperandType::Imm2: {
      return *reinterpret_cast<const i16 *>(operand_address);
    }
    case OperandType::Imm4: {
      return *reinterpret_cast<const i32 *>(operand_address);
    }
    case OperandType::Imm8: {
      return *reinterpret_cast<const i64 *>(operand_address);
    }
    default: { UNREACHABLE("Impossible!"); }
  }
}

f64 BytecodeIterator::GetFloatImmediateOperand(u32 operand_index) const {
  OperandType operand_type = Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index);
  TPL_ASSERT(OperandTypes::IsFloatImmediate(operand_type), "Operand type is not a float immediate");

  const u8 *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  switch (operand_type) {
    case OperandType::Imm4F: {
      return *reinterpret_cast<const f32 *>(operand_address);
    }
    case OperandType::Imm8F: {
      return *reinterpret_cast<const f64 *>(operand_address);
    }
    default: { UNREACHABLE("Impossible!"); }
  }
}

u64 BytecodeIterator::GetUnsignedImmediateOperand(u32 operand_index) const {
  OperandType operand_type = Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index);
  TPL_ASSERT(OperandTypes::IsUnsignedImmediate(operand_type), "Operand type is not a signed immediate");

  const u8 *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  switch (operand_type) {
    case OperandType::UImm2: {
      return *reinterpret_cast<const u16 *>(operand_address);
    }
    case OperandType::UImm4: {
      return *reinterpret_cast<const u32 *>(operand_address);
    }
    default: { UNREACHABLE("Impossible!"); }
  }
}

i32 BytecodeIterator::GetJumpOffsetOperand(u32 operand_index) const {
  TPL_ASSERT(Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index) == OperandType::JumpOffset,
             "Operand isn't a jump offset");
  const u8 *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  return *reinterpret_cast<const i32 *>(operand_address);
}

LocalVar BytecodeIterator::GetLocalOperand(u32 operand_index) const {
  TPL_ASSERT(OperandTypes::IsLocal(Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index)),
             "Operand type is not a local variable reference");
  const u8 *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  auto encoded_val = *reinterpret_cast<const u32 *>(operand_address);
  return LocalVar::Decode(encoded_val);
}

u16 BytecodeIterator::GetLocalCountOperand(u32 operand_index, std::vector<LocalVar> *locals) const {
  TPL_ASSERT(OperandTypes::IsLocalCount(Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index)),
             "Operand type is not a local variable count");

  const u8 *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  u16 num_locals = *reinterpret_cast<const u16 *>(operand_address);

  const u8 *locals_address = operand_address + OperandTypeTraits<OperandType::LocalCount>::kSize;

  for (u32 i = 0; i < num_locals; i++) {
    auto encoded_val = *reinterpret_cast<const u32 *>(locals_address);
    locals->push_back(LocalVar::Decode(encoded_val));

    locals_address += OperandTypeTraits<OperandType::Local>::kSize;
  }

  return num_locals;
}

u16 BytecodeIterator::GetFunctionIdOperand(u32 operand_index) const {
  TPL_ASSERT(Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index) == OperandType::FunctionId,
             "Operand type is not a function");

  const u8 *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  return *reinterpret_cast<const u16 *>(operand_address);
}

u32 BytecodeIterator::CurrentBytecodeSize() const {
  Bytecode bytecode = CurrentBytecode();
  u32 size = sizeof(std::underlying_type_t<Bytecode>);
  for (u32 i = 0; i < Bytecodes::NumOperands(bytecode); i++) {
    size += static_cast<u32>(Bytecodes::GetNthOperandSize(bytecode, i));
    if (Bytecodes::GetNthOperandType(bytecode, i) == OperandType::LocalCount) {
      auto num_locals = GetLocalCountOperand(i);
      size += (num_locals * OperandTypeTraits<OperandType::Local>::kSize);
    }
  }
  return size;
}

}  // namespace tpl::vm
