#include "execution/vm/bytecode_iterator.h"

#include <vector>

#include "execution/vm/bytecode_function_info.h"
#include "execution/vm/bytecode_traits.h"

namespace noisepage::execution::vm {

BytecodeIterator::BytecodeIterator(const std::vector<uint8_t> &bytecode, std::size_t start, std::size_t end)
    : bytecodes_(bytecode), start_offset_(start), end_offset_(end), curr_offset_(start) {}

BytecodeIterator::BytecodeIterator(const std::vector<uint8_t> &bytecode)
    : BytecodeIterator(bytecode, 0, bytecode.size()) {}

Bytecode BytecodeIterator::CurrentBytecode() const {
  auto raw_code = *reinterpret_cast<const std::underlying_type_t<Bytecode> *>(&bytecodes_[curr_offset_]);
  return Bytecodes::FromByte(raw_code);
}

bool BytecodeIterator::Done() const { return curr_offset_ >= end_offset_; }

void BytecodeIterator::Advance() { curr_offset_ += CurrentBytecodeSize(); }

int64_t BytecodeIterator::GetImmediateIntegerOperand(uint32_t operand_index) const {
  OperandType operand_type = Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index);
  NOISEPAGE_ASSERT(OperandTypes::IsSignedIntegerImmediate(operand_type), "Operand type is not a signed immediate");

  const uint8_t *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  switch (operand_type) {
    case OperandType::Imm1:
      return *reinterpret_cast<const int8_t *>(operand_address);
    case OperandType::Imm2:
      return *reinterpret_cast<const int16_t *>(operand_address);
    case OperandType::Imm4:
      return *reinterpret_cast<const int32_t *>(operand_address);
    case OperandType::Imm8:
      return *reinterpret_cast<const int64_t *>(operand_address);
    default: {
      UNREACHABLE("Impossible!");
    }
  }
}

double BytecodeIterator::GetImmediateFloatOperand(uint32_t operand_index) const {
  OperandType operand_type = Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index);
  NOISEPAGE_ASSERT(OperandTypes::IsFloatImmediate(operand_type), "Operand type is not a float immediate");

  const uint8_t *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  switch (operand_type) {
    case OperandType::Imm4F:
      return *reinterpret_cast<const float *>(operand_address);
    case OperandType::Imm8F:
      return *reinterpret_cast<const double *>(operand_address);
    default:
      UNREACHABLE("Impossible!");
  }
}

uint64_t BytecodeIterator::GetUnsignedImmediateIntegerOperand(uint32_t operand_index) const {
  OperandType operand_type = Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index);
  NOISEPAGE_ASSERT(OperandTypes::IsUnsignedIntegerImmediate(operand_type), "Operand type is not an unsigned immediate");

  const uint8_t *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  switch (operand_type) {
    case OperandType::UImm2:
      return *reinterpret_cast<const uint16_t *>(operand_address);
    case OperandType::UImm4:
      return *reinterpret_cast<const uint32_t *>(operand_address);
    default: {
      UNREACHABLE("Impossible!");
    }
  }
}

int32_t BytecodeIterator::GetJumpOffsetOperand(uint32_t operand_index) const {
  NOISEPAGE_ASSERT(Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index) == OperandType::JumpOffset,
                   "Operand isn't a jump offset");
  const uint8_t *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  return *reinterpret_cast<const int32_t *>(operand_address);
}

LocalVar BytecodeIterator::GetLocalOperand(uint32_t operand_index) const {
  NOISEPAGE_ASSERT(OperandTypes::IsLocal(Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index)),
                   "Operand type is not a local variable reference");

  const uint8_t *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);
  const auto encoded_val = *reinterpret_cast<const uint32_t *>(operand_address);
  return LocalVar::Decode(encoded_val);
}

LocalVar BytecodeIterator::GetStaticLocalOperand(uint32_t operand_index) const {
  NOISEPAGE_ASSERT(OperandTypes::IsStaticLocal(Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index)),
                   "Operand type is not a static local reference");

  const uint8_t *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);
  const auto encoded_val = *reinterpret_cast<const uint32_t *>(operand_address);
  return LocalVar::Decode(encoded_val);
}

uint16_t BytecodeIterator::GetLocalCountOperand(uint32_t operand_index, std::vector<LocalVar> *locals) const {
  NOISEPAGE_ASSERT(OperandTypes::IsLocalCount(Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index)),
                   "Operand type is not a local variable count");

  const uint8_t *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  uint16_t num_locals = *reinterpret_cast<const uint16_t *>(operand_address);

  const uint8_t *locals_address = operand_address + OperandTypeTraits<OperandType::LocalCount>::SIZE;

  for (uint32_t i = 0; i < num_locals; i++) {
    auto encoded_val = *reinterpret_cast<const uint32_t *>(locals_address);
    locals->push_back(LocalVar::Decode(encoded_val));

    locals_address += OperandTypeTraits<OperandType::Local>::SIZE;
  }

  return num_locals;
}

uint16_t BytecodeIterator::GetFunctionIdOperand(uint32_t operand_index) const {
  NOISEPAGE_ASSERT(Bytecodes::GetNthOperandType(CurrentBytecode(), operand_index) == OperandType::FunctionId,
                   "Operand type is not a function");

  const uint8_t *operand_address =
      bytecodes_.data() + curr_offset_ + Bytecodes::GetNthOperandOffset(CurrentBytecode(), operand_index);

  return *reinterpret_cast<const uint16_t *>(operand_address);
}

uint32_t BytecodeIterator::CurrentBytecodeSize() const {
  Bytecode bytecode = CurrentBytecode();
  uint32_t size = sizeof(std::underlying_type_t<Bytecode>);
  for (uint32_t i = 0; i < Bytecodes::NumOperands(bytecode); i++) {
    size += static_cast<uint32_t>(Bytecodes::GetNthOperandSize(bytecode, i));
    if (Bytecodes::GetNthOperandType(bytecode, i) == OperandType::LocalCount) {
      auto num_locals = GetLocalCountOperand(i);
      size += (num_locals * OperandTypeTraits<OperandType::Local>::SIZE);
    }
  }
  return size;
}

}  // namespace noisepage::execution::vm
