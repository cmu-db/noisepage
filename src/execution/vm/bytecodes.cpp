#include "execution/vm/bytecodes.h"

#include <algorithm>

#include "execution/vm/bytecode_traits.h"

namespace terrier::execution::vm {

// static
const char *Bytecodes::kBytecodeNames[] = {
#define ENTRY(name, ...) #name,
    BYTECODE_LIST(ENTRY)
#undef ENTRY
};

// static
uint32_t Bytecodes::kBytecodeOperandCounts[] = {
#define ENTRY(name, ...) BytecodeTraits<__VA_ARGS__>::kOperandCount,
    BYTECODE_LIST(ENTRY)
#undef ENTRY
};

// static
const OperandType *Bytecodes::kBytecodeOperandTypes[] = {
#define ENTRY(name, ...) BytecodeTraits<__VA_ARGS__>::kOperandTypes,
    BYTECODE_LIST(ENTRY)
#undef ENTRY
};

// static
const OperandSize *Bytecodes::kBytecodeOperandSizes[] = {
#define ENTRY(name, ...) BytecodeTraits<__VA_ARGS__>::kOperandSizes,
    BYTECODE_LIST(ENTRY)
#undef ENTRY
};

// static
const char *Bytecodes::kBytecodeHandlerName[] = {
#define ENTRY(name, ...) "Op" #name,
    BYTECODE_LIST(ENTRY)
#undef ENTRY
};

// static
uint32_t Bytecodes::MaxBytecodeNameLength() {
  static constexpr const uint32_t kMaxInstNameLength = std::max({
#define ENTRY(name, ...) sizeof(#name),
      BYTECODE_LIST(ENTRY)
#undef ENTRY
  });
  return kMaxInstNameLength;
}

uint32_t Bytecodes::GetNthOperandOffset(Bytecode bytecode, uint32_t operand_index) {
  TERRIER_ASSERT(operand_index < NumOperands(bytecode), "Invalid operand index");
  uint32_t offset = sizeof(std::underlying_type_t<Bytecode>);
  for (uint32_t i = 0; i < operand_index; i++) {
    OperandSize operand_size = GetNthOperandSize(bytecode, i);
    offset += static_cast<uint32_t>(operand_size);
  }
  return offset;
}

}  // namespace terrier::execution::vm
