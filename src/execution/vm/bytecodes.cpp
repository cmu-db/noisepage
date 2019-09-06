#include "execution/vm/bytecodes.h"

#include <algorithm>

#include "execution/vm/bytecode_traits.h"

namespace terrier::execution::vm {

// static
const char *Bytecodes::k_bytecode_names[] = {
#define ENTRY(name, ...) #name,
    BYTECODE_LIST(ENTRY)
#undef ENTRY
};

// static
uint32_t Bytecodes::k_bytecode_operand_counts[] = {
#define ENTRY(name, ...) BytecodeTraits<__VA_ARGS__>::K_OPERAND_COUNT,
    BYTECODE_LIST(ENTRY)
#undef ENTRY
};

// static
const OperandType *Bytecodes::k_bytecode_operand_types[] = {
#define ENTRY(name, ...) BytecodeTraits<__VA_ARGS__>::k_operand_types,
    BYTECODE_LIST(ENTRY)
#undef ENTRY
};

// static
const OperandSize *Bytecodes::k_bytecode_operand_sizes[] = {
#define ENTRY(name, ...) BytecodeTraits<__VA_ARGS__>::k_operand_sizes,
    BYTECODE_LIST(ENTRY)
#undef ENTRY
};

// static
const char *Bytecodes::k_bytecode_handler_name[] = {
#define ENTRY(name, ...) "Op" #name,
    BYTECODE_LIST(ENTRY)
#undef ENTRY
};

// static
uint32_t Bytecodes::MaxBytecodeNameLength() {
  static constexpr const uint32_t k_max_inst_name_length = std::max({
#define ENTRY(name, ...) sizeof(#name),
      BYTECODE_LIST(ENTRY)
#undef ENTRY
  });
  return k_max_inst_name_length;
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
