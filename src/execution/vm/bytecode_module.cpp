#include "execution/vm/bytecode_module.h"

#include <algorithm>
#include <iomanip>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "execution/ast/type.h"
#include "execution/vm/bytecode_iterator.h"

namespace noisepage::execution::vm {

BytecodeModule::BytecodeModule(std::string name, std::vector<uint8_t> &&code, std::vector<uint8_t> &&data,
                               std::vector<FunctionInfo> &&functions, std::vector<LocalInfo> &&static_locals)
    : name_(std::move(name)),
      code_(std::move(code)),
      data_(std::move(data)),
      functions_(std::move(functions)),
      static_locals_(std::move(static_locals)) {}

std::size_t BytecodeModule::GetInstructionCount() const {
  std::size_t count = 0;
  for (BytecodeIterator iter(code_); !iter.Done(); iter.Advance()) {
    count++;
  }
  return count;
}

namespace {

void PrettyPrintStaticLocals(std::ostream &os, const BytecodeModule &module, const std::size_t size,
                             const std::function<const char *(uint32_t)> &static_access_fn) {
  os << std::endl << "Data: " << std::endl;
  os << "  Data section size " << size << " bytes"
     << " (" << module.GetStaticLocalsCount() << " locals)" << std::endl;

  uint64_t max_local_len = 0;
  for (const auto &local : module.GetStaticLocalsInfo()) {
    max_local_len = std::max(max_local_len, static_cast<uint64_t>(local.GetName().length()));
  }
  for (const auto &local : module.GetStaticLocalsInfo()) {
    const auto local_data = std::string_view(static_access_fn(local.GetOffset()), local.GetSize());
    os << "     " << std::setw(max_local_len) << std::right << local.GetName() << ": ";
    os << " offset=" << std::setw(7) << std::left << local.GetOffset();
    os << " size=" << std::setw(7) << std::left << local.GetSize();
    os << " align=" << std::setw(7) << std::left << local.GetType()->GetAlignment();
    os << " data=\"" << local_data << "\"" << std::endl;
  }

  os << std::endl;
}

void PrettyPrintFuncInfo(std::ostream &os, const FunctionInfo &func) {
  os << "Function " << func.GetId() << " <" << func.GetName() << ">:" << std::endl;
  os << "  Frame size " << func.GetFrameSize() << " bytes (" << func.GetParamsCount() << " parameter"
     << (func.GetParamsCount() > 1 ? "s, " : ", ") << func.GetLocals().size() << " locals)" << std::endl;

  uint64_t max_local_len = 0;
  for (const auto &local : func.GetLocals()) {
    max_local_len = std::max(max_local_len, static_cast<uint64_t>(local.GetName().length()));
  }
  for (const auto &local : func.GetLocals()) {
    if (local.IsParameter()) {
      os << "    param  ";
    } else {
      os << "    local  ";
    }
    os << std::setw(max_local_len) << std::right << local.GetName() << ": ";
    os << " offset=" << std::setw(7) << std::left << local.GetOffset();
    os << " size=" << std::setw(7) << std::left << local.GetSize();
    os << " align=" << std::setw(7) << std::left << local.GetType()->GetAlignment();
    os << " type=" << std::setw(7) << std::left << ast::Type::ToString(local.GetType());
    os << std::endl;
  }
}

void PrettyPrintFuncCode(std::ostream &os, const BytecodeModule &module, const FunctionInfo &func,
                         BytecodeIterator *iter) {
  const uint32_t max_inst_len = Bytecodes::MaxBytecodeNameLength();
  for (; !iter->Done(); iter->Advance()) {
    Bytecode bytecode = iter->CurrentBytecode();

    // Print common bytecode info
    os << "  0x" << std::right << std::setfill('0') << std::setw(8) << std::hex << iter->GetPosition();
    os << std::setfill(' ') << "    " << std::dec << std::setw(max_inst_len) << std::left
       << Bytecodes::ToString(bytecode);

    auto print_local = [&](const LocalVar local) {
      const auto *local_info = func.LookupLocalInfoByOffset(local.GetOffset());
      NOISEPAGE_ASSERT(local_info, "No local at offset");

      os << "local=";
      if (local.GetAddressMode() == LocalVar::AddressMode::Address) {
        os << "&";
      }
      os << local_info->GetName();
    };

    for (uint32_t i = 0; i < Bytecodes::NumOperands(bytecode); i++) {
      if (i != 0) os << "  ";
      switch (Bytecodes::GetNthOperandType(bytecode, i)) {
        case OperandType::None: {
          break;
        }
        case OperandType::Imm1: {
          auto imm = iter->GetImmediateIntegerOperand(i);
          os << "i8=" << imm;
          break;
        }
        case OperandType::Imm2: {
          auto imm = iter->GetImmediateIntegerOperand(i);
          os << "i16=" << imm;
          break;
        }
        case OperandType::Imm4: {
          auto imm = iter->GetImmediateIntegerOperand(i);
          os << "i32=" << imm;
          break;
        }
        case OperandType::Imm8: {
          auto imm = iter->GetImmediateIntegerOperand(i);
          os << "i64=" << imm;
          break;
        }
        case OperandType::Imm4F: {
          auto imm = iter->GetImmediateFloatOperand(i);
          os << "f32=" << std::fixed << std::setprecision(2) << imm << std::dec;
          break;
        }
        case OperandType::Imm8F: {
          auto imm = iter->GetImmediateFloatOperand(i);
          os << "f64=" << std::fixed << std::setprecision(2) << imm << std::dec;
          break;
        }
        case OperandType::UImm2: {
          auto imm = iter->GetUnsignedImmediateIntegerOperand(i);
          os << "u16=" << imm;
          break;
        }
        case OperandType::UImm4: {
          auto imm = iter->GetUnsignedImmediateIntegerOperand(i);
          os << "u32=" << imm;
          break;
        }
        case OperandType::JumpOffset: {
          auto target =
              iter->GetPosition() + Bytecodes::GetNthOperandOffset(bytecode, i) + iter->GetJumpOffsetOperand(i);
          os << "target=0x" << std::right << std::setfill('0') << std::setw(6) << std::hex << target << std::dec;
          break;
        }
        case OperandType::Local: {
          print_local(iter->GetLocalOperand(i));
          break;
        }
        case OperandType::StaticLocal: {
          auto sl_offset = iter->GetStaticLocalOperand(i);
          auto sl_info = module.LookupStaticInfoByOffset(sl_offset.GetOffset());
          os << "data=" << sl_info->GetName();
          break;
        }
        case OperandType::LocalCount: {
          std::vector<LocalVar> locals;
          uint16_t n = iter->GetLocalCountOperand(i, &locals);
          for (uint16_t j = 0; j < n; j++) {
            print_local(locals[j]);
            os << "  ";
          }
          break;
        }
        case OperandType::FunctionId: {
          auto target = module.GetFuncInfoById(iter->GetFunctionIdOperand(i));
          os << "func=<" << target->GetName() << ">";
          break;
        }
      }
    }

    os << std::endl;
  }
}

void PrettyPrintFunc(std::ostream &os, const BytecodeModule &module, const FunctionInfo &func) {
  PrettyPrintFuncInfo(os, func);

  os << std::endl;

  BytecodeIterator iter = module.GetBytecodeForFunction(func);
  PrettyPrintFuncCode(os, module, func, &iter);

  os << std::endl;
}

}  // namespace

void BytecodeModule::Dump(std::ostream &os) const {
  // Static locals
  PrettyPrintStaticLocals(os, *this, data_.size(), [this](uint32_t offset) {
    return reinterpret_cast<const char *>(AccessStaticLocalDataRaw(offset));
  });

  // Functions
  for (const auto &func : functions_) {
    PrettyPrintFunc(os, *this, func);
  }
}

}  // namespace noisepage::execution::vm
