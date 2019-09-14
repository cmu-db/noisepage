#include "execution/vm/bytecode_module.h"

#include <algorithm>
#include <iomanip>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "execution/ast/type.h"

namespace terrier::execution::vm {

BytecodeModule::BytecodeModule(std::string name, std::vector<uint8_t> &&code, std::vector<FunctionInfo> &&functions)
    : name_(std::move(name)), code_(std::move(code)), functions_(std::move(functions)) {}

namespace {

void PrettyPrintFuncInfo(std::ostream *os, const FunctionInfo &func) {
  *os << "Function " << func.Id() << " <" << func.Name() << ">:" << std::endl;
  *os << "  Frame size " << func.FrameSize() << " bytes (" << func.NumParams() << " parameter"
      << (func.NumParams() > 1 ? "s, " : ", ") << func.Locals().size() << " locals)" << std::endl;

  uint64_t max_local_len = 0;
  for (const auto &local : func.Locals()) {
    max_local_len = std::max(max_local_len, static_cast<uint64_t>(local.Name().length()));
  }
  for (const auto &local : func.Locals()) {
    if (local.IsParameter()) {
      *os << "    param  ";
    } else {
      *os << "    local  ";
    }
    *os << std::setw(static_cast<int>(max_local_len)) << std::right << local.Name() << ":  offset=" << std::setw(7)
        << std::left << local.Offset() << " size=" << std::setw(7) << std::left << local.Size()
        << " align=" << std::setw(7) << std::left << local.GetType()->Alignment() << " type=" << std::setw(7)
        << std::left << ast::Type::ToString(local.GetType()) << std::endl;
  }
}

void PrettyPrintFuncCode(std::ostream *os, const FunctionInfo &func, BytecodeIterator *iter) {
  const uint32_t max_inst_len = Bytecodes::MaxBytecodeNameLength();
  for (; !iter->Done(); iter->Advance()) {
    Bytecode bytecode = iter->CurrentBytecode();

    // Print common bytecode info
    *os << "  0x" << std::right << std::setfill('0') << std::setw(8) << std::hex << iter->GetPosition();
    *os << std::setfill(' ') << "    " << std::dec << std::setw(max_inst_len) << std::left
        << Bytecodes::ToString(bytecode) << std::endl;
  }
}

void PrettyPrintFunc(std::ostream *os, const BytecodeModule &module, const FunctionInfo &func) {
  PrettyPrintFuncInfo(os, func);

  *os << std::endl;

  auto iter = module.BytecodeForFunction(func);
  PrettyPrintFuncCode(os, func, &iter);

  *os << std::endl;
}

}  // namespace

void BytecodeModule::PrettyPrint(std::ostream *os) {
  for (const auto &func : functions_) {
    PrettyPrintFunc(os, *this, func);
  }
}

}  // namespace terrier::execution::vm
