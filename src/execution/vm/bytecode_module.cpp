#include "execution/vm/bytecode_module.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "execution/ast/type.h"

namespace terrier::vm {

BytecodeModule::BytecodeModule(std::string name, std::vector<u8> &&code, std::vector<FunctionInfo> &&functions)
    : name_(std::move(name)), code_(std::move(code)), functions_(std::move(functions)) {}

namespace {

void PrettyPrintFuncInfo(std::ostream *os, const FunctionInfo &func) {
  *os << "Function " << func.id() << " <" << func.name() << ">:" << std::endl;
  *os << "  Frame size " << func.frame_size() << " bytes (" << func.num_params() << " parameter"
      << (func.num_params() > 1 ? "s, " : ", ") << func.locals().size() << " locals)" << std::endl;

  u64 max_local_len = 0;
  for (const auto &local : func.locals()) {
    max_local_len = std::max(max_local_len, static_cast<u64>(local.name().length()));
  }
  for (const auto &local : func.locals()) {
    if (local.is_parameter()) {
      *os << "    param  ";
    } else {
      *os << "    local  ";
    }
    *os << std::setw(static_cast<int>(max_local_len)) << std::right << local.name() << ":  offset=" << std::setw(7)
        << std::left << local.offset() << " size=" << std::setw(7) << std::left << local.size()
        << " align=" << std::setw(7) << std::left << local.type()->alignment() << " type=" << std::setw(7) << std::left
        << ast::Type::ToString(local.type()) << std::endl;
  }
}

void PrettyPrintFuncCode(std::ostream *os, const FunctionInfo &func, BytecodeIterator *iter) {
  const u32 max_inst_len = Bytecodes::MaxBytecodeNameLength();
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

}  // namespace terrier::vm
