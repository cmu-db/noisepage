#include "execution/vm/bytecode_function_info.h"

#include <string>
#include <utility>
#include <vector>

#include "common/math_util.h"
#include "execution/ast/type.h"

namespace terrier::execution::vm {

// ---------------------------------------------------------
// Local Information
// ---------------------------------------------------------

LocalInfo::LocalInfo(std::string name, ast::Type *type, uint32_t offset, LocalInfo::Kind kind) noexcept
    : name_(std::move(name)), type_(type), offset_(offset), size_(type->Size()), kind_(kind) {}

// ---------------------------------------------------------
// Function Information
// ---------------------------------------------------------

FunctionInfo::FunctionInfo(FunctionId id, std::string name, ast::FunctionType *func_type)
    : id_(id),
      name_(std::move(name)),
      func_type_(func_type),
      bytecode_range_(std::make_pair(0, 0)),
      frame_size_(0),
      params_start_pos_(0),
      params_size_(0),
      num_params_(0),
      num_temps_(0) {}

LocalVar FunctionInfo::NewLocal(ast::Type *type, const std::string &name, LocalInfo::Kind kind) {
  TERRIER_ASSERT(!name.empty(), "Local name cannot be empty");

  // Bump size to account for the alignment of the new local
  if (!common::MathUtil::IsAligned(frame_size_, type->Alignment())) {
    frame_size_ = common::MathUtil::AlignTo(frame_size_, type->Alignment());
  }

  const auto offset = static_cast<uint32_t>(frame_size_);
  locals_.emplace_back(name, type, offset, kind);

  frame_size_ += type->Size();

  return LocalVar(offset, LocalVar::AddressMode::Address);
}

LocalVar FunctionInfo::NewParameterLocal(ast::Type *type, const std::string &name) {
  const LocalVar local = NewLocal(type, name, LocalInfo::Kind::Parameter);
  num_params_++;
  params_size_ = FrameSize();
  return local;
}

LocalVar FunctionInfo::NewLocal(ast::Type *type, const std::string &name) {
  if (name.empty()) {
    const auto tmp_name = "tmp" + std::to_string(++num_temps_);
    return NewLocal(type, tmp_name, LocalInfo::Kind::Var);
  }

  return NewLocal(type, name, LocalInfo::Kind::Var);
}

LocalVar FunctionInfo::GetReturnValueLocal() const {
  // This invocation only makes sense if the function actually returns a value
  TERRIER_ASSERT(!func_type_->ReturnType()->IsNilType(),
                 "Cannot lookup local slot for function that does not have return value");
  return LocalVar(0u, LocalVar::AddressMode::Address);
}

LocalVar FunctionInfo::LookupLocal(const std::string &name) const {
  for (const auto &local_info : Locals()) {
    if (local_info.Name() == name) {
      return LocalVar(local_info.Offset(), LocalVar::AddressMode::Address);
    }
  }

  return LocalVar();
}

const LocalInfo *FunctionInfo::LookupLocalInfoByName(const std::string &name) const {
  for (const auto &local_info : Locals()) {
    if (local_info.Name() == name) {
      return &local_info;
    }
  }

  return nullptr;
}

const LocalInfo *FunctionInfo::LookupLocalInfoByOffset(uint32_t offset) const {
  for (const auto &local_info : Locals()) {
    if (local_info.Offset() == offset) {
      return &local_info;
    }
  }

  return nullptr;
}

}  // namespace terrier::execution::vm
