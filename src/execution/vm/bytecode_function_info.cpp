#include "execution/vm/bytecode_function_info.h"

#include <string>
#include <utility>
#include <vector>

#include "common/math_util.h"
#include "execution/ast/type.h"

namespace noisepage::execution::vm {

// ---------------------------------------------------------
// Local Information
// ---------------------------------------------------------

LocalInfo::LocalInfo(std::string name, ast::Type *type, uint32_t offset, LocalInfo::Kind kind) noexcept
    : name_(std::move(name)), type_(type), offset_(offset), size_(type->GetSize()), kind_(kind) {}

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
  NOISEPAGE_ASSERT(!name.empty(), "Local name cannot be empty");

  // Bump size to account for the alignment of the new local
  if (!common::MathUtil::IsAligned(frame_size_, type->GetAlignment())) {
    frame_size_ = common::MathUtil::AlignTo(frame_size_, type->GetAlignment());
  }

  const auto offset = static_cast<uint32_t>(frame_size_);
  locals_.emplace_back(name, type, offset, kind);

  frame_size_ += type->GetSize();

  return LocalVar(offset, LocalVar::AddressMode::Address);
}

LocalVar FunctionInfo::NewParameterLocal(ast::Type *type, const std::string &name) {
  const LocalVar local = NewLocal(type, name, LocalInfo::Kind::Parameter);
  num_params_++;
  params_size_ = GetFrameSize();
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
  NOISEPAGE_ASSERT(!func_type_->GetReturnType()->IsNilType(),
                   "Cannot lookup local slot for function that does not have return value");
  return LocalVar(0u, LocalVar::AddressMode::Address);
}

const LocalInfo *FunctionInfo::LookupLocalInfoByName(const std::string &name) const {
  const auto iter =
      std::find_if(locals_.begin(), locals_.end(), [&](const auto &info) { return info.GetName() == name; });
  return iter == locals_.end() ? nullptr : &*iter;
}

LocalVar FunctionInfo::LookupLocal(const std::string &name) const {
  const LocalInfo *local_info = LookupLocalInfoByName(name);
  return local_info == nullptr ? LocalVar() : LocalVar(local_info->GetOffset(), LocalVar::AddressMode::Address);
}

const LocalInfo *FunctionInfo::LookupLocalInfoByOffset(uint32_t offset) const {
  for (const auto &local_info : GetLocals()) {
    if (local_info.GetOffset() == offset) {
      return &local_info;
    }
  }

  return nullptr;
}

}  // namespace noisepage::execution::vm
