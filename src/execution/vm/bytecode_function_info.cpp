#include "execution/vm/bytecode_function_info.h"

#include <string>
#include <utility>
#include <vector>

#include "execution/ast/type.h"
#include "execution/util/math_util.h"

namespace terrier::execution::vm {

// ---------------------------------------------------------
// Local Information
// ---------------------------------------------------------

LocalInfo::LocalInfo(std::string name, ast::Type *type, u32 offset, LocalInfo::Kind kind) noexcept
    : name_(std::move(name)), type_(type), offset_(offset), size_(type->size()), kind_(kind) {}

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
  TPL_ASSERT(!name.empty(), "Local name cannot be empty");

  // Bump size to account for the alignment of the new local
  if (!util::MathUtil::IsAligned(frame_size_, type->alignment())) {
    frame_size_ = util::MathUtil::AlignTo(frame_size_, type->alignment());
  }

  const auto offset = static_cast<u32>(frame_size_);
  locals_.emplace_back(name, type, offset, kind);

  frame_size_ += type->size();

  return LocalVar(offset, LocalVar::AddressMode::Address);
}

LocalVar FunctionInfo::NewParameterLocal(ast::Type *type, const std::string &name) {
  const LocalVar local = NewLocal(type, name, LocalInfo::Kind::Parameter);
  num_params_++;
  params_size_ = frame_size();
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
  TPL_ASSERT(!func_type_->return_type()->IsNilType(),
             "Cannot lookup local slot for function that does not have return value");
  return LocalVar(0u, LocalVar::AddressMode::Address);
}

LocalVar FunctionInfo::LookupLocal(const std::string &name) const {
  for (const auto &local_info : locals()) {
    if (local_info.name() == name) {
      return LocalVar(local_info.offset(), LocalVar::AddressMode::Address);
    }
  }

  return LocalVar();
}

const LocalInfo *FunctionInfo::LookupLocalInfoByName(const std::string &name) const {
  for (const auto &local_info : locals()) {
    if (local_info.name() == name) {
      return &local_info;
    }
  }

  return nullptr;
}

const LocalInfo *FunctionInfo::LookupLocalInfoByOffset(u32 offset) const {
  for (const auto &local_info : locals()) {
    if (local_info.offset() == offset) {
      return &local_info;
    }
  }

  return nullptr;
}

}  // namespace terrier::execution::vm
