#pragma once

#include <string>

#include "execution/ast/ast.h"
#include "execution/ast/type.h"
#include "execution/compiler/codegen.h"
#include "execution/util/common.h"
#include "execution/util/region.h"
#include "execution/util/region_containers.h"

namespace tpl::compiler {

/**
 * QueryState is passed around as we traverse the plan nodes.
 * This allows the translators to register operator-specific state,
 * e.g. hash joins would require a hash table.
 */
class QueryState {
 public:
  using Id = u32;

  DISALLOW_COPY_AND_MOVE(QueryState);

  explicit QueryState(std::string struct_name, util::Region *region) :
      struct_name_(std::move(struct_name)), states_(region), constructed_type_(nullptr) {}

  ast::Expr *LoadStatePtr(CodeGen *codegen, QueryState::Id state_id) {
    TPL_ASSERT(constructed_type_ != nullptr, "Cannot index into non-finalized type.");
    TPL_ASSERT(state_id < states_.size(), "Out of bounds access.");

    const auto &state = states_[state_id];
    const auto struct_id = ast::Identifier(struct_name_.c_str());
    const auto member_id = ast::Identifier(state.name.c_str());
    return codegen->NewMemberExpr(struct_id, member_id);
  }

  ast::Expr *LoadStateValue(CodeGen *codegen, QueryState::Id state_id) {
    TPL_ASSERT(constructed_type_ != nullptr, "Cannot index into non-finalized type.");
    TPL_ASSERT(state_id < states_.size(), "Out of bounds access.");

    const auto &state = states_[state_id];
    const auto struct_id = ast::Identifier(struct_name_.c_str());
    const auto member_id = ast::Identifier(state.name.c_str());
    return codegen->NewMemberExpr(struct_id, member_id);
  }

  QueryState::Id RegisterState(std::string name, ast::Expr *type) {
    TPL_ASSERT(constructed_type_ == nullptr, "Can't register after finalizing.");
    const auto state_id = states_.size();
    states_.emplace_back(name, type);
    return state_id;
  }

  ast::Type *FinalizeType(CodeGen *codegen) {
    if (constructed_type_ != nullptr) { return constructed_type_; }

    util::RegionVector<ast::Field> fields(codegen->GetRegion());
    for (const auto &state:  states_) {
      auto id = ast::Identifier(state.name.c_str());
      auto field = codegen->NewField(id, state.type);
      fields.emplace_back(field);
    }

    constructed_type_ = codegen->NewStructType(std::move(fields));
    return constructed_type_;
  }

  ast::Type *GetType() const {
    TPL_ASSERT(constructed_type_ != nullptr, "Type hasn't been finalized.");
    return constructed_type_;
  }

 private:
  struct StateInfo {
    std::string name;
    ast::Type *type;
    ast::Expr *value;  // local states hold their value

    StateInfo(std::string name, ast::Type *type) : name(std::move(name)), type(type), value(nullptr) {}
  };

  std::string struct_name_;
  util::RegionVector<StateInfo> states_;
  ast::Type *constructed_type_;
};

}  // namespace tpl::compiler