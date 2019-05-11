#include "execution/compiler/query_state.h"

#include "execution/ast/ast_node_factory.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compiler_defs.h"
#include "execution/util/region_containers.h"

namespace tpl::compiler {

QueryState::QueryState(ast::Identifier state_name) : state_name_(state_name), constructed_type_(nullptr), states_() {}

QueryState::Id QueryState::RegisterState(std::string name, ast::Expr *type, ast::Expr *value) {
  auto id = states_.size();
  // TODO(WAN): fix naming conflicts here
  states_.emplace_back(std::move(name), type, value);
  return static_cast<u32>(id);
}

ast::MemberExpr *QueryState::GetMember(tpl::compiler::CodeGen *codegen, Id id) {
  auto struct_name = (*codegen)->NewIdentifierExpr(DUMMY_POS, state_name_);
  auto member_name = (*codegen)->NewIdentifierExpr(DUMMY_POS, ast::Identifier(states_[id].name.c_str()));
  return (*codegen)->NewMemberExpr(DUMMY_POS, struct_name, member_name);
}

void QueryState::FinalizeType(tpl::compiler::CodeGen *codegen) {
  if (constructed_type_ != nullptr) return;
  util::RegionVector<ast::FieldDecl *> members(codegen->GetRegion());
  members.reserve(states_.size());
  for (const auto &state : states_) {
    members.emplace_back((*codegen)->NewFieldDecl(DUMMY_POS, ast::Identifier(state.name.c_str()), state.type));
  }
  constructed_type_ = (*codegen)->NewStructType(DUMMY_POS, std::move(members));
}

}