#include "binder/cte/lexical_scope.h"

#include <algorithm>

#include "binder/cte/typed_table_ref.h"

namespace noisepage::binder::cte {

LexicalScope::LexicalScope(std::size_t id, std::size_t depth) : id_{id}, depth_{depth} {}

std::size_t LexicalScope::RefCount() const { return references_.size(); }

std::size_t LexicalScope::ReadRefCount() const { return RefCountWithType(RefType::READ); }

std::size_t LexicalScope::WriteRefCount() const { return RefCountWithType(RefType::WRITE); }

std::size_t LexicalScope::RefCountWithType(RefType type) const {
  return std::count_if(references_.cbegin(), references_.cend(),
                       [=](const TypedTableRef &r) { return r.Type() == type; });
}

std::vector<TypedTableRef> &LexicalScope::References() { return references_; }

const std::vector<TypedTableRef> &LexicalScope::References() const { return references_; }

void LexicalScope::AddEnclosedScope(const LexicalScope &scope) { enclosed_scopes_.push_back(scope); }

void LexicalScope::AddEnclosedScope(LexicalScope &&scope) { enclosed_scopes_.emplace_back(std::move(scope)); }

void LexicalScope::AddReference(const TypedTableRef &ref) { references_.push_back(ref); }

void LexicalScope::AddReference(TypedTableRef &&ref) { references_.push_back(ref); }

}  // namespace noisepage::binder::cte
