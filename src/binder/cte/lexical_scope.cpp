#include "binder/cte/lexical_scope.h"

#include <algorithm>

#include "binder/cte/context_sensitive_table_ref.h"

namespace noisepage::binder::cte {

LexicalScope::LexicalScope(std::size_t id, std::size_t depth, const LexicalScope *enclosing_scope)
    : id_{id}, depth_{depth}, enclosing_scope_{enclosing_scope} {}

std::size_t LexicalScope::RefCount() const { return references_.size(); }

std::size_t LexicalScope::ReadRefCount() const { return RefCountWithType(RefType::READ); }

std::size_t LexicalScope::WriteRefCount() const { return RefCountWithType(RefType::WRITE); }

std::size_t LexicalScope::RefCountWithType(RefType type) const {
  return std::count_if(references_.cbegin(), references_.cend(),
                       [=](const std::unique_ptr<ContextSensitiveTableRef> &r) { return r->Type() == type; });
}

std::vector<std::unique_ptr<ContextSensitiveTableRef>> &LexicalScope::References() { return references_; }

const std::vector<std::unique_ptr<ContextSensitiveTableRef>> &LexicalScope::References() const { return references_; }

void LexicalScope::AddEnclosedScope(std::unique_ptr<LexicalScope> &&scope) {
  enclosed_scopes_.push_back(std::move(scope));
}

void LexicalScope::AddReference(std::unique_ptr<ContextSensitiveTableRef> &&ref) {
  references_.push_back(std::move(ref));
}

}  // namespace noisepage::binder::cte
