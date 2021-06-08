#include "binder/cte/context_sensitive_table_ref.h"

#include "binder/cte/lexical_scope.h"
#include "common/macros.h"

namespace noisepage::binder::cte {

ContextSensitiveTableRef::ContextSensitiveTableRef(common::ManagedPointer<parser::TableRef> table, RefType type,
                                                   const LexicalScope *enclosing_scope)
    : table_{table}, type_{type}, enclosing_scope_{enclosing_scope}, scope_{nullptr} {
  NOISEPAGE_ASSERT(type == RefType::READ, "WRITE Table References Must Define a Scope");
}

ContextSensitiveTableRef::ContextSensitiveTableRef(common::ManagedPointer<parser::TableRef> table, RefType type,
                                                   const LexicalScope *enclosing_scope, const LexicalScope *scope)
    : table_{table}, type_{type}, enclosing_scope_{enclosing_scope}, scope_{scope} {
  NOISEPAGE_ASSERT(type == RefType::WRITE, "READ Table References Should Not Define a Scope");
}

const LexicalScope *ContextSensitiveTableRef::EnclosingScope() const { return enclosing_scope_; }

const LexicalScope *ContextSensitiveTableRef::Scope() const {
  NOISEPAGE_ASSERT(type_ == RefType::WRITE, "READ references do not define a scope");
  return scope_;
}

bool ContextSensitiveTableRef::operator==(const ContextSensitiveTableRef &rhs) const {
  return table_ == rhs.table_ && type_ == rhs.type_ && scope_ == rhs.scope_ && enclosing_scope_ == rhs.enclosing_scope_;
}

bool ContextSensitiveTableRef::operator!=(const ContextSensitiveTableRef &rhs) const { return !(*this == rhs); }

}  // namespace noisepage::binder::cte
