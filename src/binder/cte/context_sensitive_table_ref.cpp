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

const LexicalScope *ContextSensitiveTableRef::Scope() const { return scope_; }

}  // namespace noisepage::binder::cte
