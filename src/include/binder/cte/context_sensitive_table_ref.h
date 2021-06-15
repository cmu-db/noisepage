#pragma once

#include "common/managed_pointer.h"

namespace noisepage::parser {
class TableRef;
}

namespace noisepage::binder::cte {
class LexicalScope;

/**
 * Denotes the type of the table reference, dependent
 * upon the manner in which it is used in the statement.
 */
enum class RefType { READ, WRITE };

/**
 * The ContextSensitiveTableRef class provides an internal representation
 * for table references within the dependency graph; it serves
 * as a graph vertex. Apologies in advance for the overloading
 * of the terms "ref" and "reference" that is going on here.
 */
class ContextSensitiveTableRef {
 public:
  /**
   * Construct a new ContextSensitiveTableRef instance.
   * @param table The associated table reference
   * @param type The type of the reference
   * @param enclosing_scope A pointer to the enclosing scope
   */
  ContextSensitiveTableRef(common::ManagedPointer<parser::TableRef> table, RefType type,
                           const LexicalScope *enclosing_scope);

  /**
   * Construct a new ContextSensitiveTableRef instance.
   * @param table The associated table reference
   * @param type The type of the reference
   * @param enclosing_scope A pointer to the enclosing scope
   * @param scope The scope defined by this table reference
   */
  ContextSensitiveTableRef(common::ManagedPointer<parser::TableRef> table, RefType type,
                           const LexicalScope *enclosing_scope, const LexicalScope *scope);

  /** @return The table reference for this table reference */
  common::ManagedPointer<parser::TableRef> Table() const { return table_; }

  /** @return The type of the reference */
  RefType Type() const { return type_; }

  /** @return The enclosing scope for this table reference */
  const LexicalScope *EnclosingScope() const;

  /** @return The scope defined by this table reference */
  const LexicalScope *Scope() const;

  /** Equality comparison for context-sensitive table references */
  bool operator==(const ContextSensitiveTableRef &rhs) const;

  /** Inequality comparison for context-sensitive table references */
  bool operator!=(const ContextSensitiveTableRef &rhs) const;

 private:
  // The associated table reference
  common::ManagedPointer<parser::TableRef> table_;

  // The type for the context-sensitive reference
  RefType type_;

  // The scope in which the reference appears
  const LexicalScope *enclosing_scope_;

  // The scope defined by this table reference
  const LexicalScope *scope_;
};
}  // namespace noisepage::binder::cte
