#pragma once

#include <unordered_set>

#include "common/managed_pointer.h"

namespace noisepage::binder::cte {

/**
 * Denotes the type of table reference.
 */
enum class RefType { READ, WRITE };

/**
 * A ContextSensitiveTableRef is just a wrapper around
 * a regular TableRef with some extra metadata attached -
 * namely, we need to keep track of the depth and position
 * of table references within the original parse tree in
 * order to perform some subsequent validations.
 */
class ContextSensitiveTableRef {
 public:
  /**
   * Construct a new ContextSensitiveTableRef instance.
   * @param id The unique identifier for this instance
   * @param scope The identifier for the scope in which this reference appears
   * @param position The position of this table reference relative to
   * other table references in the same scope
   * @param table The associated table reference
   */
  ContextSensitiveTableRef(std::size_t id, RefType type, std::size_t scope, std::size_t position,
                           common::ManagedPointer<parser::TableRef> table)
      : id_{id}, type_{type}, scope_{scope}, position_{position}, table_{table} {}

  /** @return The unique identifier for this reference */
  std::size_t Id() const noexcept { return id_; }

  RefType Type() const noexcept { return type_; }

  /** @return The identifier for the scope in which the reference appears */
  std::size_t Scope() const noexcept { return scope_; }

  /** @return The lateral position of this reference within its scope */
  std::size_t Position() const noexcept { return position_; }

  /** @return The table associated with this context-sensitive reference */
  common::ManagedPointer<parser::TableRef> Table() const { return table_; }

  /** @return The collection of identifier's for this reference's dependencies */
  const std::unordered_set<std::size_t> &Dependencies() const { return dependencies_; }

  /**
   * Add a dependency for this context-sensitive reference.
   * @param id The unique identifier for the dependency
   */
  void AddDependency(const std::size_t id) { dependencies_.insert(id); }

 private:
  // The unique identifier for the context-sensitive reference
  const std::size_t id_;

  // The type of the reference
  const RefType type_;

  // The identifer for the scope in which this reference appears
  const std::size_t scope_;

  // The lateral position of the reference, within its local scope
  const std::size_t position_;

  // The associated table
  const common::ManagedPointer<parser::TableRef> table_;

  // The identifiers for the context-sensitive references on which this depends
  std::unordered_set<std::size_t> dependencies_;
};

}  // namespace noisepage::binder::cte
