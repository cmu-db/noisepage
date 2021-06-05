#pragma once

#include "common/managed_pointer.h"

namespace noisepage::parser {
class TableRef;
}

namespace noisepage::binder::cte {

enum class RefType { READ, WRITE };

/**
 * The TaggedTableRef class represents a table reference
 * and its associated type: READ or WRITE.
 */
class TypedTableRef {
 public:
  /**
   * Construct a new TypedTableRef instance.
   * @param table The associated table
   * @param type The type of reference
   */
  TypedTableRef(common::ManagedPointer<parser::TableRef> table, RefType type);

  /** @return The associated table reference */
  common::ManagedPointer<parser::TableRef> Table() const { return table_; }

  /** @return The type of the reference */
  RefType Type() const { return type_; }

 private:
  // The associated table reference
  common::ManagedPointer<parser::TableRef> table_;

  // The type of the reference
  RefType type_;
};

}  // namespace noisepage::binder::cte
