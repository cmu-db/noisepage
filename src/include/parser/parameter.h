#pragma once

#include "execution/sql/sql.h"

namespace noisepage::parser {

/**
 * Stores parameter metadata, e.g. type.
 */
class Parameter {
 public:
  /** Whether a parameter is a constant or a variable. */
  enum class Mutability { CONSTANT = 0, VARIABLE = 1 };

  /**
   * Instantiates a new parameter with the given arguments.
   * @param mutability whether parameter is constant or variable
   * @param type_id the SQL type ID
   * @param is_nullable whether this parameter is nullable
   */
  Parameter(Mutability mutability, execution::sql::SqlTypeId type_id, bool is_nullable)
      : type_(mutability), type_id_(type_id), is_nullable_(is_nullable) {}

  /**
   * Creates a new constant parameter.
   * @param type_id SQL type
   * @param is_nullable whether the parameter is nullable
   * @return the new constant parameter
   */
  static Parameter CreateConstantParameter(const execution::sql::SqlTypeId type_id, const bool is_nullable) {
    return {Mutability::CONSTANT, type_id, is_nullable};
  }

  /**
   * Creates a new variable parameter.
   * @param type_id SQL type
   * @param is_nullable whether the parameter is nullable
   * @return the new variable parameter
   */
  static Parameter CreateVariableParameter(const execution::sql::SqlTypeId type_id, const bool is_nullable) {
    return {Mutability::VARIABLE, type_id, is_nullable};
  }

  /** @return parameter type (constant or variable) */
  Mutability GetMutability() const { return type_; }

  /** @return SQL type ID */
  execution::sql::SqlTypeId GetTypeId() const { return type_id_; }

  /** @return true if parameter is nullable, false otherwise */
  bool IsNullable() const { return is_nullable_; }

 private:
  const Mutability type_;
  const execution::sql::SqlTypeId type_id_;
  const bool is_nullable_;
};

}  // namespace noisepage::parser
