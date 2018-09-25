#pragma once

#include "type/type_id.h"

namespace terrier {
namespace type {

namespace expression {

/**
 * Stores parameter metadata, e.g. type.
 */
class Parameter {
 public:
  /**
   * Whether a parameter is a constant or a variable.
   */
  enum class ParameterType { CONSTANT = 0, VARIABLE = 1 };

  /**
   * Instantiates a new parameter with the given arguments.
   * @param type type of parameter
   * @param type_id the SQL type ID
   * @param is_nullable whether this parameter is nullable
   */
  Parameter(ParameterType type, TypeId type_id, bool is_nullable)
      : type_(type), type_id_(type_id), is_nullable_(is_nullable) {}

  /**
   * Creates a new constant parameter.
   * @param type_id SQL type
   * @param is_nullable whether the parameter is nullable
   * @return the new constant parameter
   */
  static Parameter CreateConstantParameter(const TypeId type_id, const bool is_nullable) {
    return Parameter(ParameterType::CONSTANT, type_id, is_nullable);
  }

  /**
   * Creates a new variable parameter.
   * @param type_id SQL type
   * @param is_nullable whether the parameter is nullable
   * @return the new variable parameter
   */
  static Parameter CreateVariableParameter(const TypeId type_id, const bool is_nullable) {
    return Parameter(ParameterType::VARIABLE, type_id, is_nullable);
  }

  /**
   * Return the parameter type (constant or variable).
   * @return parameter type
   */
  ParameterType GetParameterType() const { return type_; }

  /**
   * Return the SQL type ID.
   * @return SQL type ID
   */
  TypeId GetTypeId() const { return type_id_; }

  /**
   * Return whether the parameter is nullable.
   * @return true if parameter is nullable, false otherwise
   */
  bool IsNullable() const { return is_nullable_; }

 private:
  const ParameterType type_;
  const TypeId type_id_;
  const bool is_nullable_;
};

}  // namespace expression
}  // namespace type
}  // namespace terrier
