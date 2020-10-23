#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage::binder {
/**
 * BinderSherpa tracks state that is communicated throughout the visitor pattern such as the parse result and also
 * expression-specific metadata.
 *
 * N.B. This was originally going to be called BinderContext, but there's already a BinderContext that doesn't seem to
 * have exactly the type of lifecycle that we wanted.
 */
class BinderSherpa {
 public:
  /**
   * Create a new BinderSherpa.
   * @param parse_result The parse result to be tracked
   * @param parameters parameters for the query being bound, can be nullptr if there are no parameters
   * @param desired_parameter_types same size as parameters, can be nullptr if there are no parameters
   */
  explicit BinderSherpa(const common::ManagedPointer<parser::ParseResult> parse_result,
                        const common::ManagedPointer<std::vector<parser::ConstantValueExpression>> parameters,
                        const common::ManagedPointer<std::vector<type::TypeId>> desired_parameter_types)
      : parse_result_(parse_result), parameters_(parameters), desired_parameter_types_(desired_parameter_types) {
    NOISEPAGE_ASSERT(parse_result != nullptr, "We shouldn't be trying to bind something without a ParseResult.");
    NOISEPAGE_ASSERT((parameters == nullptr && desired_parameter_types == nullptr) ||
                         (parameters != nullptr && desired_parameter_types != nullptr),
                     "Either need both the parameters vector and desired types vector, or neither.");
  }

  /**
   * @return The parse result that we're tracking.
   */
  common::ManagedPointer<parser::ParseResult> GetParseResult() const { return parse_result_; }

  /**
   * @return parameters for the query being bound
   * @warning can be nullptr if there are no parameters
   */
  common::ManagedPointer<std::vector<parser::ConstantValueExpression>> GetParameters() const { return parameters_; }

  /**
   * @param expr The expression whose type constraints we want to look up.
   * @return The previously recorded type constraints, or the expression's current return value type if none exist.
   */
  type::TypeId GetDesiredType(const common::ManagedPointer<parser::AbstractExpression> expr) const {
    const auto it = desired_expr_types_.find(reinterpret_cast<uintptr_t>(expr.Get()));
    if (it != desired_expr_types_.end()) return it->second;
    return expr->GetReturnValueType();
  }

  /**
   * Set the desired type of expr. The sherpa does not do anything except take note of the request.
   * @param expr The expression whose type we want to constrain.
   * @param type The desired type.
   */
  void SetDesiredType(const common::ManagedPointer<parser::AbstractExpression> expr, const type::TypeId type) {
    desired_expr_types_[reinterpret_cast<uintptr_t>(expr.Get())] = type;
  }

  /**
   * Stash the desired parameter type for fast-path binding
   * @param parameter_index offset of the parameter in the statement
   * @param type desired type to cast to on future bindings
   */
  void SetDesiredParameterType(const uint32_t parameter_index, const type::TypeId type) {
    (*desired_parameter_types_)[parameter_index] = type;
  }

  /**
   * Convenience function. Common case of wanting the left and right children to have compatible types, where one child
   * currently has the correct type and the other child's type must be derived from the correct child.
   *
   * @param left left child of an expression
   * @param right right child of an expression
   */
  void SetDesiredTypePair(common::ManagedPointer<parser::AbstractExpression> left,
                          common::ManagedPointer<parser::AbstractExpression> right);

  /**
   * Convenience function. Check that the desired type of the child expression matches previously specified constraints.
   * Throws a BINDER_EXCEPTION if a type constraint fails.
   *
   * @param expr The expression to be checked. If no type constraint was imposed before, then the check will pass.
   */
  void CheckDesiredType(common::ManagedPointer<parser::AbstractExpression> expr) const;

 private:
  const common::ManagedPointer<parser::ParseResult> parse_result_ = nullptr;
  const common::ManagedPointer<std::vector<parser::ConstantValueExpression>> parameters_ = nullptr;
  const common::ManagedPointer<std::vector<type::TypeId>> desired_parameter_types_ = nullptr;
  std::unordered_map<uintptr_t, type::TypeId> desired_expr_types_;
};
}  // namespace noisepage::binder
