#pragma once

#include <string>
#include <unordered_map>

#include "parser/expression/abstract_expression.h"

namespace terrier {

namespace parser {
class ParseResult;
class AbstractExpression;
};  // namespace parser

namespace binder {
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
   * @param parse_result The parse result to be tracked.
   */
  explicit BinderSherpa(const common::ManagedPointer<parser::ParseResult> parse_result) : parse_result_(parse_result) {}

  /**
   * @return The parse result that we're tracking.
   */
  common::ManagedPointer<parser::ParseResult> GetParseResult() { return parse_result_; }

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
   * Convenience function. Common case of wanting the left and right children to have compatible types, where one child
   * currently has the correct type and the other child's type must be derived from the correct child.
   *
   * @param left left child of an expression
   * @param right right child of an expression
   */
  void SetDesiredTypePair(const common::ManagedPointer<parser::AbstractExpression> left,
                          const common::ManagedPointer<parser::AbstractExpression> right) {
    auto left_type = left->GetReturnValueType();
    auto right_type = right->GetReturnValueType();

    // If the types are mismatched, try to convert types accordingly.
    if (left_type != right_type) {
      /*
       * The way we use libpg_query has the following quirks.
       * - NULL comes in with type::TypeId::INVALID.
       * - Dates and timestamps can potentially come in as VARCHAR.
       * - All integers come in as INTEGER. TODO(WAN): I don't know how BIGINT works.
       */

      auto is_left_maybe_null = left_type == type::TypeId::INVALID;
      auto is_left_varchar = left_type == type::TypeId::VARCHAR;
      auto is_left_integer = left_type == type::TypeId::INTEGER;

      if (right_type != type::TypeId::INVALID && (is_left_maybe_null || is_left_varchar || is_left_integer)) {
        SetDesiredType(left, right_type);
      }

      auto is_right_maybe_null = right_type == type::TypeId::INVALID;
      auto is_right_varchar = right_type == type::TypeId::VARCHAR;
      auto is_right_integer = right_type == type::TypeId::INTEGER;

      if (left_type != type::TypeId::INVALID && (is_right_maybe_null || is_right_varchar || is_right_integer)) {
        SetDesiredType(left, right_type);
      }
    }
  }

  /**
   * Convenience function. Check that the desired type of the child expression matches previously specified constraints.
   * Throws a BINDER_EXCEPTION if a type constraint fails.
   *
   * @param expr The expression to be checked. If no type constraint was imposed before, then the check will pass.
   */
  void CheckDesiredType(const common::ManagedPointer<parser::AbstractExpression> expr) const {
    const auto it = desired_expr_types_.find(reinterpret_cast<uintptr_t>(expr.Get()));
    if (it != desired_expr_types_.end() && it->second != expr->GetReturnValueType()) {
      // There was a constraint and the expression did not satisfy it. Blow up.
      throw BINDER_EXCEPTION("BinderSherpa expected expr to have a different type.");
    }
  }

  /**
   * Convenience function. Used by the visitor sheep to report that an error has occurred, causing BINDER_EXCEPTION.
   * @param message The error message.
   */
  void ReportFailure(const std::string &message) { throw BINDER_EXCEPTION(message.c_str()); }

 private:
  const common::ManagedPointer<parser::ParseResult> parse_result_;
  std::unordered_map<uintptr_t, type::TypeId> desired_expr_types_;
};
}  // namespace binder

}  // namespace terrier
