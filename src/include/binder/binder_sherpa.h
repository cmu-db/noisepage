#pragma once

#include <unordered_map>

#include "parser/expression/abstract_expression.h"

namespace terrier {

namespace parser {
class ParseResult;
class AbstractExpression;
};  // namespace parser

namespace binder {
/**
 * Sherps things.
 */
class BinderSherpa {
 public:
  explicit BinderSherpa(const common::ManagedPointer<parser::ParseResult> parse_result) : parse_result_(parse_result) {}

  common::ManagedPointer<parser::ParseResult> GetParseResult() { return parse_result_; }

  /**
   * Check the desired type of the child expression matches the parent's expectations.
   *
   * Throws a BINDER_EXCEPTION if the sheep misbehave.
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

  type::TypeId GetDesiredType(const common::ManagedPointer<parser::AbstractExpression> expr) const {
    const auto it = desired_expr_types_.find(reinterpret_cast<uintptr_t>(expr.Get()));
    if (it != desired_expr_types_.end()) return it->second;
    return expr->GetReturnValueType();
  }

  void SetDesiredType(const common::ManagedPointer<parser::AbstractExpression> expr, const type::TypeId type) {
    desired_expr_types_[reinterpret_cast<uintptr_t>(expr.Get())] = type;
  }

  /**
   * Convenience method for the common case of wanting to make the left child and the right child have the same
   * resulting type.
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

  void ReportFailure(std::string message) { throw BINDER_EXCEPTION(message.c_str()); }

 private:
  const common::ManagedPointer<parser::ParseResult> parse_result_;
  std::unordered_map<uintptr_t, type::TypeId> desired_expr_types_;
};
}  // namespace binder

}  // namespace terrier