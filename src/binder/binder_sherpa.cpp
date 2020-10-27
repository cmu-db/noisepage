#include "binder/binder_sherpa.h"

#include <unordered_map>

#include "binder/binder_util.h"
#include "parser/expression/abstract_expression.h"
#include "spdlog/fmt/fmt.h"
namespace noisepage::binder {

void BinderSherpa::SetDesiredTypePair(const common::ManagedPointer<parser::AbstractExpression> left,
                                      const common::ManagedPointer<parser::AbstractExpression> right) {
  type::TypeId left_type = type::TypeId::INVALID;
  type::TypeId right_type = type::TypeId::INVALID;
  bool has_constraints = false;

  // Check if the left type has been constrained.
  auto it = desired_expr_types_.find(reinterpret_cast<uintptr_t>(left.Get()));
  if (it != desired_expr_types_.end()) {
    left_type = it->second;
    has_constraints = true;
  }

  // Check if the right type has been constrained.
  it = desired_expr_types_.find(reinterpret_cast<uintptr_t>(right.Get()));
  if (it != desired_expr_types_.end()) {
    right_type = it->second;
    has_constraints = true;
  }

  // If neither the left nor the right type has been constrained, operate off the return value type.
  if (!has_constraints) {
    left_type = left->GetReturnValueType();
    right_type = right->GetReturnValueType();
  }

  // If the types are mismatched, try to convert types accordingly.
  if (left_type != right_type) {
    /*
     * The way we use libpg_query has the following quirks.
     * - NULL comes in with type::TypeId::INVALID.
     * - Dates and timestamps can potentially come in as VARCHAR.
     * - All small-enough integers come in as INTEGER.
     *   Too-big integers will come in as BIGINT.
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
      SetDesiredType(right, left_type);
    }
  }
}

void BinderSherpa::CheckDesiredType(const common::ManagedPointer<parser::AbstractExpression> expr) const {
  const auto it = desired_expr_types_.find(reinterpret_cast<uintptr_t>(expr.Get()));
  if (it != desired_expr_types_.end() && it->second != expr->GetReturnValueType()) {
    // There was a constraint and the expression did not satisfy it. Blow up.
    throw BINDER_EXCEPTION(
        fmt::format("BinderSherpa expected expr to have a different type. Expected: {}, Expression type: {}",
                    type::TypeUtil::TypeIdToString(it->second),
                    type::TypeUtil::TypeIdToString(expr->GetReturnValueType())),
        common::ErrorCode::ERRCODE_SYNTAX_ERROR);
  }
}

}  // namespace noisepage::binder
