#pragma once

#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include "execution/sql/value.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "util/time_util.h"

namespace terrier::binder {
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
   */
  explicit BinderSherpa(const common::ManagedPointer<parser::ParseResult> parse_result,
                        const common::ManagedPointer<std::vector<parser::ConstantValueExpression>> parameters)
      : parse_result_(parse_result), parameters_(parameters) {
    TERRIER_ASSERT(parse_result != nullptr, "We shouldn't be trying to bind something without a ParseResult.");
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

  /**
   * Attempt to convert the transient value to the desired type.
   * Note that type promotion could be an upcast or downcast size-wise.
   *
   * @param value The transient value to be checked and potentially promoted.
   * @param desired_type The type to promote the transient value to.
   */
  void CheckAndTryPromoteType(common::ManagedPointer<parser::ConstantValueExpression> value,
                              type::TypeId desired_type) const;

  /**
   * Convenience function. Used by the visitor sheep to report that an error has occurred, causing BINDER_EXCEPTION.
   * @param message The error message.
   */
  void ReportFailure(const std::string &message) const { throw BINDER_EXCEPTION(message.c_str()); }

 private:
  /**
   * @return True if the value of @p int_val fits in the Output type, false otherwise.
   */
  template <typename Output, typename Input>
  static bool IsRepresentable(const Input int_val) {
    return std::numeric_limits<Output>::lowest() <= int_val && int_val <= std::numeric_limits<Output>::max();
  }

  /**
   * @return Casted numeric type, or an exception if the cast fails.
   */
  template <typename Input>
  static void TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                const Input int_val, const type::TypeId desired_type) {
    switch (desired_type) {
      case type::TypeId::TINYINT: {
        if (IsRepresentable<int8_t>(int_val)) {
          value->SetReturnValueType(desired_type);
          return;
        }
        break;
      }
      case type::TypeId::SMALLINT: {
        if (IsRepresentable<int16_t>(int_val)) {
          value->SetReturnValueType(desired_type);
          return;
        }
        break;
      }
      case type::TypeId::INTEGER: {
        if (IsRepresentable<int32_t>(int_val)) {
          value->SetReturnValueType(desired_type);
          return;
        }
        break;
      }
      case type::TypeId::BIGINT: {
        if (IsRepresentable<int64_t>(int_val)) {
          value->SetReturnValueType(desired_type);
          return;
        }
        break;
      }
      case type::TypeId::DECIMAL: {
        if (IsRepresentable<double>(int_val)) {
          value->SetValue(desired_type, std::make_unique<execution::sql::Real>(static_cast<double>(int_val)));
          return;
        }
        break;
      }
      default:
        throw BINDER_EXCEPTION("BinderSherpa TryCastNumericAll not a numeric type!");
    }
    throw BINDER_EXCEPTION("BinderSherpa TryCastNumericAll value out of bounds!");
  }

  const common::ManagedPointer<parser::ParseResult> parse_result_ = nullptr;
  const common::ManagedPointer<std::vector<parser::ConstantValueExpression>> parameters_ = nullptr;
  std::unordered_map<uintptr_t, type::TypeId> desired_expr_types_;
};
}  // namespace terrier::binder
