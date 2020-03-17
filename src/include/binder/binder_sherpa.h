#pragma once

#include <string>
#include <unordered_map>

#include "parser/expression/abstract_expression.h"
#include "type/transient_value_factory.h"
#include "util/time_util.h"

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
  explicit BinderSherpa(const common::ManagedPointer<parser::ParseResult> parse_result,
                        const common::ManagedPointer<std::vector<type::TransientValue>> parameters)
      : parse_result_(parse_result), parameters_(parameters) {}

  /**
   * @return The parse result that we're tracking.
   */
  common::ManagedPointer<parser::ParseResult> GetParseResult() const { return parse_result_; }

  common::ManagedPointer<std::vector<type::TransientValue>> GetParameters() const { return parameters_; }

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

  void CheckAndTryPromoteType(const common::ManagedPointer<type::TransientValue> value,
                              const type::TypeId desired_type) const {
    const auto curr_type = value->Type();

    // Check if types are mismatched, and convert them if possible.
    if (curr_type != desired_type) {
      switch (curr_type) {
        // NULL conversion.
        case type::TypeId::INVALID: {
          *value = type::TransientValueFactory::GetNull(desired_type);
          break;
        }

          // INTEGER casting (upwards and downwards).
        case type::TypeId::INTEGER: {
          auto intval = type::TransientValuePeeker::PeekInteger(*value);

          // TODO(WAN): check if intval fits in the desired type
          if (desired_type == type::TypeId::TINYINT) {
            *value = type::TransientValueFactory::GetTinyInt(intval);
          } else if (desired_type == type::TypeId::SMALLINT) {
            *value = type::TransientValueFactory::GetSmallInt(intval);
          } else if (desired_type == type::TypeId::BIGINT) {
            // TODO(WAN): how does BIGINT come in on libpg_query?
            *value = type::TransientValueFactory::GetBigInt(intval);
          } else if (desired_type == type::TypeId::DECIMAL) {
            *value = type::TransientValueFactory::GetDecimal(intval);
          }
          break;
        }

          // DATE and TIMESTAMP conversion.
        case type::TypeId::VARCHAR: {
          const auto str_view = type::TransientValuePeeker::PeekVarChar(*value);

          // TODO(WAN): A bit stupid to take the string view back into a string.
          if (desired_type == type::TypeId::DATE) {
            auto parsed_date = util::TimeConvertor::ParseDate(std::string(str_view));
            if (!parsed_date.first) {
              ReportFailure("Binder conversion from VARCHAR to DATE failed.");
            }
            *value = type::TransientValueFactory::GetDate(parsed_date.second);
          } else if (desired_type == type::TypeId::TIMESTAMP) {
            auto parsed_timestamp = util::TimeConvertor::ParseTimestamp(std::string(str_view));
            if (!parsed_timestamp.first) {
              ReportFailure("Binder conversion from VARCHAR to TIMESTAMP failed.");
            }
            *value = type::TransientValueFactory::GetTimestamp(parsed_timestamp.second);
          }
          break;
        }

        default: {
          ReportFailure("Binder conversion of ConstantValueExpression type failed.");
        }
      }
    }
  }

  /**
   * Convenience function. Used by the visitor sheep to report that an error has occurred, causing BINDER_EXCEPTION.
   * @param message The error message.
   */
  void ReportFailure(const std::string &message) const { throw BINDER_EXCEPTION(message.c_str()); }

 private:
  const common::ManagedPointer<parser::ParseResult> parse_result_ = nullptr;
  const common::ManagedPointer<std::vector<type::TransientValue>> parameters_ = nullptr;
  std::unordered_map<uintptr_t, type::TypeId> desired_expr_types_;
};
}  // namespace binder

}  // namespace terrier
