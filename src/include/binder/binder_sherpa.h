#pragma once

#include <limits>
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
  explicit BinderSherpa(const common::ManagedPointer<parser::ParseResult> parse_result) : parse_result_(parse_result) {}

  /**
   * @return The parse result that we're tracking.
   */
  common::ManagedPointer<parser::ParseResult> GetParseResult() const { return parse_result_; }

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
   * Attempt to convert the transient value to the desired type.
   * Note that type promotion could be an upcast or downcast size-wise.
   *
   * @param value The transient value to be checked and potentially promoted.
   * @param desired_type The type to promote the transient value to.
   */
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
        case type::TypeId::TINYINT: {
          auto int_val = type::TransientValuePeeker::PeekTinyInt(*value);
          *value = TryCastNumericAll(int_val, desired_type);
          break;
        }
        case type::TypeId::SMALLINT: {
          auto int_val = type::TransientValuePeeker::PeekSmallInt(*value);
          *value = TryCastNumericAll(int_val, desired_type);
          break;
        }
        case type::TypeId::INTEGER: {
          auto int_val = type::TransientValuePeeker::PeekInteger(*value);
          *value = TryCastNumericAll(int_val, desired_type);
          break;
        }
        case type::TypeId::BIGINT: {
          auto int_val = type::TransientValuePeeker::PeekBigInt(*value);
          *value = TryCastNumericAll(int_val, desired_type);
          break;
        }

        // DATE and TIMESTAMP conversion. String to numeric type conversion.
        // TODO(WAN): float-type numerics are probably broken.
        case type::TypeId::VARCHAR: {
          const auto str_view = type::TransientValuePeeker::PeekVarChar(*value);

          // TODO(WAN): A bit stupid to take the string view back into a string.
          switch (desired_type) {
            case type::TypeId::DATE: {
              auto parsed_date = util::TimeConvertor::ParseDate(std::string(str_view));
              if (!parsed_date.first) {
                ReportFailure("Binder conversion from VARCHAR to DATE failed.");
              }
              *value = type::TransientValueFactory::GetDate(parsed_date.second);
              break;
            }
            case type::TypeId::TIMESTAMP: {
              auto parsed_timestamp = util::TimeConvertor::ParseTimestamp(std::string(str_view));
              if (!parsed_timestamp.first) {
                ReportFailure("Binder conversion from VARCHAR to TIMESTAMP failed.");
              }
              *value = type::TransientValueFactory::GetTimestamp(parsed_timestamp.second);
              break;
            }
            case type::TypeId::TINYINT:
            case type::TypeId::SMALLINT:
            case type::TypeId::INTEGER:
            case type::TypeId::BIGINT: {
              auto int_val = std::stol(std::string(str_view));
              *value = TryCastNumericAll(int_val, desired_type);
              break;
            }
            default:
              throw BINDER_EXCEPTION("BinderSherpa VARCHAR cannot be cast to desired type.");
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
  /**
   * @return True if the value of @p int_val fits in the Output type, false otherwise.
   */
  template <typename Output, typename Input>
  static bool IsRepresentable(Input int_val) {
    return std::numeric_limits<Output>::min() <= int_val && int_val <= std::numeric_limits<Output>::max();
  }

  /**
   * @return The TransientValue obtained by peeking @p int_val with @p peeker if the value fits.
   */
  template <typename Output, typename Input>
  static type::TransientValue TryCastNumeric(Input int_val, type::TransientValue peeker(Output)) {
    if (!IsRepresentable<Output>(int_val)) {
      throw BINDER_EXCEPTION("BinderSherpa TryCastNumeric value out of bounds!");
    }
    return peeker(static_cast<Output>(int_val));
  }

  /**
   * @return Casted numeric type, or an exception if the cast fails.
   */
  template <typename Input>
  static type::TransientValue TryCastNumericAll(Input int_val, type::TypeId desired_type) {
    switch (desired_type) {
      case type::TypeId::TINYINT:
        return TryCastNumeric<int8_t>(int_val, &type::TransientValueFactory::GetTinyInt);
      case type::TypeId::SMALLINT:
        return TryCastNumeric<int16_t>(int_val, &type::TransientValueFactory::GetSmallInt);
      case type::TypeId::INTEGER:
        return TryCastNumeric<int32_t>(int_val, &type::TransientValueFactory::GetInteger);
      case type::TypeId::BIGINT:
        return TryCastNumeric<int64_t>(int_val, &type::TransientValueFactory::GetBigInt);
      case type::TypeId::DECIMAL:
        return TryCastNumeric<double>(int_val, &type::TransientValueFactory::GetDecimal);
      default:
        throw BINDER_EXCEPTION("BinderSherpa TryCastNumericAll not a numeric type!");
    }
  }

  const common::ManagedPointer<parser::ParseResult> parse_result_ = nullptr;
  std::unordered_map<uintptr_t, type::TypeId> desired_expr_types_;
};
}  // namespace binder

}  // namespace terrier
