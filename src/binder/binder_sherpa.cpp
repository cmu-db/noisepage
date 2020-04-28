
#include "binder/binder_sherpa.h"

#include <stdint.h>
#include <string>
#include <string_view>
#include <unordered_map>

#include "parser/expression/abstract_expression.h"
#include "type/transient_value_peeker.h"
#include "util/time_util.h"

namespace terrier::binder {

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
    const auto desired UNUSED_ATTRIBUTE = it->second;
    const auto current UNUSED_ATTRIBUTE = expr->GetReturnValueType();
    throw BINDER_EXCEPTION("BinderSherpa expected expr to have a different type.");
  }
}

void BinderSherpa::CheckAndTryPromoteType(const common::ManagedPointer<type::TransientValue> value,
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
        ReportFailure("Binder conversion of expression type failed.");
      }
    }
  }
}

}  // namespace terrier::binder
