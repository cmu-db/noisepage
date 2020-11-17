#include "binder/binder_util.h"

#include <algorithm>
#include <limits>

#include "common/error/error_code.h"
#include "network/postgres/postgres_defs.h"
#include "parser/expression/constant_value_expression.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::binder {

void BinderUtil::PromoteParameters(
    const common::ManagedPointer<std::vector<parser::ConstantValueExpression> > parameters,
    const std::vector<type::TypeId> &desired_parameter_types) {
  NOISEPAGE_ASSERT(parameters->size() == desired_parameter_types.size(), "They have to be equal in size.");
  for (uint32_t parameter_index = 0; parameter_index < desired_parameter_types.size(); parameter_index++) {
    const auto desired_type = desired_parameter_types[parameter_index];

    if (desired_type != type::TypeId::INVALID) {
      const auto param = common::ManagedPointer(&(*parameters)[parameter_index]);
      BinderUtil::CheckAndTryPromoteType(param, desired_type);
    }
  }
}

void BinderUtil::CheckAndTryPromoteType(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                        const type::TypeId desired_type) {
  const auto curr_type = value->GetReturnValueType();

  // Check if types are mismatched, and convert them if possible.
  if (curr_type != desired_type) {
    switch (curr_type) {
      // NULL conversion.
      case type::TypeId::INVALID: {
        value->SetValue(desired_type, execution::sql::Val(true));
        break;
      }

        // INTEGER casting (upwards and downwards).
      case type::TypeId::TINYINT: {
        const auto int_val = value->Peek<int8_t>();
        TryCastNumericAll(value, int_val, desired_type);
        break;
      }
      case type::TypeId::SMALLINT: {
        const auto int_val = value->Peek<int16_t>();
        TryCastNumericAll(value, int_val, desired_type);
        break;
      }
      case type::TypeId::INTEGER: {
        const auto int_val = value->Peek<int32_t>();
        TryCastNumericAll(value, int_val, desired_type);
        break;
      }
      case type::TypeId::BIGINT: {
        const auto int_val = value->Peek<int64_t>();
        TryCastNumericAll(value, int_val, desired_type);
        break;
      }

        // DATE and TIMESTAMP conversion. String to boolean conversion. String to numeric type conversion.
      case type::TypeId::VARCHAR: {
        const auto str_view = value->Peek<std::string_view>();

        // TODO(Matt): see issue #977
        switch (desired_type) {
          case type::TypeId::DATE: {
            auto parsed_date = execution::sql::Date::FromString(str_view);
            value->SetValue(type::TypeId::DATE, execution::sql::DateVal(parsed_date));
            break;
          }
          case type::TypeId::TIMESTAMP: {
            auto parsed_timestamp = execution::sql::Timestamp::FromString(str_view);
            value->SetValue(type::TypeId::TIMESTAMP, execution::sql::TimestampVal(parsed_timestamp));
            break;
          }
          case type::TypeId::BOOLEAN: {
            if (std::find(network::POSTGRES_BOOLEAN_STR_TRUES.cbegin(), network::POSTGRES_BOOLEAN_STR_TRUES.cend(),
                          str_view) != network::POSTGRES_BOOLEAN_STR_TRUES.cend()) {
              value->SetValue(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
            } else if (std::find(network::POSTGRES_BOOLEAN_STR_FALSES.cbegin(),
                                 network::POSTGRES_BOOLEAN_STR_FALSES.cend(),
                                 str_view) != network::POSTGRES_BOOLEAN_STR_FALSES.cend()) {
              value->SetValue(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));
            } else {
              throw BINDER_EXCEPTION(fmt::format("invalid input syntax for type boolean: \"{}\"", str_view),
                                     common::ErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION);
            }
            break;
          }
          case type::TypeId::TINYINT: {
            size_t size;
            int64_t int_val;
            try {
              int_val = std::stol(std::string(str_view), &size);
            } catch (const std::out_of_range &e) {
              throw BINDER_EXCEPTION(fmt::format("tinyint out of range, string to convert was {}", str_view),
                                     common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
            }

            if (size != str_view.size()) {
              throw BINDER_EXCEPTION(fmt::format("invalid input format for type tinyint: \"{}\"", str_view),
                                     common::ErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION);
            }

            if (!IsRepresentable<int8_t>(int_val)) {
              throw BINDER_EXCEPTION(fmt::format("tinyint out of range, string to convert was {}", str_view),
                                     common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
            }
            value->SetValue(type::TypeId::TINYINT, execution::sql::Integer(int_val));
            break;
          }
          case type::TypeId::SMALLINT: {
            size_t size;
            int64_t int_val;
            try {
              int_val = std::stol(std::string(str_view), &size);
            } catch (const std::out_of_range &e) {
              throw BINDER_EXCEPTION(fmt::format("smallint out of range, string to convert was {}", str_view),
                                     common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
            }

            if (size != str_view.size()) {
              throw BINDER_EXCEPTION(fmt::format("invalid input format for type smallint: \"{}\"", str_view),
                                     common::ErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION);
            }

            if (!IsRepresentable<int16_t>(int_val)) {
              throw BINDER_EXCEPTION(fmt::format("smallint out of range, string to convert was {}", str_view),
                                     common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
            }
            value->SetValue(type::TypeId::SMALLINT, execution::sql::Integer(int_val));
            break;
          }
          case type::TypeId::INTEGER: {
            size_t size;
            int64_t int_val;
            try {
              int_val = std::stol(std::string(str_view), &size);
            } catch (const std::out_of_range &e) {
              throw BINDER_EXCEPTION(fmt::format("integer out of range, string to convert was {}", str_view),
                                     common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
            }

            if (size != str_view.size()) {
              throw BINDER_EXCEPTION(fmt::format("invalid input format for type integer: \"{}\"", str_view),
                                     common::ErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION);
            }

            if (!IsRepresentable<int32_t>(int_val)) {
              throw BINDER_EXCEPTION(fmt::format("integer out of range, string to convert was {}", str_view),
                                     common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
            }
            value->SetValue(type::TypeId::INTEGER, execution::sql::Integer(int_val));
            break;
          }
          case type::TypeId::BIGINT: {
            size_t size;
            int64_t int_val;
            try {
              int_val = std::stol(std::string(str_view), &size);
            } catch (const std::out_of_range &e) {
              throw BINDER_EXCEPTION(fmt::format("bigint out of range, string to convert was {}", str_view),
                                     common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
            }

            if (size != str_view.size()) {
              throw BINDER_EXCEPTION(fmt::format("invalid input format for type bigint: \"{}\"", str_view),
                                     common::ErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION);
            }

            if (!IsRepresentable<int64_t>(int_val)) {
              throw BINDER_EXCEPTION(fmt::format("bigint out of range, string to convert was {}", str_view),
                                     common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
            }
            value->SetValue(type::TypeId::BIGINT, execution::sql::Integer(int_val));
            break;
          }
          case type::TypeId::DECIMAL: {
            {
              double double_val;
              try {
                double_val = std::stod(std::string(str_view));
              } catch (std::exception &e) {
                throw BINDER_EXCEPTION(fmt::format("decimal out of range, string to convert was {}", str_view),
                                       common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
              }
              value->SetValue(type::TypeId::DECIMAL, execution::sql::Real(double_val));
              break;
            }
          }
          default:
            throw BINDER_EXCEPTION(
                fmt::format("failed to convert string to another type, string to convert was {}", str_view),
                common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        }

        break;
      }

      default: {
        throw BINDER_EXCEPTION(
            fmt::format("Binder conversion of expression failed. Desired type is {}, expression type is {}",
                        type::TypeUtil::TypeIdToString(desired_type), type::TypeUtil::TypeIdToString(curr_type)),
            common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
      }
    }
  }
}

template <typename Output, typename Input>
bool BinderUtil::IsRepresentable(const Input int_val) {
  // Fixes this hideously obscure bug: https://godbolt.org/z/M14jdb
  if constexpr (std::numeric_limits<Output>::is_integer && !std::numeric_limits<Input>::is_integer) {  // NOLINT
    return std::numeric_limits<Output>::lowest() <= int_val &&
           static_cast<Output>(int_val) < std::numeric_limits<Output>::max();
  } else {  // NOLINT
    return std::numeric_limits<Output>::lowest() <= int_val && int_val <= std::numeric_limits<Output>::max();
  }
}

/**
 * @return Casted numeric type, or an exception if the cast fails.
 */
template <typename Input>
void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                   const Input int_val, const type::TypeId desired_type) {
  switch (desired_type) {
    case type::TypeId::TINYINT: {
      if (IsRepresentable<int8_t>(int_val)) {
        value->SetReturnValueType(desired_type);
        return;
      }
      throw BINDER_EXCEPTION(fmt::format("tinyint out of range. number to convert was {}", int_val),
                             common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }
    case type::TypeId::SMALLINT: {
      if (IsRepresentable<int16_t>(int_val)) {
        value->SetReturnValueType(desired_type);
        return;
      }
      throw BINDER_EXCEPTION(fmt::format("smallint out of range. number to convert was {}", int_val),
                             common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }
    case type::TypeId::INTEGER: {
      if (IsRepresentable<int32_t>(int_val)) {
        value->SetReturnValueType(desired_type);
        return;
      }
      throw BINDER_EXCEPTION(fmt::format("integer out of range. number to convert was {}", int_val),
                             common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }
    case type::TypeId::BIGINT: {
      if (IsRepresentable<int64_t>(int_val)) {
        value->SetReturnValueType(desired_type);
        return;
      }
      throw BINDER_EXCEPTION(fmt::format("bigint out of range. number to convert was {}", int_val),
                             common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }
    case type::TypeId::DECIMAL: {
      if (IsRepresentable<double>(int_val)) {
        value->SetValue(desired_type, execution::sql::Real(static_cast<double>(int_val)));
        return;
      }
      throw BINDER_EXCEPTION(fmt::format("decimal out of range", int_val),
                             common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }
    default:
      throw BINDER_EXCEPTION(
          fmt::format("TryCastNumericAll not a numeric type! Desired type was {}, number to convert was {}",
                      type::TypeUtil::TypeIdToString(desired_type), int_val),
          common::ErrorCode::ERRCODE_DATA_EXCEPTION);
  }
}

template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                            const int8_t int_val, const type::TypeId desired_type);
template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                            const int16_t int_val, const type::TypeId desired_type);
template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                            const int32_t int_val, const type::TypeId desired_type);
template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                            const int64_t int_val, const type::TypeId desired_type);

template bool BinderUtil::IsRepresentable<int8_t>(const int8_t int_val);
template bool BinderUtil::IsRepresentable<int16_t>(const int8_t int_val);
template bool BinderUtil::IsRepresentable<int32_t>(const int8_t int_val);
template bool BinderUtil::IsRepresentable<int64_t>(const int8_t int_val);
template bool BinderUtil::IsRepresentable<double>(const int8_t int_val);

template bool BinderUtil::IsRepresentable<int8_t>(const int16_t int_val);
template bool BinderUtil::IsRepresentable<int16_t>(const int16_t int_val);
template bool BinderUtil::IsRepresentable<int32_t>(const int16_t int_val);
template bool BinderUtil::IsRepresentable<int64_t>(const int16_t int_val);
template bool BinderUtil::IsRepresentable<double>(const int16_t int_val);

template bool BinderUtil::IsRepresentable<int8_t>(const int32_t int_val);
template bool BinderUtil::IsRepresentable<int16_t>(const int32_t int_val);
template bool BinderUtil::IsRepresentable<int32_t>(const int32_t int_val);
template bool BinderUtil::IsRepresentable<int64_t>(const int32_t int_val);
template bool BinderUtil::IsRepresentable<double>(const int32_t int_val);

template bool BinderUtil::IsRepresentable<int8_t>(const int64_t int_val);
template bool BinderUtil::IsRepresentable<int16_t>(const int64_t int_val);
template bool BinderUtil::IsRepresentable<int32_t>(const int64_t int_val);
template bool BinderUtil::IsRepresentable<int64_t>(const int64_t int_val);
template bool BinderUtil::IsRepresentable<double>(const int64_t int_val);

template bool BinderUtil::IsRepresentable<int8_t>(const double int_val);
template bool BinderUtil::IsRepresentable<int16_t>(const double int_val);
template bool BinderUtil::IsRepresentable<int32_t>(const double int_val);
template bool BinderUtil::IsRepresentable<int64_t>(const double int_val);
template bool BinderUtil::IsRepresentable<double>(const double int_val);

}  // namespace noisepage::binder
