#include "binder/binder_util.h"

#include <algorithm>
#include <limits>

#include "catalog/catalog_accessor.h"
#include "common/error/error_code.h"
#include "network/postgres/postgres_defs.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/table_ref.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::binder {

void BinderUtil::ValidateWhereClause(const common::ManagedPointer<parser::AbstractExpression> value) {
  if (value->GetReturnValueType() != execution::sql::SqlTypeId::Boolean) {
    // TODO(Matt): NULL literal (execution::sql::SqlTypeId::Invalid and cve->IsNull()) should be allowed but breaks
    // stuff downstream
    throw BINDER_EXCEPTION("argument of WHERE must be type boolean", common::ErrorCode::ERRCODE_DATATYPE_MISMATCH);
  }
}

void BinderUtil::PromoteParameters(
    const common::ManagedPointer<std::vector<parser::ConstantValueExpression>> parameters,
    const std::vector<execution::sql::SqlTypeId> &desired_parameter_types) {
  NOISEPAGE_ASSERT(parameters->size() == desired_parameter_types.size(), "They have to be equal in size.");
  for (uint32_t parameter_index = 0; parameter_index < desired_parameter_types.size(); parameter_index++) {
    const auto desired_type = desired_parameter_types[parameter_index];

    if (desired_type != execution::sql::SqlTypeId::Invalid) {
      const auto param = common::ManagedPointer(&(*parameters)[parameter_index]);
      BinderUtil::CheckAndTryPromoteType(param, desired_type);
    }
  }
}

void BinderUtil::CheckAndTryPromoteType(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                        const execution::sql::SqlTypeId desired_type) {
  const auto curr_type = value->GetReturnValueType();

  // Check if types are mismatched, and convert them if possible.
  if (curr_type != desired_type) {
    switch (curr_type) {
      // NULL conversion.
      case execution::sql::SqlTypeId::Invalid: {
        value->SetValue(desired_type, execution::sql::Val(true));
        break;
      }

        // INTEGER casting (upwards and downwards).
      case execution::sql::SqlTypeId::TinyInt: {
        const auto int_val = value->Peek<int8_t>();
        TryCastNumericAll(value, int_val, desired_type);
        break;
      }
      case execution::sql::SqlTypeId::SmallInt: {
        const auto int_val = value->Peek<int16_t>();
        TryCastNumericAll(value, int_val, desired_type);
        break;
      }
      case execution::sql::SqlTypeId::Integer: {
        const auto int_val = value->Peek<int32_t>();
        TryCastNumericAll(value, int_val, desired_type);
        break;
      }
      case execution::sql::SqlTypeId::BigInt: {
        const auto int_val = value->Peek<int64_t>();
        TryCastNumericAll(value, int_val, desired_type);
        break;
      }

        // DATE and TIMESTAMP conversion. String to boolean conversion. String to numeric type conversion.
      case execution::sql::SqlTypeId::Varchar: {
        const auto str_view = value->Peek<std::string_view>();

        // TODO(Matt): see issue #977
        switch (desired_type) {
          case execution::sql::SqlTypeId::Date: {
            try {
              auto parsed_date = execution::sql::Date::FromString(str_view);
              value->SetValue(execution::sql::SqlTypeId::Date, execution::sql::DateVal(parsed_date));
              break;
            } catch (ConversionException &exception) {
              // For now, treat all conversion errors as 22007.
              // TODO(amogkam): Differentiate between 22007 and 22008. See comments in PR #1462.
              throw BINDER_EXCEPTION(exception.what(), common::ErrorCode::ERRCODE_INVALID_DATETIME_FORMAT);
            }
          }
          case execution::sql::SqlTypeId::Timestamp: {
            try {
              auto parsed_timestamp = execution::sql::Timestamp::FromString(str_view);
              value->SetValue(execution::sql::SqlTypeId::Timestamp, execution::sql::TimestampVal(parsed_timestamp));
              break;
            } catch (ConversionException &exception) {
              // For now, treat all conversion errors as 22007.
              // TODO(amogkam): Differentiate between 22007 and 22008. See comments in PR #1462.
              throw BINDER_EXCEPTION(exception.what(), common::ErrorCode::ERRCODE_INVALID_DATETIME_FORMAT);
            }
          }
          case execution::sql::SqlTypeId::Boolean: {
            if (std::find(network::POSTGRES_BOOLEAN_STR_TRUES.cbegin(), network::POSTGRES_BOOLEAN_STR_TRUES.cend(),
                          str_view) != network::POSTGRES_BOOLEAN_STR_TRUES.cend()) {
              value->SetValue(execution::sql::SqlTypeId::Boolean, execution::sql::BoolVal(true));
            } else if (std::find(network::POSTGRES_BOOLEAN_STR_FALSES.cbegin(),
                                 network::POSTGRES_BOOLEAN_STR_FALSES.cend(),
                                 str_view) != network::POSTGRES_BOOLEAN_STR_FALSES.cend()) {
              value->SetValue(execution::sql::SqlTypeId::Boolean, execution::sql::BoolVal(false));
            } else {
              throw BINDER_EXCEPTION(fmt::format("invalid input syntax for type boolean: \"{}\"", str_view),
                                     common::ErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION);
            }
            break;
          }
          case execution::sql::SqlTypeId::TinyInt: {
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
            value->SetValue(execution::sql::SqlTypeId::TinyInt, execution::sql::Integer(int_val));
            break;
          }
          case execution::sql::SqlTypeId::SmallInt: {
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
            value->SetValue(execution::sql::SqlTypeId::SmallInt, execution::sql::Integer(int_val));
            break;
          }
          case execution::sql::SqlTypeId::Integer: {
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
            value->SetValue(execution::sql::SqlTypeId::Integer, execution::sql::Integer(int_val));
            break;
          }
          case execution::sql::SqlTypeId::BigInt: {
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
            value->SetValue(execution::sql::SqlTypeId::BigInt, execution::sql::Integer(int_val));
            break;
          }
          case execution::sql::SqlTypeId::Double: {
            {
              double double_val;
              try {
                double_val = std::stod(std::string(str_view));
              } catch (std::exception &e) {
                throw BINDER_EXCEPTION(fmt::format("real out of range, string to convert was {}", str_view),
                                       common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
              }
              value->SetValue(execution::sql::SqlTypeId::Double, execution::sql::Real(double_val));
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
                        execution::sql::SqlTypeIdToString(desired_type), execution::sql::SqlTypeIdToString(curr_type)),
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
                                   const Input int_val, const execution::sql::SqlTypeId desired_type) {
  switch (desired_type) {
    case execution::sql::SqlTypeId::TinyInt: {
      if (IsRepresentable<int8_t>(int_val)) {
        value->SetReturnValueType(desired_type);
        return;
      }
      throw BINDER_EXCEPTION(fmt::format("tinyint out of range. number to convert was {}", int_val),
                             common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }
    case execution::sql::SqlTypeId::SmallInt: {
      if (IsRepresentable<int16_t>(int_val)) {
        value->SetReturnValueType(desired_type);
        return;
      }
      throw BINDER_EXCEPTION(fmt::format("smallint out of range. number to convert was {}", int_val),
                             common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }
    case execution::sql::SqlTypeId::Integer: {
      if (IsRepresentable<int32_t>(int_val)) {
        value->SetReturnValueType(desired_type);
        return;
      }
      throw BINDER_EXCEPTION(fmt::format("integer out of range. number to convert was {}", int_val),
                             common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }
    case execution::sql::SqlTypeId::BigInt: {
      if (IsRepresentable<int64_t>(int_val)) {
        value->SetReturnValueType(desired_type);
        return;
      }
      throw BINDER_EXCEPTION(fmt::format("bigint out of range. number to convert was {}", int_val),
                             common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }
    case execution::sql::SqlTypeId::Double: {
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
                      execution::sql::SqlTypeIdToString(desired_type), int_val),
          common::ErrorCode::ERRCODE_DATA_EXCEPTION);
  }
}

template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                            const int8_t int_val, const execution::sql::SqlTypeId desired_type);
template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                            const int16_t int_val, const execution::sql::SqlTypeId desired_type);
template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                            const int32_t int_val, const execution::sql::SqlTypeId desired_type);
template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                            const int64_t int_val, const execution::sql::SqlTypeId desired_type);

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
