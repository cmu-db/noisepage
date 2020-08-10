#include "binder/binder_util.h"

#include <limits>

#include "parser/expression/constant_value_expression.h"

namespace terrier::binder {

void BinderUtil::PromoteParameters(
    const common::ManagedPointer<std::vector<parser::ConstantValueExpression> > parameters,
    const std::vector<type::TypeId> &desired_parameter_types) {
  TERRIER_ASSERT(parameters->size() == desired_parameter_types.size(), "They have to be equal in size.");
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

        // DATE and TIMESTAMP conversion. String to numeric type conversion.
      case type::TypeId::VARCHAR: {
        const auto str_view = value->Peek<std::string_view>();

        // TODO(WAN): A bit stupid to take the string view back into a string.
        switch (desired_type) {
          case type::TypeId::DATE: {
            auto parsed_date = util::TimeConvertor::ParseDate(std::string(str_view));
            if (!parsed_date.first) {
              ReportFailure("Binder conversion from VARCHAR to DATE failed.");
            }
            value->SetValue(
                type::TypeId::DATE,
                execution::sql::DateVal(execution::sql::Date::FromNative(static_cast<uint32_t>(parsed_date.second))));
            break;
          }
          case type::TypeId::TIMESTAMP: {
            auto parsed_timestamp = util::TimeConvertor::ParseTimestamp(std::string(str_view));
            if (!parsed_timestamp.first) {
              ReportFailure("Binder conversion from VARCHAR to TIMESTAMP failed.");
            }
            value->SetValue(type::TypeId::TIMESTAMP, execution::sql::TimestampVal(execution::sql::Timestamp::FromNative(
                                                         static_cast<uint64_t>(parsed_timestamp.second))));
            break;
          }
          case type::TypeId::TINYINT: {
            const auto int_val = std::stol(std::string(str_view));
            if (!IsRepresentable<int8_t>(int_val)) {
              throw BINDER_EXCEPTION("BinderSherpa cannot fit that VARCHAR into the desired type!");
            }
            value->SetValue(type::TypeId::TINYINT, execution::sql::Integer(int_val));
            break;
          }
          case type::TypeId::SMALLINT: {
            const auto int_val = std::stol(std::string(str_view));
            if (!IsRepresentable<int16_t>(int_val)) {
              throw BINDER_EXCEPTION("BinderSherpa cannot fit that VARCHAR into the desired type!");
            }
            value->SetValue(type::TypeId::SMALLINT, execution::sql::Integer(int_val));
            break;
          }
          case type::TypeId::INTEGER: {
            const auto int_val = std::stol(std::string(str_view));
            if (!IsRepresentable<int32_t>(int_val)) {
              throw BINDER_EXCEPTION("BinderSherpa cannot fit that VARCHAR into the desired type!");
            }
            value->SetValue(type::TypeId::INTEGER, execution::sql::Integer(int_val));
            break;
          }
          case type::TypeId::BIGINT: {
            const auto int_val = std::stol(std::string(str_view));
            if (!IsRepresentable<int64_t>(int_val)) {
              throw BINDER_EXCEPTION("BinderSherpa cannot fit that VARCHAR into the desired type!");
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
                throw BINDER_EXCEPTION("BinderSherpa cannot fit that VARCHAR into the desired type!");
              }
              value->SetValue(type::TypeId::DECIMAL, execution::sql::Real(double_val));
              break;
            }
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

template <typename Output, typename Input>
bool BinderUtil::IsRepresentable(const Input int_val) {
  return std::numeric_limits<Output>::lowest() <= int_val && int_val <= std::numeric_limits<Output>::max();
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
        value->SetValue(desired_type, execution::sql::Real(static_cast<double>(int_val)));
        return;
      }
      break;
    }
    default:
      throw BINDER_EXCEPTION("TryCastNumericAll not a numeric type!");
  }
  throw BINDER_EXCEPTION("TryCastNumericAll value out of bounds!");
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

}  // namespace terrier::binder
