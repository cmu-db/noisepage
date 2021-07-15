#include "network/postgres/postgres_packet_util.h"

#include <string>
#include <vector>

#include "execution/sql/value.h"
#include "execution/sql/value_util.h"
#include "execution/util/execution_common.h"
#include "network/network_io_utils.h"
#include "network/postgres/postgres_defs.h"
#include "network/postgres/postgres_protocol_util.h"
#include "parser/expression/constant_value_expression.h"
#include "type/type_id.h"

namespace noisepage::network {

std::vector<FieldFormat> PostgresPacketUtil::ReadFormatCodes(const common::ManagedPointer<ReadBufferView> read_buffer) {
  const auto num_formats = read_buffer->ReadValue<int16_t>();

  if (num_formats == 0) {
    return {FieldFormat::text};
  }

  std::vector<FieldFormat> formats;
  formats.reserve(num_formats);
  for (uint16_t i = 0; i < num_formats; i++) {
    formats.emplace_back(static_cast<FieldFormat>(read_buffer->ReadValue<int16_t>()));
  }
  return formats;
}

std::vector<execution::sql::SqlTypeId> PostgresPacketUtil::ReadParamTypes(const common::ManagedPointer<ReadBufferView> read_buffer) {
  const auto num_params = read_buffer->ReadValue<int16_t>();
  std::vector<execution::sql::SqlTypeId> param_types;
  param_types.reserve(num_params);
  for (uint16_t i = 0; i < num_params; i++) {
    param_types.emplace_back(
        PostgresProtocolUtil::PostgresValueTypeToInternalValueType(read_buffer->ReadValue<PostgresValueType>()));
  }
  return param_types;
}

parser::ConstantValueExpression PostgresPacketUtil::TextValueToInternalValue(
    const common::ManagedPointer<ReadBufferView> read_buffer, const int32_t size, const execution::sql::SqlTypeId type) {
  if (size == -1) {
    // it's a NULL
    return {type, execution::sql::Val(true)};
  }

  const auto string = read_buffer->ReadString(size);
  switch (type) {
    case execution::sql::SqlTypeId::Boolean: {
      // Matt: as best as I can tell, we only expect 'TRUE' of 'FALSE' coming in here, rather than the 't' or 'f' that
      // results use. We can simplify this logic a bit if that assumption can be verified
      if (string == "TRUE") return {type, execution::sql::BoolVal(true)};
      NOISEPAGE_ASSERT(string == "FALSE", "Input equals something other than TRUE or FALSE. We should check that.");
      return {type, execution::sql::BoolVal(false)};
    }
    case execution::sql::SqlTypeId::TinyInt:
      return {type, execution::sql::Integer(static_cast<int8_t>(std::stoll(string)))};
    case execution::sql::SqlTypeId::SmallInt:
      return {type, execution::sql::Integer(static_cast<int16_t>(std::stoll(string)))};
    case execution::sql::SqlTypeId::Integer:
      return {type, execution::sql::Integer(static_cast<int32_t>(std::stoll(string)))};
    case execution::sql::SqlTypeId::BigInt:
      return {type, execution::sql::Integer(static_cast<int64_t>(std::stoll(string)))};
    case execution::sql::SqlTypeId::Double:
      return {type, execution::sql::Real(std::stod(string))};
    case execution::sql::SqlTypeId::Varchar: {
      auto string_val = execution::sql::ValueUtil::CreateStringVal(string);
      return {type, string_val.first, std::move(string_val.second)};
    }
    case execution::sql::SqlTypeId::Timestamp: {
      const auto parse_result = execution::sql::Timestamp::FromString(string);
      return {type, execution::sql::TimestampVal(parse_result)};
    }
    case execution::sql::SqlTypeId::Date: {
      const auto parse_result = execution::sql::Date::FromString(string);
      return {type, execution::sql::DateVal(parse_result)};
    }
    case execution::sql::SqlTypeId::Invalid: {
      // Postgres may not have told us the type in Parse message. Right now in oltpbench the JDBC driver is doing this
      // with timestamps on inserting into the Customer table. Let's just try to parse it and fall back to VARCHAR?
      try {
        const auto ts_parse_result = execution::sql::Timestamp::FromString(string);
        return {execution::sql::SqlTypeId::Timestamp, execution::sql::TimestampVal(ts_parse_result)};
      } catch (...) {
      }
      // try date?
      try {
        const auto date_parse_result = execution::sql::Date::FromString(string);
        return {execution::sql::SqlTypeId::Date, execution::sql::DateVal(date_parse_result)};
      } catch (...) {
      }
      // fall back to VARCHAR?
      auto string_val = execution::sql::ValueUtil::CreateStringVal(string);
      return {execution::sql::SqlTypeId::Varchar, string_val.first, std::move(string_val.second)};
    }
    default:
      // TODO(Matt): Note that not all types are handled yet. Add them as we support them.
      UNREACHABLE("Unsupported type for parameter.");
  }
}

parser::ConstantValueExpression PostgresPacketUtil::BinaryValueToInternalValue(
    const common::ManagedPointer<ReadBufferView> read_buffer, const int32_t size, const execution::sql::SqlTypeId type) {
  if (size == -1) {
    // it's a NULL
    return {type, execution::sql::Val(true)};
  }

  switch (type) {
    case execution::sql::SqlTypeId::TinyInt: {
      NOISEPAGE_ASSERT(size == 1, "Unexpected size for this type.");
      return {type, execution::sql::Integer(read_buffer->ReadValue<int8_t>())};
    }
    case execution::sql::SqlTypeId::SmallInt: {
      NOISEPAGE_ASSERT(size == 2, "Unexpected size for this type.");
      return {type, execution::sql::Integer(read_buffer->ReadValue<int16_t>())};
    }
    case execution::sql::SqlTypeId::Integer: {
      NOISEPAGE_ASSERT(size == 4, "Unexpected size for this type.");
      return {type, execution::sql::Integer(read_buffer->ReadValue<int32_t>())};
    }
    case execution::sql::SqlTypeId::BigInt: {
      NOISEPAGE_ASSERT(size == 8, "Unexpected size for this type.");
      return {type, execution::sql::Integer(read_buffer->ReadValue<int64_t>())};
    }
    case execution::sql::SqlTypeId::Double: {
      NOISEPAGE_ASSERT(size == 8, "Unexpected size for this type.");
      return {type, execution::sql::Real(read_buffer->ReadValue<double>())};
    }
    case execution::sql::SqlTypeId::Date: {
      // TODO(Matt): unsure if this is correct. Need tests.
      return {type, execution::sql::DateVal(static_cast<uint32_t>(read_buffer->ReadValue<int32_t>()))};
    }
    default:
      // (Matt): from looking at jdbc source code, that seems like all the possible binary types
      UNREACHABLE("Unsupported type for parameter.");
  }
}

std::vector<parser::ConstantValueExpression> PostgresPacketUtil::ReadParameters(
    const common::ManagedPointer<ReadBufferView> read_buffer, const std::vector<execution::sql::SqlTypeId> &param_types,
    const std::vector<FieldFormat> &param_formats) {
  const auto num_params = static_cast<size_t>(read_buffer->ReadValue<int16_t>());
  NOISEPAGE_ASSERT(num_params == param_types.size(),
                   "We don't support type inference on parameters yet, so the size of param_types should equal the "
                   "number of parameters.");
  std::vector<parser::ConstantValueExpression> params;
  params.reserve(num_params);
  for (uint16_t i = 0; i < num_params; i++) {
    const auto param_size = read_buffer->ReadValue<int32_t>();

    const auto param_format = param_formats[i <= param_formats.size() ? i : 0];

    params.emplace_back(param_format == FieldFormat::text
                            ? TextValueToInternalValue(read_buffer, param_size, param_types[i])
                            : BinaryValueToInternalValue(read_buffer, param_size, param_types[i]));
  }
  return params;
}

}  // namespace noisepage::network
