#pragma once

#include <string>
#include <vector>

#include "execution/util/execution_common.h"
#include "network/network_io_utils.h"
#include "network/postgres/postgres_defs.h"
#include "network/postgres/postgres_packet_writer.h"
#include "network/postgres/postgres_protocol_utils.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

namespace terrier::network {

class PostgresPacketUtil {
 public:
  PostgresPacketUtil() = delete;

  static std::vector<FieldFormat> ReadFormatCodes(const common::ManagedPointer<ReadBufferView> read_buffer) {
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

  static std::vector<type::TypeId> ReadParamTypes(const common::ManagedPointer<ReadBufferView> read_buffer) {
    const auto num_params = read_buffer->ReadValue<int16_t>();
    std::vector<type::TypeId> param_types;
    param_types.reserve(num_params);
    for (uint16_t i = 0; i < num_params; i++) {
      param_types.emplace_back(PostgresValueTypeToInternalValueType(read_buffer->ReadValue<PostgresValueType>()));
    }
    return param_types;
  }

  static type::TransientValue TextValueToInternalValue(const common::ManagedPointer<ReadBufferView> read_buffer,
                                                       const int32_t size, const type::TypeId type) {
    if (size == -1) {
      // it's a NULL
      return type::TransientValueFactory::GetNull(type);
    }

    const auto string_val = read_buffer->ReadString(size);
    switch (type) {
      case type::TypeId::BOOLEAN: {
        // Matt: as best as I can tell, we only expect 'TRUE' of 'FALSE' coming in here, rather than the 't' or 'f' that
        // results use. We can simplify this logic a bit if that assumption can be verified
        if (string_val == "TRUE") return type::TransientValueFactory::GetBoolean(true);
        TERRIER_ASSERT(string_val == "FALSE", "Input equals something other than TRUE or FALSE. We should check that.");
        return type::TransientValueFactory::GetBoolean(false);
      }
      case type::TypeId::TINYINT:
        return type::TransientValueFactory::GetTinyInt(static_cast<int8_t>(std::stoll(string_val)));
      case type::TypeId::SMALLINT:
        return type::TransientValueFactory::GetSmallInt(static_cast<int16_t>(std::stoll(string_val)));
      case type::TypeId::INTEGER:
        return type::TransientValueFactory::GetInteger(static_cast<int32_t>(std::stoll(string_val)));
      case type::TypeId::BIGINT:
        return type::TransientValueFactory::GetBigInt(static_cast<int64_t>(std::stoll(string_val)));
      case type::TypeId::DECIMAL:
        return type::TransientValueFactory::GetDecimal(std::stod(string_val));
      case type::TypeId::VARCHAR:
        return type::TransientValueFactory::GetVarChar(string_val);
      default:
        UNREACHABLE("Unsupported type for parameter.");
    }
  }

  static type::TransientValue BinaryValueToInternalValue(const common::ManagedPointer<ReadBufferView> read_buffer,
                                                         const int32_t size, const type::TypeId type) {
    if (size == -1) {
      // it's a NULL
      return type::TransientValueFactory::GetNull(type);
    }

    switch (type) {
      case type::TypeId::TINYINT: {
        TERRIER_ASSERT(size == 1, "Unexpected size for this type.");
        return type::TransientValueFactory::GetTinyInt(read_buffer->ReadValue<int8_t>());
      }
      case type::TypeId::SMALLINT: {
        TERRIER_ASSERT(size == 2, "Unexpected size for this type.");
        return type::TransientValueFactory::GetSmallInt(read_buffer->ReadValue<int16_t>());
      }
      case type::TypeId::INTEGER: {
        TERRIER_ASSERT(size == 4, "Unexpected size for this type.");
        return type::TransientValueFactory::GetInteger(read_buffer->ReadValue<int32_t>());
      }
      case type::TypeId::BIGINT: {
        TERRIER_ASSERT(size == 8, "Unexpected size for this type.");
        return type::TransientValueFactory::GetBigInt(read_buffer->ReadValue<int64_t>());
      }
      case type::TypeId::DECIMAL: {
        TERRIER_ASSERT(size == 8, "Unexpected size for this type.");
        return type::TransientValueFactory::GetDecimal(read_buffer->ReadValue<double>());
      }
      default:
        // TODO(Matt): from looking at JDBC source code, DATE is the only other possible binary value
        // https://github.com/pgjdbc/pgjdbc/blob/db228a4ffd8b356a9028363b35b0eb9055ea53f0/pgjdbc/src/main/java/org/postgresql/jdbc/PgPreparedStatement.java
        // line 1272
        UNREACHABLE("Unsupported type for parameter.");
    }
  }

  static std::vector<type::TransientValue> ReadParameters(const common::ManagedPointer<ReadBufferView> read_buffer,
                                                          const std::vector<type::TypeId> &param_types,
                                                          const std::vector<FieldFormat> &param_formats) {
    const auto num_params = static_cast<size_t>(read_buffer->ReadValue<int16_t>());
    std::vector<type::TransientValue> params;
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
};

}  // namespace terrier::network
