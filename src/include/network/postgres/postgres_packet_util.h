#pragma once

#include "network/network_io_utils.h"
#include "network/postgres/postgres_defs.h"
#include "postgres_protocol_utils.h"
#include "type/type_id.h"

namespace terrier::network {

class PostgresPacketUtil {
 public:
  PostgresPacketUtil() = delete;

  static std::vector<type::TypeId> ReadParamTypes(const common::ManagedPointer<ReadBufferView> read_buffer) {
    const auto num_params = read_buffer->ReadValue<int16_t>();
    std::vector<type::TypeId> param_types;
    param_types.reserve(num_params);
    for (uint16_t i = 0; i < num_params; i++) {
      param_types.emplace_back(PostgresValueTypeToInternalValueType(read_buffer->ReadValue<PostgresValueType>()));
    }
    return param_types;
  }
};

}  // namespace terrier::network