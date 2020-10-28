#pragma once

#include <string>
#include <vector>

#include "common/managed_pointer.h"
#include "network/network_defs.h"
#include "type/type_id.h"

namespace noisepage::parser {
class ConstantValueExpression;
}

namespace noisepage::network {

class ReadBufferView;

/**
 * Static helper class for dealing with the Postgres wire protocol
 */
class PostgresPacketUtil {
 public:
  PostgresPacketUtil() = delete;

  /**
   * Given a read buffer that starts at the format codes for a Parse or Bind message, reads the values out
   * @param read_buffer incoming postgres packet with next fields as format codes
   * @return vector of format codes for the attributes
   */
  static std::vector<FieldFormat> ReadFormatCodes(common::ManagedPointer<ReadBufferView> read_buffer);

  /**
   * Given a read buffer that starts at the parameter types for a Parse message, reads the values out
   * @param read_buffer incoming postgres packet with next fields as parameter types
   * @return vector of internal types for the parameters
   */
  static std::vector<type::TypeId> ReadParamTypes(common::ManagedPointer<ReadBufferView> read_buffer);

  /**
   * Given a read buffer that starts at a text value, consumes it and returns a ConstantValueExpression for that type
   * @param read_buffer incoming postgres packet with next field as a value
   * @param size size of the value
   * @param type internal type of the value
   * @return ConstantValueExpression containing the value from the packet
   */
  static parser::ConstantValueExpression TextValueToInternalValue(common::ManagedPointer<ReadBufferView> read_buffer,
                                                                  int32_t size, type::TypeId type);

  /**
   * Given a read buffer that starts at a binary value, consumes it and returns a ConstantValueExpression for that type
   * @param read_buffer incoming postgres packet with next field as a value
   * @param size size of the value
   * @param type internal type of the value
   * @return ConstantValueExpression containing the value from the packet
   */
  static parser::ConstantValueExpression BinaryValueToInternalValue(common::ManagedPointer<ReadBufferView> read_buffer,
                                                                    int32_t size, type::TypeId type);

  /**
   * Given a read buffer that starts at the parameter types for a Parse message, reads the values out
   * @param read_buffer incoming postgres packet with next fields as parameter types
   * @param param_types
   * @param param_formats
   * @return vector of internal types for the parameters
   */
  static std::vector<parser::ConstantValueExpression> ReadParameters(common::ManagedPointer<ReadBufferView> read_buffer,
                                                                     const std::vector<type::TypeId> &param_types,
                                                                     const std::vector<FieldFormat> &param_formats);
};

}  // namespace noisepage::network
