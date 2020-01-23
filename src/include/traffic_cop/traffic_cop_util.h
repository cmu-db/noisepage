#pragma once

#include <memory>
#include <string>

#include "common/managed_pointer.h"
#include "network/network_defs.h"

namespace terrier::parser {
class SQLStatement;
}

namespace terrier::trafficcop {

/**
 * Static helper methods for accessing some of the TrafficCop's functionality without instantiating an object
 */
class TrafficCopUtil {
 public:
  TrafficCopUtil() = delete;

  /**
   * Converts parser statement types (which rely on multiple enums) to a single QueryType enum from the network layer
   * @param statement
   * @return
   */
  static network::QueryType QueryTypeForStatement(common::ManagedPointer<parser::SQLStatement> statement);
};

}  // namespace terrier::trafficcop
