#pragma once

#include <string>
#include <unordered_map>

#include "traffic_cop/portal.h"
#include "traffic_cop/statement.h"

namespace terrier::network {

/**
 * A ConnectionContext stores the state of a connection.
 */

struct ConnectionContext {
  std::unordered_map<std::string, traffic_cop::Statement> statements;
  std::unordered_map<std::string, traffic_cop::Portal> portals;
};

}  // namespace terrier::network
