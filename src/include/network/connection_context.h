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
  /* The statements in this connection */
  std::unordered_map<std::string, traffic_cop::Statement> statements;

  /* The portals in this connection */
  std::unordered_map<std::string, traffic_cop::Portal> portals;
};

}  // namespace terrier::network
