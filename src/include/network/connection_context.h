#pragma once

#include <unordered_map>
#include <string>

#include "traffic_cop/statement.h"

namespace terrier::network {

/**
 * A ConnectionContext stores the state of a connection.
 */

struct ConnectionContext {
  std::unordered_map<std::string, traffic_cop::Statement> statements;


};

}