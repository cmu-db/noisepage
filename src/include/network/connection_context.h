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
  /**
   * Commandline arguments parsed from protocol interpreter
   */
  std::unordered_map<std::string, std::string> cmdline_args_;

  /**
   * The OID of the database accessed by this connection
   */
  catalog::db_oid_t db_oid_;

  /**
   * The OID of the temporary namespace for this connection
   */
  catalog::namespace_oid_t temp_namespace_oid_;

  /**
   * The statements in this connection
   */
  std::unordered_map<std::string, trafficcop::Statement> statements_;

  /**
   * The portals in this connection
   */
  std::unordered_map<std::string, trafficcop::Portal> portals_;

  /**
   * Indicate whether the current command is in a transaction block
   */
  bool in_transaction_ = false;

  /**
   * Cleans up this ConnectionContext.
   * This is called when its connection handle is reused to occupy another connection or destroyed.
   */
  void Reset() {
    // Cleans up all the sqlite statements in this connection
    for (auto pair : statements_) pair.second.Finalize();
    cmdline_args_.clear();
    db_oid_ = catalog::INVALID_DATABASE_OID;
    temp_namespace_oid_ = catalog::INVALID_NAMESPACE_OID;
    statements_.clear();
    portals_.clear();
  }
};

}  // namespace terrier::network
