#pragma once

#include <string>

#include "common/managed_pointer.h"
#include "messenger/connection_destination.h"
#include "messenger/messenger.h"

namespace noisepage::replication {

/** Abstraction around a replica. */
class Replica {
 public:
  /**
   * Create a replica.
   *
   * @param messenger       The messenger to use.
   * @param replica_name    The name of the replica.
   * @param hostname        The hostname of the replica.
   * @param port            The port of the replica.
   */
  Replica(common::ManagedPointer<messenger::Messenger> messenger, const std::string &replica_name,
          const std::string &hostname, uint16_t port);

  /** @return The connection ID for this replica. */
  messenger::connection_id_t GetConnectionId() const { return connection_id_; }

 private:
  messenger::ConnectionDestination replica_info_;  ///< The connection metadata for this replica.
  messenger::connection_id_t connection_id_;       ///< The connection ID to this replica.
};

}  // namespace noisepage::replication
