#include "replication/replica.h"

namespace noisepage::replication {

Replica::Replica(common::ManagedPointer<messenger::Messenger> messenger, const std::string &replica_name,
                 const std::string &hostname, uint16_t port)
    : replica_info_(messenger::ConnectionDestination::MakeTCP(replica_name, hostname, port)),
      connection_id_(messenger->MakeConnection(replica_info_)) {}

}  // namespace noisepage::replication
