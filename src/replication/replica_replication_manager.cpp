#include "replication/replica_replication_manager.h"

namespace noisepage::replication {

ReplicaReplicationManager::ReplicaReplicationManager(
    common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
    const std::string &replication_hosts_path,
    common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue)
    : ReplicationManager(messenger, network_identity, port, replication_hosts_path, empty_buffer_queue) {}

ReplicaReplicationManager::~ReplicaReplicationManager() = default;

void ReplicaReplicationManager::Handle(const messenger::ZmqMessage &zmq_msg, const RecordsBatchMsg &msg) {
  // TODO(WAN): process batch of records
}

void ReplicaReplicationManager::EventLoop(common::ManagedPointer<messenger::Messenger> messenger,
                                          const messenger::ZmqMessage &zmq_msg, const BaseReplicationMessage &msg) {
  switch (msg.GetMessageType()) {
    case ReplicationMessageType::RECORDS_BATCH: {
      Handle(zmq_msg, *(msg.GetAs<RecordsBatchMsg>()));
      break;
    }
    default: {
      // Delegate to the common ReplicationManager event loop.
      ReplicationManager::EventLoop(messenger, zmq_msg, msg);
      break;
    }
  }
}

void ReplicaReplicationManager::NotifyPrimaryTransactionApplied(transaction::timestamp_t txn_start_time) {
  // TODO(WAN): send message to primary, add corresponding handling code
}

}  // namespace noisepage::replication
