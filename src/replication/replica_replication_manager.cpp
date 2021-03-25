#include "replication/replica_replication_manager.h"

#include "loggers/replication_logger.h"
#include "replication/replication_messages.h"

namespace noisepage::replication {

ReplicaReplicationManager::ReplicaReplicationManager(
    common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
    const std::string &replication_hosts_path,
    common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue)
    : ReplicationManager(messenger, network_identity, port, replication_hosts_path, empty_buffer_queue) {}

ReplicaReplicationManager::~ReplicaReplicationManager() = default;

void ReplicaReplicationManager::Handle(const messenger::ZmqMessage &zmq_msg, const RecordsBatchMsg &msg) {
  REPLICATION_LOG_TRACE(fmt::format("[RECV] RecordsBatchMsg from {}: ID {} BATCH {}", zmq_msg.GetRoutingId(),
                                    msg.GetMessageId(), msg.GetBatchId()));
  // Acknowledge receipt of the batch of records.
  SendAckForMessage(zmq_msg, msg);
  // Add the batch of log records directly to the provider, which handles out of order batches.
  provider_.AddBatchOfRecords(msg);
}

void ReplicaReplicationManager::EventLoop(common::ManagedPointer<messenger::Messenger> messenger,
                                          const messenger::ZmqMessage &zmq_msg,
                                          common::ManagedPointer<BaseReplicationMessage> msg) {
  switch (msg->GetMessageType()) {
    case ReplicationMessageType::RECORDS_BATCH: {
      Handle(zmq_msg, *msg.CastManagedPointerTo<RecordsBatchMsg>());
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
  msg_id_t msg_id = GetNextMessageId();
  REPLICATION_LOG_TRACE(fmt::format("[SEND] TxnAppliedMsg -> primary: ID {} START {}", msg_id, txn_start_time));

  TxnAppliedMsg msg(ReplicationMessageMetadata(msg_id), txn_start_time);
  Send("primary", msg, nullptr, messenger::Messenger::GetBuiltinCallback(messenger::Messenger::BuiltinCallback::NOOP),
       true);
}

}  // namespace noisepage::replication
