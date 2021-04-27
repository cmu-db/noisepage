#include "replication/replica_replication_manager.h"

#include "common/json.h"
#include "loggers/replication_logger.h"
#include "replication/replication_messages.h"

namespace noisepage::replication {

ReplicaReplicationManager::ReplicaReplicationManager(
    common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
    const std::string &replication_hosts_path,
    common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue)
    : ReplicationManager(messenger, network_identity, port, replication_hosts_path, empty_buffer_queue) {}

ReplicaReplicationManager::~ReplicaReplicationManager() = default;

void ReplicaReplicationManager::Handle(const messenger::ZmqMessage &zmq_msg, const NotifyOATMsg &msg) {
  REPLICATION_LOG_TRACE(fmt::format("[RECV] NotifyOATMsg from {}: OAT {} BATCH {}", zmq_msg.GetRoutingId(),
                                    msg.GetOldestActiveTxn(), msg.GetBatchId()));
  // Notify the provider that the latest OAT has arrived, though the OAT itself may need to be batched.
  provider_.UpdateOAT(msg.GetOldestActiveTxn(), msg.GetBatchId());
}

void ReplicaReplicationManager::Handle(const messenger::ZmqMessage &zmq_msg, const RecordsBatchMsg &msg) {
  REPLICATION_LOG_TRACE(fmt::format("[RECV] RecordsBatchMsg from {}: ID {} BATCH {}", zmq_msg.GetRoutingId(),
                                    msg.GetMessageId(), msg.GetBatchId()));
  // Add the batch of log records directly to the provider, which handles out of order batches.
  provider_.AddBatchOfRecords(msg);
}

void ReplicaReplicationManager::EventLoop(common::ManagedPointer<messenger::Messenger> messenger,
                                          const messenger::ZmqMessage &zmq_msg,
                                          common::ManagedPointer<BaseReplicationMessage> msg) {
  switch (msg->GetMessageType()) {
    case ReplicationMessageType::NOTIFY_OAT: {
      Handle(zmq_msg, *msg.CastManagedPointerTo<NotifyOATMsg>());
      break;
    }
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
  const std::string msg_string = msg.Serialize();
  Send("primary", msg_id, msg_string, nullptr,
       messenger::Messenger::GetBuiltinCallback(messenger::Messenger::BuiltinCallback::NOOP));
}

}  // namespace noisepage::replication
