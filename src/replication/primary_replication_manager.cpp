#include "replication/primary_replication_manager.h"

#include "common/json.h"
#include "loggers/replication_logger.h"
#include "replication/replication_messages.h"

namespace noisepage::replication {

PrimaryReplicationManager::PrimaryReplicationManager(
    common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
    const std::string &replication_hosts_path,
    common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue)
    : ReplicationManager(messenger, network_identity, port, replication_hosts_path, empty_buffer_queue) {}

PrimaryReplicationManager::~PrimaryReplicationManager() = default;

void PrimaryReplicationManager::EventLoop(common::ManagedPointer<messenger::Messenger> messenger,
                                          const messenger::ZmqMessage &zmq_msg,
                                          common::ManagedPointer<BaseReplicationMessage> msg) {
  switch (msg->GetMessageType()) {
    case ReplicationMessageType::TXN_APPLIED: {
      Handle(zmq_msg, *msg.CastManagedPointerTo<TxnAppliedMsg>());
      break;
    }
    default: {
      // Delegate to the common ReplicationManager event loop.
      ReplicationManager::EventLoop(messenger, zmq_msg, msg);
      break;
    }
  }
}

void PrimaryReplicationManager::ReplicateBatchOfRecords(storage::BufferedLogWriter *records_batch,
                                                        const std::vector<storage::CommitCallback> &commit_callbacks,
                                                        const transaction::ReplicationPolicy &policy,
                                                        const transaction::timestamp_t newest_buffer_txn) {
  REPLICATION_LOG_TRACE(fmt::format("[SEND] Preparing ReplicateBatchOfRecords."));
  NOISEPAGE_ASSERT(policy != transaction::ReplicationPolicy::DISABLE, "Replication is disabled, so why are we here?");

  // Handle the degenerate case of having no replicas.
  if (replicas_.empty()) {
    // Invoke all the commit callbacks.
    for (const auto &cb : commit_callbacks) {
      cb.fn_(cb.arg_);
    }
    // Return the buffered log writer to the pool if necessary.
    if (records_batch != nullptr && records_batch->MarkSerialized()) {
      empty_buffer_queue_->Enqueue(records_batch);
    }
    return;
  }

  if (policy == transaction::ReplicationPolicy::ASYNC) {
    // In asynchronous replication, just invoke the commit callbacks immediately.
    for (const auto &cb : commit_callbacks) {
      cb.fn_(cb.arg_);
    }
  } else {
    // Copy the commit callbacks into our local list. The callbacks are invoked when the replicas notify the primary
    // that the replicas have applied their corresponding transactions.
    {
      std::unique_lock lock(callbacks_mutex_);
      // If there are currently no callbacks, execute everything that won't be sent over to the replica.
      bool was_empty = txn_callbacks_.empty();
      txn_callbacks_.emplace(commit_callbacks);
      if (was_empty) {
        // TODO(WAN): The log serializer task is then sometimes processing transaction callbacks, where normally
        //            this would be done by the Messenger's dedicated thread as part of the server-loop callback.
        ProcessTxnCallbacks();
      }
    }
  }

  if (records_batch != nullptr) {
    // Send the batch of records to all replicas.
    ReplicationMessageMetadata metadata(GetNextMessageId());
    RecordsBatchMsg msg(metadata, GetNextBatchId(), records_batch);
    REPLICATION_LOG_TRACE(fmt::format("[SEND] BATCH {}", msg.GetBatchId()));

    messenger::callback_id_t destination_cb =
        messenger::Messenger::GetBuiltinCallback(messenger::Messenger::BuiltinCallback::NOOP);
    const msg_id_t msg_id = msg.GetMessageId();
    const std::string msg_string = msg.Serialize();
    for (const auto &replica : replicas_) {
      Send(replica.first, msg_id, msg_string, messenger::CallbackFns::Noop, destination_cb);
    }

    NOISEPAGE_ASSERT(newest_buffer_txn >= newest_txn_sent_,
                     "The assumption is that transactions are monotonically increasing.");
    newest_txn_sent_ = newest_buffer_txn;

    // Return the buffered log writer to the pool if necessary.
    if (records_batch->MarkSerialized()) {
      empty_buffer_queue_->Enqueue(records_batch);
    }
  }
}

void PrimaryReplicationManager::NotifyReplicasOfOAT(transaction::timestamp_t oldest_active_txn) {
  ReplicationMessageMetadata metadata(GetNextMessageId());
  NotifyOATMsg msg(metadata, last_sent_batch_id_, oldest_active_txn);
  REPLICATION_LOG_TRACE(fmt::format("[SEND] BATCH {} OAT {}", msg.GetBatchId(), msg.GetOldestActiveTxn()));

  messenger::callback_id_t destination_cb =
      messenger::Messenger::GetBuiltinCallback(messenger::Messenger::BuiltinCallback::NOOP);

  const msg_id_t msg_id = msg.GetMessageId();
  const std::string msg_string = msg.Serialize();
  for (const auto &replica : replicas_) {
    Send(replica.first, msg_id, msg_string, messenger::CallbackFns::Noop, destination_cb);
  }
}

record_batch_id_t PrimaryReplicationManager::GetNextBatchId() {
  record_batch_id_t next_batch_id = next_batch_id_++;
  if (next_batch_id_ == INVALID_RECORD_BATCH_ID) {
    next_batch_id_++;
  }
  last_sent_batch_id_ = next_batch_id;
  return next_batch_id;
}

void PrimaryReplicationManager::Handle(const messenger::ZmqMessage &zmq_msg, const TxnAppliedMsg &msg) {
  REPLICATION_LOG_TRACE(fmt::format("[RECV] TxnAppliedMsg from {}: ID {} TXN {}", zmq_msg.GetRoutingId(),
                                    msg.GetMessageId(), msg.GetAppliedTxnId()));
  // Mark the transaction as applied by the specific replica.
  transaction::timestamp_t txn_id = msg.GetAppliedTxnId();
  {
    std::unique_lock lock(callbacks_mutex_);
    if (txns_applied_on_replicas_.find(txn_id) == txns_applied_on_replicas_.end()) {
      txns_applied_on_replicas_.emplace(txn_id, std::unordered_set<std::string>{});
    }
    std::unordered_set<std::string> &replicas = txns_applied_on_replicas_.at(txn_id);
    replicas.emplace(zmq_msg.GetRoutingId());
    // Check if all the replicas have applied this transaction.
    if (replicas.size() == replicas_.size()) {
      // If so, there may be new transaction callbacks that we can invoke.
      ProcessTxnCallbacks();
    }
  }
}

void PrimaryReplicationManager::ProcessTxnCallbacks() {
  while (!txn_callbacks_.empty()) {
    // Check that each respective callback's transaction has been applied on all the replicas.
    std::vector<storage::CommitCallback> &callbacks = txn_callbacks_.front();
    for (auto vec_it = callbacks.begin(); vec_it != callbacks.end();) {
      storage::CommitCallback &callback = *vec_it;
      // Read-only commit records are not even replicated. Only check if not read-only.
      if (!callback.is_from_read_only_) {
        // Otherwise, the commit record was replicated, so check if all the replicas have applied the transaction.
        bool all_replicas_applied =
            (txns_applied_on_replicas_.find(callback.txn_start_time_) != txns_applied_on_replicas_.end()) &&
            (txns_applied_on_replicas_.at(callback.txn_start_time_).size() == replicas_.size());
        if (!all_replicas_applied) {
          return;
        }
      }
      // If read-only commit record OR all replicas have applied the txn, then invoke and erase the callback.
      callback.fn_(callback.arg_);
      txns_applied_on_replicas_.erase(callback.txn_start_time_);
      REPLICATION_LOG_TRACE(fmt::format("Commit callback invoked for txn: {}", callback.txn_start_time_));
      vec_it = callbacks.erase(vec_it);
    }
    // If all the callbacks in one batch have been exhausted, erase the exhausted batch.
    txn_callbacks_.pop();
  }
}

}  // namespace noisepage::replication
