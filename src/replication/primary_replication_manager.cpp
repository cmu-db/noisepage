#include "replication/primary_replication_manager.h"

#include "replication/replication_messages.h"

namespace noisepage::replication {

PrimaryReplicationManager::PrimaryReplicationManager(
    common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
    const std::string &replication_hosts_path,
    common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue)
    : ReplicationManager(messenger, network_identity, port, replication_hosts_path, empty_buffer_queue) {}

PrimaryReplicationManager::~PrimaryReplicationManager() = default;

void PrimaryReplicationManager::EventLoop(common::ManagedPointer<messenger::Messenger> messenger,
                                          const messenger::ZmqMessage &zmq_msg, const BaseReplicationMessage &msg) {
  switch (msg.GetMessageType()) {
    case ReplicationMessageType::TXN_APPLIED: {
      Handle(zmq_msg, *(msg.GetAs<TxnAppliedMsg>()));
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
                                                        const std::vector<storage::CommitCallback> &commit_callbacks) {
  // Copy the commit callbacks into our local list.
  txn_callbacks_.emplace_back(commit_callbacks);

  NOISEPAGE_ASSERT(records_batch != nullptr,
                   "Don't try to replicate null buffers. That's pointless."
                   "You might plausibly want to track statistics at some point, but that should not happen here.");
  ReplicationMessageMetadata metadata(GetNextMessageId());

  RecordsBatchMsg msg(metadata, next_batch_id_++, records_batch);

  messenger::messenger_cb_id_t destination_cb =
      messenger::Messenger::GetBuiltinCallback(messenger::Messenger::BuiltinCallback::NOOP);
  for (const auto &replica : replicas_) {
    Send(replica.first, msg, messenger::CallbackFns::Noop, destination_cb);
  }

  if (records_batch->MarkSerialized()) {
    empty_buffer_queue_->Enqueue(records_batch);
  }
}

void PrimaryReplicationManager::Handle(const messenger::ZmqMessage &zmq_msg, const TxnAppliedMsg &msg) {
  // Mark the transaction as applied by the specific replica.
  transaction::timestamp_t txn_id = msg.GetAppliedTxnId();
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

void PrimaryReplicationManager::ProcessTxnCallbacks() {
  // TODO(WAN): In theory, is async just "go ahead and process these without checking"?
  // TODO(WAN): Do NOT reuse this function without checking for races.
  std::vector<std::vector<storage::CommitCallback>> tmp_txn_callbacks;
  tmp_txn_callbacks = std::move(txn_callbacks_);

  for (auto vecvec_it = tmp_txn_callbacks.begin(); vecvec_it != tmp_txn_callbacks.end();) {
    std::vector<storage::CommitCallback> &callbacks = *vecvec_it;
    for (auto vec_it = callbacks.begin(); vec_it != callbacks.end();) {
      storage::CommitCallback &callback = *vec_it;
      // Check if all the replicas have applied the transaction.
      bool all_replicas_applied = txns_applied_on_replicas_.at(callback.txn_start_time_).size() == replicas_.size();
      if (!all_replicas_applied) {
        return;
      }
      // If all replicas have applied the transaction, then invoke the callback and erase the callback.
      callback.fn_(callback.arg_);
      vec_it = callbacks.erase(vec_it);
    }
    // If all the callbacks in one batch have been exhausted, erase the exhausted batch.
    vecvec_it = tmp_txn_callbacks.erase(vecvec_it);
  }
}

}  // namespace noisepage::replication
