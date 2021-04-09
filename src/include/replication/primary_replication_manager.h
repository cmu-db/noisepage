#pragma once

#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "replication/replication_manager.h"
#include "storage/write_ahead_log/log_io.h"
#include "transaction/transaction_defs.h"

namespace noisepage::replication {

/** The replication manager that should run on the primary. */
class PrimaryReplicationManager final : public ReplicationManager {
 public:
  /**
   * On construction, the replication manager establishes a listen destination on the messenger and connect to all the
   * replicas specified in the replication.config file located at @p replication_hosts_path.
   *
   * @param messenger                   The messenger instance to use.
   * @param network_identity            The identity of this node in the network.
   * @param port                        The port to listen on.
   * @param replication_hosts_path      The path to the replication.config file.
   * @param empty_buffer_queue          A queue of empty buffers that the replication manager may return buffers to.
   */
  PrimaryReplicationManager(
      common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
      const std::string &replication_hosts_path,
      common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue);

  /** Destructor. */
  ~PrimaryReplicationManager() final;

  /** @return True since this is the primary. */
  bool IsPrimary() const override { return true; }

  /**
   * Send a batch of log records to all of the replicas.
   *
   * @param records_batch       The batch of records to be replicated.
   * @param commit_callbacks    The commit callbacks associated with the batch of records.
   * @param policy              The replication policy to use.
   * @param newest_buffer_txn   (performance optimization) The newest transaction inside the batch of records.
   */
  void ReplicateBatchOfRecords(storage::BufferedLogWriter *records_batch,
                               const std::vector<storage::CommitCallback> &commit_callbacks,
                               const transaction::ReplicationPolicy &policy,
                               transaction::timestamp_t newest_buffer_txn);

  /**
   * Notify all of the replicas that the current oldest active transaction (OAT) is at least as new as the supplied OAT.
   *
   * @param oldest_active_txn   The begin timestamp of the oldest active transaction.
   */
  void NotifyReplicasOfOAT(transaction::timestamp_t oldest_active_txn);

  /** @return The ID of the last transaction that was sent to the replicas. */
  transaction::timestamp_t GetLastSentTransactionId() const { return newest_txn_sent_; }

 protected:
  /** The main event loop that the primary runs. This handles receiving messages. */
  void EventLoop(common::ManagedPointer<messenger::Messenger> messenger, const messenger::ZmqMessage &zmq_msg,
                 common::ManagedPointer<BaseReplicationMessage> msg) override;

 private:
  /** Process every transaction callback where the associated transaction has been applied by all replicas. */
  void ProcessTxnCallbacks();

  /** @return The next batch ID that should be assigned in ReplicateBatchOfRecords(). */
  record_batch_id_t GetNextBatchId();

  void Handle(const messenger::ZmqMessage &zmq_msg, const TxnAppliedMsg &msg);

  /**
   * Queue of batches of commit callbacks. Each batch is tagged with whether there are corresponding commit records.
   * Each item in the queue is a separate invocation of ReplicateBatchOfRecords() being recorded.
   * Each commit callback should be executed in order of addition. The reason for the queue wrapper is so that
   * multiple calls to ReplicateBatchOfRecords() will not end up growing resizing a single vector repeatedly.
   * */
  std::queue<std::vector<storage::CommitCallback>> txn_callbacks_;
  /** Map from transaction start times (aka transaction ID) to list of replicas that have applied the transaction. */
  std::unordered_map<transaction::timestamp_t, std::unordered_set<std::string>> txns_applied_on_replicas_;
  std::mutex callbacks_mutex_;  ///< Protecting txn_callbacks_ and txns_applied_on_replicas_.

  /** ID of the next batch of log records to be sent out to all replicas. */
  record_batch_id_t next_batch_id_{1};
  /** ID of the last batch of log records that was sent out to all replicas. */
  record_batch_id_t last_sent_batch_id_ = INVALID_RECORD_BATCH_ID;
  /** ID of the newest transaction that was sent out to all replicas. */
  transaction::timestamp_t newest_txn_sent_ = transaction::INITIAL_TXN_TIMESTAMP;
};

}  // namespace noisepage::replication
