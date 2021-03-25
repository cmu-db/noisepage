#pragma once

#include <queue>
#include <unordered_set>

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
   * Send a batch of records to all of the replicas as a REPLICATE_BUFFER_BATCH message.
   *
   * @param records_batch       The batch of records to be replicated.
   * @param commit_callbacks    The commit callbacks associated with the batch of records.
   */
  void ReplicateBatchOfRecords(storage::BufferedLogWriter *records_batch,
                               const std::vector<storage::CommitCallback> &commit_callbacks);

  /** @return The ID of the last transaction that was sent to the replicas. */
  transaction::timestamp_t GetLastSentTransactionId() const { return transaction::timestamp_t{0}; }

 protected:
  /** The main event loop that the primary runs. This handles receiving messages. */
  void EventLoop(common::ManagedPointer<messenger::Messenger> messenger, const messenger::ZmqMessage &zmq_msg,
                 common::ManagedPointer<BaseReplicationMessage> msg) override;

 private:
  record_batch_id_t GetNextBatchId();

  void Handle(const messenger::ZmqMessage &zmq_msg, const TxnAppliedMsg &msg);

  /**
   * Process every transaction callback where the associated transaction has been applied by all replicas.
   * TODO(WAN): If implementing async, do NOT reuse this function in ReplicateBatchOfRecords() unless you add
   * mutex+cvar.
   */
  void ProcessTxnCallbacks();

  /**
   * Vector of vector of commit callbacks.
   * Each vector is a separate invocation of ReplicateBatchOfRecords()'s commit callbacks.
   * Each commit callback should be executed in order of addition, the reason for the double vector is so that
   * multiple calls to ReplicateBatchOfRecords() will not end up growing resizing a single vector repeatedly.
   * */
  std::queue<std::vector<storage::CommitCallback>> txn_callbacks_;
  /** Map from transaction start times (aka transaction ID) to list of replicas that have applied the transaction. */
  std::unordered_map<transaction::timestamp_t, std::unordered_set<std::string>> txns_applied_on_replicas_;
  /** ID of the next batch of log records to be sent out to replicas. */
  std::atomic<record_batch_id_t> next_batch_id_{1};
};

}  // namespace noisepage::replication
