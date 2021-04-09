#pragma once

#include <string>

#include "replication/replication_manager.h"
#include "storage/recovery/replication_log_provider.h"
#include "transaction/transaction_defs.h"

namespace noisepage::replication {

/**
 * The replication manager that should run on the replicas.
 *
 * TODO(WAN): If the primary fails, how does the replica switch its replication manager? I guess possible, but annoying.
 */
class ReplicaReplicationManager final : public ReplicationManager {
 public:
  /**
   * On construction, the replication manager establishes a listen destination on the messenger and connects to all the
   * replicas specified in the replication.config file located at @p replication_hosts_path.
   *
   * @param messenger                   The messenger instance to use.
   * @param network_identity            The identity of this node in the network.
   * @param port                        The port to listen on.
   * @param replication_hosts_path      The path to the replication.config file.
   * @param empty_buffer_queue          A queue of empty buffers that the replication manager may return buffers to.
   */
  ReplicaReplicationManager(
      common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
      const std::string &replication_hosts_path,
      common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue);

  /** Destructor. */
  ~ReplicaReplicationManager() final;

  /** @return False since this is a replica. */
  bool IsPrimary() const override { return false; }

  /** @return The replication log provider that (ordered) replication logs are pushed to. */
  common::ManagedPointer<storage::ReplicationLogProvider> GetReplicationLogProvider() {
    return common::ManagedPointer(&provider_);
  }

  /**
   * Notify the primary that the given transaction has been applied.
   *
   * @param txn_start_time              The start time (aka ID) of the transaction that was applied.
   */
  void NotifyPrimaryTransactionApplied(transaction::timestamp_t txn_start_time);

 protected:
  /** The main event loop that all replicas run. This handles receiving messages. */
  void EventLoop(common::ManagedPointer<messenger::Messenger> messenger, const messenger::ZmqMessage &zmq_msg,
                 common::ManagedPointer<BaseReplicationMessage> msg) override;

 private:
  void Handle(const messenger::ZmqMessage &zmq_msg, const NotifyOATMsg &msg);
  void Handle(const messenger::ZmqMessage &zmq_msg, const RecordsBatchMsg &msg);

  storage::ReplicationLogProvider provider_;  ///< The log records being provided to recovery.
};

}  // namespace noisepage::replication
