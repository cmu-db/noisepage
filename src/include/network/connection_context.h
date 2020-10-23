#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "catalog/catalog_accessor.h"
#include "catalog/catalog_cache.h"
#include "catalog/catalog_defs.h"
#include "network/network_defs.h"
#include "transaction/transaction_context.h"

namespace noisepage::network {

/**
 * A ConnectionContext stores the state of a connection. There should be as little as possible that is protocol-specific
 * in this layer, and if you find yourself wanting to put more the design should be discussed.
 *
 * This class is designed to share information between several layers: ConnectionHandle, ProtocolInterpreter,
 * NetworkCommands, and TrafficCop.
 */
class ConnectionContext {
 public:
  /**
   * Cleans up this ConnectionContext.
   * This is called when its connection handle is reused to occupy another connection or destroyed.
   */
  void Reset() {
    connection_id_ = static_cast<connection_id_t>(0);
    cmdline_args_.clear();
    db_oid_ = catalog::INVALID_DATABASE_OID;
    db_name_.clear();
    temp_namespace_oid_ = catalog::INVALID_NAMESPACE_OID;
    txn_ = nullptr;
    accessor_ = nullptr;
    callback_ = nullptr;
    callback_arg_ = nullptr;
    catalog_cache_.Reset(transaction::INITIAL_TXN_TIMESTAMP);
  }

  /**
   * @return unique identifier (among currently open connections, not over the lifetime of the system) for this
   * connection
   */
  connection_id_t GetConnectionID() const { return connection_id_; }

  /**
   * @return database name. It should be in cmdline_args_ as well, this just makes it quicker to find
   */
  const std::string &GetDatabaseName() const { return db_name_; }

  /**
   * @return database OID connected to, or INVALID_DATABASE_OID in the event of failure
   */
  catalog::db_oid_t GetDatabaseOid() const { return db_oid_; }

  /**
   * @return temporary namespace OID that was generated at connection, or INVALID_NAMESPACE_OID in the event of failure
   */
  catalog::namespace_oid_t GetTempNamespaceOid() const { return temp_namespace_oid_; }

  /**
   * @param db_oid database OID that the connection is using
   * @warning only to be used by the protocol interpreter during startup
   */
  void SetDatabaseOid(const catalog::db_oid_t db_oid) { db_oid_ = db_oid; }

  /**
   * @param ns_oid temporary namespace OID that was generated at connection
   * @warning only to be used by the protocol interpreter during startup
   */
  void SetTempNamespaceOid(const catalog::namespace_oid_t ns_oid) { temp_namespace_oid_ = ns_oid; }

  /**
   * @param db_name database name. It should be in cmdline_args_ as well, this just makes it quicker to find
   * @warning only to be used by the protocol interpreter during startup
   */
  void SetDatabaseName(std::string &&db_name) { db_name_ = std::move(db_name); }

  /**
   * @param connection_id
   * @warning only to be used by the ConnectionHandle during its constructor.
   */
  void SetConnectionID(const connection_id_t connection_id) { connection_id_ = connection_id; }

  /**
   * @return const reference to cmdline_args_ for reading values back out
   */
  const std::unordered_map<std::string, std::string> &CommandLineArgs() const { return cmdline_args_; }

  /**
   * @return mutable reference to cmdline_args_. For PostgresProtocolInterpreter during setup
   */
  std::unordered_map<std::string, std::string> &CommandLineArgs() { return cmdline_args_; }

  /**
   * Helper method to determine a connection's transaction state without needing to know the type or API of
   * TransactionContext
   * @return transaction state
   * @see NetworkTransactionStateType definition for state definitions
   */
  NetworkTransactionStateType TransactionState() const {
    if (txn_ != nullptr) {
      if (txn_->MustAbort()) {
        return NetworkTransactionStateType::FAIL;
      }
      return NetworkTransactionStateType::BLOCK;
    }
    return NetworkTransactionStateType::IDLE;
  }

  /**
   * @return managed pointer to the connection's current txn (may be nullptr if none running)
   */
  common::ManagedPointer<transaction::TransactionContext> Transaction() const { return txn_; }

  /**
   * @param txn new value
   * @warning this should only be used by TrafficCop::BeginTransaction and TrafficCop::EndTransaction
   * @warning it should match the txn used for this ConnectionContext's current CatalogAccessor
   */
  void SetTransaction(const common::ManagedPointer<transaction::TransactionContext> txn) { txn_ = txn; }

  /**
   * @return current CatalogAccesor for connection
   */
  common::ManagedPointer<catalog::CatalogAccessor> Accessor() const {
    NOISEPAGE_ASSERT(accessor_ != nullptr, "Requesting CatalogAccessor that doesn't exist yet.");
    // TODO(Matt): I'd like an assert here that the accessor's txn matches the connection context's txn, but the
    // accessor doesn't expose a getter.
    return common::ManagedPointer(accessor_);
  }

  /**
   * @param accessor new value
   * @warning this should only be used by TrafficCop::BeginTransaction and TrafficCop::EndTransaction
   * @warning it should match the txn used for this ConnectionContext's current txn
   */
  void SetAccessor(std::unique_ptr<catalog::CatalogAccessor> accessor) { accessor_ = std::move(accessor); }

  /**
   * @param callback static method for callback in ConnectionHandle
   * @param callback_arg this from ConnectionHandle constructor
   * @warning only to be used by ConnectionHandle's constructor
   */
  void SetCallback(const network::NetworkCallback callback, void *const callback_arg) {
    callback_ = callback;
    callback_arg_ = callback_arg;
  }

  /**
   * @return handle to the ConnectionHandle callback to issue a libevent wakeup in the event of WAIT_ON_NOISEPAGE
   * state. Currently not used, but may in the future for asynchronous execution.
   */
  network::NetworkCallback Callback() const { return callback_; }

  /**
   * @return args to the ConnectionHandle callback to issue a libevent wakeup in the event of WAIT_ON_NOISEPAGE
   * state. Currently not used, but may in the future for asynchronous execution.
   */
  void *CallbackArg() const { return callback_arg_; }

  /**
   * @return CatalogCache to be injected into requests for CatalogAcessors
   */
  common::ManagedPointer<catalog::CatalogCache> GetCatalogCache() { return common::ManagedPointer(&catalog_cache_); }

 private:
  /**
   * This is a unique identifier (among currently open connections, not over the lifetime of the system) for this
   * connection.
   */
  connection_id_t connection_id_;

  /**
   * Commandline arguments parsed from protocol interpreter
   */
  std::unordered_map<std::string, std::string> cmdline_args_;

  /**
   * The OID of the database accessed by this connection. Only mutable by the Setter via ConnectionHandle, or Reset
   */
  catalog::db_oid_t db_oid_ = catalog::INVALID_DATABASE_OID;

  /**
   * The binder needs database name as an argument for some reason even though we've already bound the oid in
   * ConnectionHandle::StartUp. I suspect this can be removed in the future.
   */
  std::string db_name_;

  /**
   * The OID of the temporary namespace for this connection. Only mutable by the Setter via ConnectionHandle, or Reset
   */
  catalog::namespace_oid_t temp_namespace_oid_ = catalog::INVALID_NAMESPACE_OID;

  /**
   * In theory the ConnectionContext owns this too, but for legacy reasons (and safety about who can delete them) we
   * don't use unique_ptrs for txns. If that ever changes, then the ConnectionContext should probably own it and
   * transfer ownership to the TransactionManager (via the TrafficCop) at commit or abort.
   */
  common::ManagedPointer<transaction::TransactionContext> txn_ = nullptr;

  /**
   * The ConnectionContext owns this, and I don't expect that should ever change. It's life cycle dominates objects
   * later in the pipeline so this is a reasonable owner and will just hand out ManagedPointers for other components.
   */
  std::unique_ptr<catalog::CatalogAccessor> accessor_ = nullptr;

  /**
   * ConnectionHandle callback stuff to issue a libevent wakeup in the event of WAIT_ON_NOISEPAGE state. Currently
   * not used, but may in the future for asynchronous execution.
   */
  network::NetworkCallback callback_;
  void *callback_arg_;

  catalog::CatalogCache catalog_cache_;
};

}  // namespace noisepage::network
