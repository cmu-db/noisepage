#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "network/network_defs.h"

namespace terrier::transaction {
class TransactionContext;
}

namespace terrier::catalog {
class CatalogAccessor;
}

namespace terrier::network {

/**
 * A ConnectionContext stores the state of a connection.
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
  }

  catalog::db_oid_t GetDatabaseOid() const { return db_oid_; }
  catalog::namespace_oid_t GetTempNamespaceOid() const { return temp_namespace_oid_; }
  void SetDatabaseOid(const catalog::db_oid_t db_oid) { db_oid_ = db_oid; }
  void SetTempNamespaceOid(const catalog::namespace_oid_t ns_oid) { temp_namespace_oid_ = ns_oid; }
  connection_id_t GetConnectionID() const { return connection_id_; }

  /**
   * To be used by the ConnectionHandle during its constructor.
   * @param connection_id
   */
  void SetConnectionID(const connection_id_t connection_id) { connection_id_ = connection_id; }

  const std::string &GetDatabaseName() const { return db_name_; }
  void SetDatabaseName(std::string &&db_name) { db_name_ = std::move(db_name); }

  /**
   * @return const reference to cmdline_args_ for reading values back out
   */
  const std::unordered_map<std::string, std::string> &CommandLineArgs() const { return cmdline_args_; }

  /**
   * @return mutable reference to cmdline_args_. For PostgresProtocolInterpreter during setup
   */
  std::unordered_map<std::string, std::string> &CommandLineArgs() { return cmdline_args_; }

  NetworkTransactionStateType TransactionState() const {
    if (txn_ != nullptr) {
      if (txn_->MustAbort()) {
        return NetworkTransactionStateType::FAIL;
      }
      return NetworkTransactionStateType::BLOCK;
    }
    return NetworkTransactionStateType::IDLE;
  }

  common::ManagedPointer<transaction::TransactionContext> Transaction() const { return txn_; }

  void SetTransaction(const common::ManagedPointer<transaction::TransactionContext> txn) { txn_ = txn; }

  common::ManagedPointer<catalog::CatalogAccessor> Accessor() const {
    TERRIER_ASSERT(accessor_ != nullptr, "Requesting CatalogAccessor that doesn't exist yet.");

    // TODO(Matt): I'd like an assert here that the accessor's txn matches the connection context's txn, but the
    // accessor doesn't expose a getter
    return common::ManagedPointer(accessor_);
  }

  void SetAccessor(std::unique_ptr<catalog::CatalogAccessor> accessor) { accessor_ = std::move(accessor); }

  void SetCallback(const network::NetworkCallback callback, void *const callback_arg) {
    callback_ = callback;
    callback_arg_ = callback_arg;
  }

  network::NetworkCallback Callback() const { return callback_; }
  void *CallbackArg() const { return callback_arg_; }

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

  common::ManagedPointer<transaction::TransactionContext> txn_ = nullptr;

  std::unique_ptr<catalog::CatalogAccessor> accessor_ = nullptr;

  network::NetworkCallback callback_;
  void *callback_arg_;
};

}  // namespace terrier::network
