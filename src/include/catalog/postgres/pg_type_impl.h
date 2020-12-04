#pragma once

#include <string>

#include "catalog/postgres/pg_type.h"
#include "common/managed_pointer.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace noisepage::storage {
class RecoveryManager;
class SqlTable;

namespace index {
class Index;
}  // namespace index
}  // namespace noisepage::storage

namespace noisepage::transaction {
class TransactionContext;
}  // namespace noisepage::transaction

namespace noisepage::catalog::postgres {
class Builder;

/** The NoisePage version of pg_type. */
class PgTypeImpl {
 public:
  /**
   * @brief Prepare to create pg_type.
   *
   * Does NOT create anything until the relevant bootstrap functions are called.
   *
   * @param db_oid          The OID of the database that pg_type should be created in.
   */
  explicit PgTypeImpl(db_oid_t db_oid);

  /** @brief Bootstrap the projected row initializers for pg_type. */
  void BootstrapPRIs();

  /**
   * @brief Create pg_type and associated indexes.
   *
   * Bootstrap:
   *    pg_type
   *    pg_type_oid_index
   *    pg_type_name_index
   *    pg_type_namespace_index
   *
   * Dependencies (for bootstrapping):
   *    pg_core must have been bootstrapped.
   * Dependencies (for execution):
   *    No other dependencies.
   *
   * @param txn             The transaction to bootstrap in.
   * @param dbc             The catalog object to bootstrap in.
   */
  void Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn,
                 common::ManagedPointer<DatabaseCatalog> dbc);

  /**
   * @brief Create a new type for the pg_type table.
   *
   * @param txn             The transaction to use.
   * @param type_oid        The OID to assign to the type.
   * @param name            The name of the type.
   * @param namespace_oid   The namespace that the type should be added to.
   * @param len             The length of type in bytes. len must be -1 for varlen types.
   * @param by_val          True if type should be passed by value. False if passed by reference.
   * @param type_category   The category of the type.
   */
  void InsertType(common::ManagedPointer<transaction::TransactionContext> txn, type_oid_t type_oid,
                  const std::string &name, namespace_oid_t namespace_oid, int16_t len, bool by_val,
                  PgType::Type type_category);

 private:
  friend class Builder;
  friend class storage::RecoveryManager;

  /** @brief Bootstrap all the builtin types in pg_type. */
  void BootstrapTypes(common::ManagedPointer<DatabaseCatalog> dbc,
                      common::ManagedPointer<transaction::TransactionContext> txn);

  const db_oid_t db_oid_;

  common::ManagedPointer<storage::SqlTable> types_;
  common::ManagedPointer<storage::index::Index> types_oid_index_;
  common::ManagedPointer<storage::index::Index> types_name_index_;  // indexed on namespace OID and name
  common::ManagedPointer<storage::index::Index> types_namespace_index_;
  storage::ProjectedRowInitializer pg_type_all_cols_pri_;
  storage::ProjectionMap pg_type_all_cols_prm_;
};

}  // namespace noisepage::catalog::postgres
