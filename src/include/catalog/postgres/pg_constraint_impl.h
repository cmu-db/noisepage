#pragma once

#include <vector>

#include "common/managed_pointer.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace noisepage::parser {
class AbstractExpression;
}  // namespace noisepage::parser

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

/** The NoisePage version of pg_constraint. */
class PgConstraintImpl {
 public:
  /**
   * @brief Prepare to create pg_constraint.
   *
   * Does NOT create anything until the relevant bootstrap functions are called.
   *
   * @param db_oid  The OID of the database that pg_constraint should be created in.
   */
  explicit PgConstraintImpl(db_oid_t db_oid);

  /** Bootstrap the projected row initializers for pg_constraint. */
  void BootstrapPRIs();

  /**
   * @brief Create pg_constraint and associated indexes.
   *
   * Bootstrap:
   *    pg_constraint
   *    pg_constraint_oid_index
   *    pg_constraint_name_index
   *    pg_constraint_namespace_index
   *    pg_constraint_table_index
   *    pg_constraint_index_index
   *    pg_constraint_foreigntable_index
   *
   * Dependencies (for bootstrapping):
   *    pg_core must have been bootstrapped.
   * Dependencies (for execution):
   *    Doesn't do anything right now, so nothing.
   *
   * @param txn     The transaction to bootstrap in.
   * @param dbc     The catalog object to bootstrap in.
   */
  void Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn,
                 common::ManagedPointer<DatabaseCatalog> dbc);

  /**
   * @brief Obtain a function that will teardown pg_constraint as it exists at the time of the provided txn.
   *
   * @param txn     The transaction to perform the teardown in.
   * @return        A function that will teardown pg_constraint when invoked.
   */
  std::function<void(void)> GetTearDownFn(common::ManagedPointer<transaction::TransactionContext> txn);

 private:
  friend class Builder;
  friend class storage::RecoveryManager;

  const db_oid_t db_oid_;

  /**
   * The table and indexes that define pg_constraint.
   * Created by: Builder::CreateDatabaseCatalog.
   * Cleaned up by: DatabaseCatalog::TearDown, where the scans from pg_class and pg_index pick these up.
   */
  ///@{
  common::ManagedPointer<storage::SqlTable> constraints_;                         ///< The constraints table.
  common::ManagedPointer<storage::index::Index> constraints_oid_index_;           ///< Indexed on: conoid
  common::ManagedPointer<storage::index::Index> constraints_name_index_;          ///< Indexed on: connamespace, conname
  common::ManagedPointer<storage::index::Index> constraints_namespace_index_;     ///< Indexed on: connamespace
  common::ManagedPointer<storage::index::Index> constraints_table_index_;         ///< Indexed on: conrelid
  common::ManagedPointer<storage::index::Index> constraints_index_index_;         ///< Indexed on: conindid
  common::ManagedPointer<storage::index::Index> constraints_foreigntable_index_;  ///< Indexed on: confrelid
  ///@}
};

}  // namespace noisepage::catalog::postgres
