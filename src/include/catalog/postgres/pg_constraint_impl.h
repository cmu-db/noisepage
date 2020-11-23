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
   * Prepare to create pg_constraint.
   * Does NOT create anything until the relevant bootstrap functions are called.
   *
   * @param db_oid          The OID of the database that pg_constraint should be created in.
   */
  explicit PgConstraintImpl(db_oid_t db_oid);

  /** Bootstrap the projected row initializers for pg_constraint. */
  void BootstrapPRIs();

  /**
   * Bootstrap:
   *    pg_constraint
   *    pg_constraint_oid_index
   *    pg_constraint_name_index
   *    pg_constraint_namespace_index
   *    pg_constraint_table_index
   *    pg_constraint_index_index
   *    pg_constraint_foreigntable_index
   *
   * @param dbc             The catalog object to bootstrap in.
   * @param txn             The transaction to bootstrap in.
   */
  void Bootstrap(common::ManagedPointer<DatabaseCatalog> dbc,
                 common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * Get all of the expressions used for constraints, used in TearDown.
   * A buffer is passed in to allow for buffer reuse optimizations.
   *
   * @param txn             The transaction to obtain all the expressions in.
   * @param buffer          The buffer to use for getting function contexts back out.
   * @param buffer_len      The length of buffer. Only used in debug mode to assert correctness.
   * @return All the expressions used for constraints.
   */
  std::vector<parser::AbstractExpression *> TearDownGetExpressions(
      common::ManagedPointer<transaction::TransactionContext> txn, byte *buffer, UNUSED_ATTRIBUTE uint64_t buffer_len);

 private:
  friend class Builder;
  friend class storage::RecoveryManager;

  const db_oid_t db_oid_;

  storage::SqlTable *constraints_;
  storage::index::Index *constraints_oid_index_;
  storage::index::Index *constraints_name_index_;  // indexed on namespace OID and name
  storage::index::Index *constraints_namespace_index_;
  storage::index::Index *constraints_table_index_;
  storage::index::Index *constraints_index_index_;
  storage::index::Index *constraints_foreigntable_index_;
};

}  // namespace noisepage::catalog::postgres
