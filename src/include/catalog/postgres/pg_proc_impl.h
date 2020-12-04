#pragma once

#include <string>
#include <vector>

#include "catalog/postgres/pg_proc.h"
#include "common/managed_pointer.h"
#include "execution/ast/builtins.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace noisepage::catalog {
class DatabaseCatalog;
}  // namespace noisepage::catalog

namespace noisepage::execution::functions {
class FunctionContext;
}  // namespace noisepage::execution::functions

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

/** The NoisePage version of pg_proc. */
class PgProcImpl {
 private:
  friend class Builder;                   ///< The builder is used to construct pg_proc. TODO(WAN) refactor nuke builder
  friend class storage::RecoveryManager;  ///< The RM accesses tables and indexes without going through the catalog.
  friend class catalog::DatabaseCatalog;  ///< DatabaseCatalog sets up and owns pg_proc.

  /**
   * @brief Prepare to create pg_proc.
   *
   * Does NOT create anything until the relevant bootstrap functions are called.
   *
   * @param db_oid      The OID of the database that pg_proc should be created in.
   */
  explicit PgProcImpl(db_oid_t db_oid);

  /** @brief Bootstrap the projected row initializers for pg_proc. */
  void BootstrapPRIs();

  /**
   * @brief Create pg_proc and associated indexes.
   *
   * Bootstrap:
   *    pg_proc
   *    pg_proc_oid_index
   *    pg_proc_name_index
   *
   * Dependencies (for bootstrapping):
   *    pg_core must have been bootstrapped.
   * Dependencies (for execution):
   *    pg_language must have been bootstrapped.
   *
   * @param txn         The transaction to bootstrap in.
   * @param dbc         The catalog object to bootstrap in.
   */
  void Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn,
                 common::ManagedPointer<DatabaseCatalog> dbc);

  /**
   * @brief Obtain a function that will teardown pg_proc as it exists at the time of the provided txn.
   *
   * @param txn             The transaction to perform the teardown in.
   * @return A function that will teardown pg_proc when invoked.
   */
  std::function<void(void)> GetTearDownFn(common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * @brief Create a procedure in the pg_proc table.
   *
   * @param txn             The transaction to use.
   * @param oid             The OID to assign to the procedure.
   * @param procname        The name of the procedure.
   * @param language_oid    The OID for the language that this procedure is written in.
   * @param procns          The namespace that the procedure should be added to.
   * @param args            The names of the arguments to this procedure.
   * @param arg_types       The types of the arguments to this procedure. Must be in the same order as in args.
   *                        (only for in and inout arguments)
   * @param all_arg_types   The types of all the arguments.
   * @param arg_modes       The modes of the arguments. Must be in the same order as in args.
   * @param rettype         The OID of the type of return value.
   * @param src             The source code of the procedure.
   * @param is_aggregate    True iff this is an aggregate procedure.
   * @return                True if the creation succeeded. False otherwise.
   *
   * TODO(WAN): This should be refactored to have a cleaner signature. See #1354.
   */
  bool CreateProcedure(common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t oid,
                       const std::string &procname, language_oid_t language_oid, namespace_oid_t procns,
                       const std::vector<std::string> &args, const std::vector<type_oid_t> &arg_types,
                       const std::vector<type_oid_t> &all_arg_types,
                       const std::vector<postgres::PgProc::ArgModes> &arg_modes, type_oid_t rettype,
                       const std::string &src, bool is_aggregate);

  /**
   * @brief Drop a procedure from the pg_proc table.
   *
   * @param txn             The transaction to use.
   * @param proc            The OID of the procedure to drop.
   * @return                True if the procedure was successfully found and dropped. False otherwise.
   */
  bool DropProcedure(common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc);

  /**
   * @brief Set the procedure context pointer column of the specified procedure.
   *
   * @param txn             The transaction to use.
   * @param proc            The OID of the procedure to set the context for.
   * @param func_context    The procedure context to set to.
   * @return                False if the proc_oid does not map to a valid procedure. True if the set was successful.
   *
   * TODO(WAN): See #1356 for a discussion on whether this should be exposed separately.
   */
  bool SetProcCtxPtr(common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid,
                     const execution::functions::FunctionContext *func_context);

  /**
   * @brief Get the procedure context pointer column of the specified procedure.
   *
   * @param txn             The transaction to use.
   * @param proc_oid        The OID of the procedure whose procedure context is to be obtained.
   * @return                The procedure context pointer of the specified procedure, guaranteed to not be nullptr.
   */
  common::ManagedPointer<execution::functions::FunctionContext> GetProcCtxPtr(
      common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid);

  /**
   * @brief Get the OID of a procedure from pg_proc.
   *
   * @param txn             The transaction to use.
   * @param dbc             The catalog that pg_proc is in.
   * @param procns          The namespace of the procedure to look in.
   * @param procname        The name of the procedure to look for.
   * @param arg_types       The types of all arguments in this function.
   * @return                The OID of the procedure if found. Else INVALID_PROC_OID.
   */
  proc_oid_t GetProcOid(common::ManagedPointer<transaction::TransactionContext> txn,
                        common::ManagedPointer<DatabaseCatalog> dbc, namespace_oid_t procns,
                        const std::string &procname, const std::vector<type_oid_t> &arg_types);

  /** @brief Bootstrap all the builtin procedures in pg_proc. This assumes exclusive use of dbc->next_oid_. */
  void BootstrapProcs(common::ManagedPointer<transaction::TransactionContext> txn,
                      common::ManagedPointer<DatabaseCatalog> dbc);

  /** @brief Bootstrap all the procedure function contexts in pg_proc. */
  void BootstrapProcContexts(common::ManagedPointer<transaction::TransactionContext> txn,
                             common::ManagedPointer<DatabaseCatalog> dbc);

  /**
   * @brief Allocate the FunctionContext and insert the pointer.
   *
   * @param txn                     The transaction to insert into the catalog with.
   * @param dbc                     The catalog that pg_proc is in.
   * @param func_name               The name of the function.
   * @param func_ret_type           The return type of the function.
   * @param arg_types               The types of the arguments.
   * @param builtin                 The builtin this context refers to.
   * @param is_exec_ctx_required    True if this function requires an execution context variable as its first argument.
   */
  void BootstrapProcContext(common::ManagedPointer<transaction::TransactionContext> txn,
                            common::ManagedPointer<DatabaseCatalog> dbc, std::string &&func_name,
                            type::TypeId func_ret_type, std::vector<type::TypeId> &&arg_types,
                            execution::ast::Builtin builtin, bool is_exec_ctx_required);

  const db_oid_t db_oid_;

  /**
   * The table and indexes that define pg_proc.
   * Created by: Builder::CreateDatabaseCatalog.
   * Cleaned up by: DatabaseCatalog::TearDown, where the scans from pg_class and pg_index pick these up.
   */
  ///@{
  common::ManagedPointer<storage::SqlTable> procs_;
  common::ManagedPointer<storage::index::Index> procs_oid_index_;
  common::ManagedPointer<storage::index::Index> procs_name_index_;
  ///@}

  storage::ProjectedRowInitializer pg_proc_all_cols_pri_;
  storage::ProjectionMap pg_proc_all_cols_prm_;
  storage::ProjectedRowInitializer pg_proc_ptr_pri_;
};

}  // namespace noisepage::catalog::postgres
