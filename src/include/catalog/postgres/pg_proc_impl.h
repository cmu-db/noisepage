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
 public:
  /**
   * Prepare to create pg_proc.
   * Does NOT create anything until the relevant bootstrap functions are called.
   *
   * @param db_oid          The OID of the database that pg_proc should be created in.
   */
  explicit PgProcImpl(db_oid_t db_oid);

  /** Bootstrap the projected row initializers for pg_proc. */
  void BootstrapPRIs();

  /**
   * Bootstrap:
   *    pg_proc
   *    pg_proc_oid_index
   *    pg_proc_name_index
   *
   * @param txn             The transaction to bootstrap in.
   * @param dbc             The catalog object to bootstrap in.
   */
  void Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn,
                 common::ManagedPointer<DatabaseCatalog> dbc);

  /**
   * Obtain a function that will teardown pg_proc as it exists at the time of the provided txn.
   *
   * @param txn             The transaction to perform the teardown in.
   * @return A function that will teardown pg_proc when invoked.
   */
  std::function<void(void)> GetTearDownFn(common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * Create a procedure for the pg_proc table.
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
   * @return True if the creation succeeded. False otherwise.
   *
   * @warning Does not support variadics yet.
   */
  bool CreateProcedure(common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t oid,
                       const std::string &procname, language_oid_t language_oid, namespace_oid_t procns,
                       const std::vector<std::string> &args, const std::vector<type_oid_t> &arg_types,
                       const std::vector<type_oid_t> &all_arg_types,
                       const std::vector<postgres::PgProc::ProArgModes> &arg_modes, type_oid_t rettype,
                       const std::string &src, bool is_aggregate);

  /**
   * Drop a procedure from the pg_proc table.
   *
   * @param txn             The transaction to use.
   * @param proc            The OID of the procedure to drop.
   * @return True if the process was successfully found and dropped. False otherwise.
   */
  bool DropProcedure(common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc);

  /**
   * Set the procedure context pointer column of the specified procedure.
   *
   * @return False if the proc_oid does not correspond to a valid procedure. True if the set was successful.
   */
  bool SetProcCtxPtr(common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid,
                     const execution::functions::FunctionContext *func_context);

  /**
   * Get the procedure context pointer column of the specified procedure.
   *
   * @return The procedure context pointer if it exists. nullptr if proc_oid is invalid or no such context exists.
   */
  common::ManagedPointer<execution::functions::FunctionContext> GetProcCtxPtr(
      common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid);

  /**
   * Get the OID of a procedure from pg_proc.
   *
   * @param dbc             The catalog that pg_proc is in. Used for type ID translation.
   * @param txn             The transaction to use.
   * @param procns          The namespace of the procedure to look in.
   * @param procname        The name of the procedure to look for.
   * @param arg_types       The types of all arguments in this function.
   * @return The OID of the procedure if found. Else INVALID_PROC_OID.
   */
  proc_oid_t GetProcOid(common::ManagedPointer<DatabaseCatalog> dbc,
                        common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t procns,
                        const std::string &procname, const std::vector<type_oid_t> &arg_types);

 private:
  friend class Builder;
  friend class storage::RecoveryManager;

  /** Bootstrap all the builtin procedures in pg_proc. */
  void BootstrapProcs(common::ManagedPointer<DatabaseCatalog> dbc,
                      common::ManagedPointer<transaction::TransactionContext> txn);

  /** Bootstrap all the procedure function contexts in pg_proc. */
  void BootstrapProcContexts(common::ManagedPointer<DatabaseCatalog> dbc,
                             common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * Allocate the FunctionContext and insert the pointer.
   *
   * @param txn                     The transaction to insert into the catalog with.
   * @param proc_oid                The OID of the procedure to to associate with this proc's and its context
   * @param func_name               The name of the function.
   * @param func_ret_type           The return type of the function.
   * @param args_type               The types of the arguments.
   * @param builtin                 The builtin this context refers to.
   * @param is_exec_ctx_required    True if this function requires an execution context variable as its first argument.
   */
  void BootstrapProcContext(common::ManagedPointer<DatabaseCatalog> dbc,
                            common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid,
                            std::string &&func_name, type::TypeId func_ret_type, std::vector<type::TypeId> &&args_type,
                            execution::ast::Builtin builtin, bool is_exec_ctx_required);

  const db_oid_t db_oid_;

  storage::SqlTable *procs_;
  storage::index::Index *procs_oid_index_;
  storage::index::Index *procs_name_index_;
  storage::ProjectedRowInitializer pg_proc_all_cols_pri_;
  storage::ProjectionMap pg_proc_all_cols_prm_;
  storage::ProjectedRowInitializer pg_proc_ptr_pri_;
};

}  // namespace noisepage::catalog::postgres
