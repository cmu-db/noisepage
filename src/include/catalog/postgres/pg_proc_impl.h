#pragma once

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
class SqlTable;

namespace index {
class Index;
}  // namespace index
}  // namespace noisepage::storage

namespace noisepage::transaction {
class TransactionContext;
}  // namespace noisepage::transaction

namespace noisepage::catalog::postgres {

/** The NoisePage version of pg_proc. */
class PgProcImpl {
 public:
  PgProcImpl(db_oid_t db_oid);

  void BootstrapPRIs();
  void Bootstrap(common::ManagedPointer<DatabaseCatalog> dbc,
                 common::ManagedPointer<transaction::TransactionContext> txn);

  std::vector<execution::functions::FunctionContext *> TearDownGetFuncContexts(
      common::ManagedPointer<transaction::TransactionContext> txn, byte *buffer, uint64_t buffer_len);

  bool CreateProcedure(const common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t oid,
                       const std::string &procname, language_oid_t language_oid, namespace_oid_t procns,
                       const std::vector<std::string> &args, const std::vector<type_oid_t> &arg_types,
                       const std::vector<type_oid_t> &all_arg_types,
                       const std::vector<postgres::PgProc::ProArgModes> &arg_modes, type_oid_t rettype,
                       const std::string &src, bool is_aggregate);

  bool DropProcedure(const common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc);

  bool SetProcCtxPtr(common::ManagedPointer<transaction::TransactionContext> txn, const proc_oid_t proc_oid,
                     const execution::functions::FunctionContext *func_context);

  common::ManagedPointer<execution::functions::FunctionContext> GetProcCtxPtr(
      common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid);

  proc_oid_t GetProcOid(common::ManagedPointer<DatabaseCatalog> dbc,
                        common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t procns,
                        const std::string &procname, const std::vector<type_oid_t> &arg_types);

 private:
  /**
   * Bootstraps the built-in procs found in pg_proc
   * @param txn transaction to insert into catalog with
   */
  void BootstrapProcs(common::ManagedPointer<DatabaseCatalog> dbc,
                      common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * Internal helper method to reduce copy-paste code for populating proc contexts. Allocates the FunctionContext and
   * inserts the pointer.
   * @param txn transaction to insert into catalog with
   * @param proc_oid oid to associate with this proc's and its context
   * @param func_name Name of function
   * @param func_ret_type Return type of function
   * @param args_type Vector of argument types
   * @param builtin Which builtin this context refers to
   * @param is_exec_ctx_required true if this function requires an execution context var as its first argument
   */
  void BootstrapProcContext(const common::ManagedPointer<transaction::TransactionContext> txn,
                            const proc_oid_t proc_oid, std::string &&func_name, const type::TypeId func_ret_type,
                            std::vector<type::TypeId> &&args_type, const execution::ast::Builtin builtin,
                            const bool is_exec_ctx_required);
  /**
   * Bootstraps the proc functions contexts in pg_proc
   * @param txn transaction to insert into catalog with
   */
  void BootstrapProcContexts(const common::ManagedPointer<transaction::TransactionContext> txn);

 public:
  const db_oid_t db_oid_;

  storage::SqlTable *procs_;
  storage::index::Index *procs_oid_index_;
  storage::index::Index *procs_name_index_;
  storage::ProjectedRowInitializer pg_proc_all_cols_pri_;
  storage::ProjectionMap pg_proc_all_cols_prm_;
  storage::ProjectedRowInitializer pg_proc_ptr_pri_;
};

}  // namespace noisepage::catalog::postgres
