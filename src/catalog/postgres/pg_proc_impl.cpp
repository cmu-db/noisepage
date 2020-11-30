#include "catalog/postgres/pg_proc_impl.h"

#include <vector>

#include "catalog/database_catalog.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_proc.h"
#include "catalog/schema.h"
#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "execution/functions/function_context.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "transaction/deferred_action_manager.h"

namespace noisepage::catalog::postgres {

PgProcImpl::PgProcImpl(db_oid_t db_oid) : db_oid_(db_oid) {}

void PgProcImpl::BootstrapPRIs() {
  const std::vector<col_oid_t> pg_proc_all_oids{PgProc::PG_PRO_ALL_COL_OIDS.cbegin(),
                                                PgProc::PG_PRO_ALL_COL_OIDS.cend()};
  pg_proc_all_cols_pri_ = procs_->InitializerForProjectedRow(pg_proc_all_oids);
  pg_proc_all_cols_prm_ = procs_->ProjectionMapForOids(pg_proc_all_oids);

  const std::vector<col_oid_t> set_pg_proc_ptr_oids{PgProc::PRO_CTX_PTR_COL_OID};
  pg_proc_ptr_pri_ = procs_->InitializerForProjectedRow(set_pg_proc_ptr_oids);
}

void PgProcImpl::Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn,
                           common::ManagedPointer<DatabaseCatalog> dbc) {
  dbc->BootstrapTable(txn, PgProc::PRO_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, "pg_proc",
                      Builder::GetProcTableSchema(), procs_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgProc::PRO_TABLE_OID,
                      PgProc::PRO_OID_INDEX_OID, "pg_proc_oid_index", Builder::GetProcOidIndexSchema(db_oid_),
                      procs_oid_index_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgProc::PRO_TABLE_OID,
                      PgProc::PRO_NAME_INDEX_OID, "pg_proc_name_index", Builder::GetProcNameIndexSchema(db_oid_),
                      procs_name_index_);

  BootstrapProcs(txn, dbc);
}

std::function<void(void)> PgProcImpl::GetTearDownFn(common::ManagedPointer<transaction::TransactionContext> txn) {
  std::vector<execution::functions::FunctionContext *> func_contexts;

  const std::vector<col_oid_t> pg_proc_contexts{PgProc::PRO_CTX_PTR_COL_OID};
  const storage::ProjectedColumnsInitializer pci =
      procs_->InitializerForProjectedColumns(pg_proc_contexts, DatabaseCatalog::TEARDOWN_MAX_TUPLES);
  byte *buffer = common::AllocationUtil::AllocateAligned(pci.ProjectedColumnsSize());
  auto pc = pci.Initialize(buffer);

  auto ctxts = reinterpret_cast<execution::functions::FunctionContext **>(pc->ColumnStart(0));

  auto table_iter = procs_->begin();
  while (table_iter != procs_->end()) {
    procs_->Scan(txn, &table_iter, pc);

    for (uint i = 0; i < pc->NumTuples(); i++) {
      if (ctxts[i] == nullptr) {
        continue;
      }
      func_contexts.emplace_back(ctxts[i]);
    }
  }

  delete[] buffer;
  return [func_contexts{std::move(func_contexts)}]() {
    for (auto ctx : func_contexts) {
      delete ctx;
    }
  };
}

bool PgProcImpl::CreateProcedure(const common::ManagedPointer<transaction::TransactionContext> txn,
                                 const proc_oid_t oid, const std::string &procname, const language_oid_t language_oid,
                                 const namespace_oid_t procns, const std::vector<std::string> &args,
                                 const std::vector<type_oid_t> &arg_types, const std::vector<type_oid_t> &all_arg_types,
                                 const std::vector<PgProc::ArgModes> &arg_modes, const type_oid_t rettype,
                                 const std::string &src, const bool is_aggregate) {
  NOISEPAGE_ASSERT(args.size() < UINT16_MAX, "Number of arguments must fit in a SMALLINT");
  const auto name_varlen = storage::StorageUtil::CreateVarlen(procname);

  std::vector<std::string> arg_name_vec;
  arg_name_vec.reserve(args.size() * sizeof(storage::VarlenEntry));

  for (auto &arg : args) {
    arg_name_vec.push_back(arg);
  }

  const auto arg_names_varlen = storage::StorageUtil::CreateVarlen(arg_name_vec);
  const auto arg_types_varlen = storage::StorageUtil::CreateVarlen(arg_types);
  const auto all_arg_types_varlen = storage::StorageUtil::CreateVarlen(all_arg_types);
  const auto arg_modes_varlen = storage::StorageUtil::CreateVarlen(arg_modes);
  const auto src_varlen = storage::StorageUtil::CreateVarlen(src);

  auto *const redo = txn->StageWrite(db_oid_, PgProc::PRO_TABLE_OID, pg_proc_all_cols_pri_);
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PRONAME_COL_OID]))) = name_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROARGNAMES_COL_OID]))) = arg_names_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROARGTYPES_COL_OID]))) = arg_types_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROALLARGTYPES_COL_OID]))) = all_arg_types_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROARGMODES_COL_OID]))) = arg_modes_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROSRC_COL_OID]))) = src_varlen;

  *(reinterpret_cast<proc_oid_t *>(redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROOID_COL_OID]))) =
      oid;
  *(reinterpret_cast<language_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROLANG_COL_OID]))) = language_oid;
  *(reinterpret_cast<namespace_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PRONAMESPACE_COL_OID]))) = procns;
  *(reinterpret_cast<type_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PRORETTYPE_COL_OID]))) = rettype;

  *(reinterpret_cast<uint16_t *>(redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PRONARGS_COL_OID]))) =
      static_cast<uint16_t>(args.size());

  // setting zero default args
  *(reinterpret_cast<uint16_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PRONARGDEFAULTS_COL_OID]))) = 0;
  redo->Delta()->SetNull(pg_proc_all_cols_prm_[PgProc::PROARGDEFAULTS_COL_OID]);

  *reinterpret_cast<bool *>(redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROISAGG_COL_OID])) =
      is_aggregate;

  // setting defaults of unexposed attributes
  // proiswindow, proisstrict, provolatile, provariadic, prorows, procost, proconfig

  // postgres documentation says this should be 0 if no variadics are there
  *(reinterpret_cast<type_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROVARIADIC_COL_OID]))) = type_oid_t{0};

  *(reinterpret_cast<bool *>(redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROISWINDOW_COL_OID]))) =
      false;

  // stable by default
  *(reinterpret_cast<char *>(redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROVOLATILE_COL_OID]))) =
      's';

  // strict by default
  *(reinterpret_cast<bool *>(redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROISSTRICT_COL_OID]))) =
      true;

  *(reinterpret_cast<double *>(redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROROWS_COL_OID]))) = 0;

  *(reinterpret_cast<double *>(redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROCOST_COL_OID]))) = 0;

  redo->Delta()->SetNull(pg_proc_all_cols_prm_[PgProc::PROCONFIG_COL_OID]);
  redo->Delta()->SetNull(pg_proc_all_cols_prm_[PgProc::PRO_CTX_PTR_COL_OID]);

  const auto tuple_slot = procs_->Insert(txn, redo);

  auto oid_pri = procs_oid_index_->GetProjectedRowInitializer();
  auto name_pri = procs_name_index_->GetProjectedRowInitializer();

  byte *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto name_pr = name_pri.InitializeRow(buffer);
  auto name_map = procs_name_index_->GetKeyOidToOffsetMap();
  *(reinterpret_cast<namespace_oid_t *>(name_pr->AccessForceNotNull(name_map[indexkeycol_oid_t(1)]))) = procns;
  *(reinterpret_cast<storage::VarlenEntry *>(name_pr->AccessForceNotNull(name_map[indexkeycol_oid_t(2)]))) =
      name_varlen;

  auto result = procs_name_index_->Insert(txn, *name_pr, tuple_slot);
  if (!result) {
    delete[] buffer;
    return false;
  }

  auto oid_pr = oid_pri.InitializeRow(buffer);
  *(reinterpret_cast<proc_oid_t *>(oid_pr->AccessForceNotNull(0))) = oid;
  result = procs_oid_index_->InsertUnique(txn, *oid_pr, tuple_slot);
  NOISEPAGE_ASSERT(result, "Oid insertion should be unique");

  delete[] buffer;
  return true;
}

bool PgProcImpl::DropProcedure(const common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc) {
  NOISEPAGE_ASSERT(proc != INVALID_PROC_OID, "Invalid oid passed");

  auto name_pri = procs_name_index_->GetProjectedRowInitializer();
  auto oid_pri = procs_oid_index_->GetProjectedRowInitializer();

  byte *const buffer = common::AllocationUtil::AllocateAligned(pg_proc_all_cols_pri_.ProjectedRowSize());

  auto oid_pr = oid_pri.InitializeRow(buffer);
  *reinterpret_cast<proc_oid_t *>(oid_pr->AccessForceNotNull(0)) = proc;

  std::vector<storage::TupleSlot> results;
  procs_oid_index_->ScanKey(*txn, *oid_pr, &results);
  if (results.empty()) {
    delete[] buffer;
    return false;
  }

  NOISEPAGE_ASSERT(results.size() == 1, "More than one non-unique result found in unique index.");

  auto to_delete_slot = results[0];
  txn->StageDelete(db_oid_, PgLanguage::LANGUAGE_TABLE_OID, to_delete_slot);

  if (!procs_->Delete(txn, to_delete_slot)) {
    // Someone else has a write-lock. Free the buffer and return false to indicate failure
    delete[] buffer;
    return false;
  }

  procs_oid_index_->Delete(txn, *oid_pr, to_delete_slot);

  auto table_pr = pg_proc_all_cols_pri_.InitializeRow(buffer);
  bool UNUSED_ATTRIBUTE visible = procs_->Select(txn, to_delete_slot, table_pr);

  auto name_varlen = *reinterpret_cast<storage::VarlenEntry *>(
      table_pr->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PRONAME_COL_OID]));
  auto proc_ns = *reinterpret_cast<namespace_oid_t *>(
      table_pr->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PRONAMESPACE_COL_OID]));

  auto ctx_ptr = table_pr->AccessWithNullCheck(pg_proc_all_cols_prm_[PgProc::PRO_CTX_PTR_COL_OID]);

  auto name_pr = name_pri.InitializeRow(buffer);

  auto name_map = procs_name_index_->GetKeyOidToOffsetMap();
  *reinterpret_cast<namespace_oid_t *>(name_pr->AccessForceNotNull(name_map[indexkeycol_oid_t(1)])) = proc_ns;
  *reinterpret_cast<storage::VarlenEntry *>(name_pr->AccessForceNotNull(name_map[indexkeycol_oid_t(2)])) = name_varlen;

  procs_name_index_->Delete(txn, *name_pr, to_delete_slot);

  delete[] buffer;

  if (ctx_ptr != nullptr) {
    txn->RegisterCommitAction([=](transaction::DeferredActionManager *deferred_action_manager) {
      deferred_action_manager->RegisterDeferredAction(
          [=]() { deferred_action_manager->RegisterDeferredAction([=]() { delete ctx_ptr; }); });
    });
  }
  return true;
}

bool PgProcImpl::SetProcCtxPtr(common::ManagedPointer<transaction::TransactionContext> txn, const proc_oid_t proc_oid,
                               const execution::functions::FunctionContext *func_context) {
  // Do not need to store the projection map because it is only a single column
  auto oid_pri = procs_oid_index_->GetProjectedRowInitializer();

  auto *const index_buffer = common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize());
  auto *const key_pr = oid_pri.InitializeRow(index_buffer);

  // Find the entry using the index
  *(reinterpret_cast<proc_oid_t *>(key_pr->AccessForceNotNull(0))) = proc_oid;
  std::vector<storage::TupleSlot> index_results;
  procs_oid_index_->ScanKey(*txn, *key_pr, &index_results);
  NOISEPAGE_ASSERT(
      index_results.size() == 1,
      "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function was "
      "called with an oid that doesn't exist in the Catalog, which implies a programmer error. There's no reasonable "
      "code path for this to be called on an oid that isn't present.");

  delete[] index_buffer;
  auto *const update_redo = txn->StageWrite(db_oid_, PgProc::PRO_TABLE_OID, pg_proc_ptr_pri_);
  *reinterpret_cast<const execution::functions::FunctionContext **>(update_redo->Delta()->AccessForceNotNull(0)) =
      func_context;
  update_redo->SetTupleSlot(index_results[0]);
  return procs_->Update(txn, update_redo);
}

common::ManagedPointer<execution::functions::FunctionContext> PgProcImpl::GetProcCtxPtr(
    common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid) {
  // Do not need to store the projection map because it is only a single column
  auto oid_pri = procs_oid_index_->GetProjectedRowInitializer();

  auto *const buffer = common::AllocationUtil::AllocateAligned(pg_proc_ptr_pri_.ProjectedRowSize());
  auto *const key_pr = oid_pri.InitializeRow(buffer);

  // Find the entry using the index
  *(reinterpret_cast<proc_oid_t *>(key_pr->AccessForceNotNull(0))) = proc_oid;
  std::vector<storage::TupleSlot> index_results;
  procs_oid_index_->ScanKey(*txn, *key_pr, &index_results);
  NOISEPAGE_ASSERT(
      index_results.size() == 1,
      "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function was "
      "called with an oid that doesn't exist in the Catalog, which implies a programmer error. There's no reasonable "
      "code path for this to be called on an oid that isn't present.");

  auto *select_pr = pg_proc_ptr_pri_.InitializeRow(buffer);
  const auto result UNUSED_ATTRIBUTE = procs_->Select(txn, index_results[0], select_pr);
  NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");

  auto *ptr_ptr = (reinterpret_cast<void **>(select_pr->AccessWithNullCheck(0)));

  execution::functions::FunctionContext *ptr;
  NOISEPAGE_ASSERT(nullptr != ptr_ptr, "GetProcCtxPtr called on an invalid OID or before SetProcCtxPtr.");
  ptr = *reinterpret_cast<execution::functions::FunctionContext **>(ptr_ptr);

  delete[] buffer;
  return common::ManagedPointer<execution::functions::FunctionContext>(ptr);
}

proc_oid_t PgProcImpl::GetProcOid(const common::ManagedPointer<transaction::TransactionContext> txn,
                                  const common::ManagedPointer<DatabaseCatalog> dbc, const namespace_oid_t procns,
                                  const std::string &procname, const std::vector<type_oid_t> &arg_types) {
  auto name_pri = procs_name_index_->GetProjectedRowInitializer();
  byte *const buffer = common::AllocationUtil::AllocateAligned(pg_proc_all_cols_pri_.ProjectedRowSize());

  auto name_pr = name_pri.InitializeRow(buffer);
  auto name_map = procs_name_index_->GetKeyOidToOffsetMap();

  auto name_varlen = storage::StorageUtil::CreateVarlen(procname);
  auto all_arg_types_varlen = storage::StorageUtil::CreateVarlen(arg_types);
  *reinterpret_cast<namespace_oid_t *>(name_pr->AccessForceNotNull(name_map[indexkeycol_oid_t(1)])) = procns;
  *reinterpret_cast<storage::VarlenEntry *>(name_pr->AccessForceNotNull(name_map[indexkeycol_oid_t(2)])) = name_varlen;

  std::vector<storage::TupleSlot> results;
  procs_name_index_->ScanKey(*txn, *name_pr, &results);

  proc_oid_t ret = INVALID_PROC_OID;
  std::vector<proc_oid_t> matching_functions;
  if (!results.empty()) {
    const std::vector<type_oid_t> variadic = {dbc->GetTypeOidForType(type::TypeId::VARIADIC)};
    auto variadic_varlen = storage::StorageUtil::CreateVarlen(variadic);

    // Search through results and see if any match the parsed function by argument types
    for (auto &tuple : results) {
      auto table_pr = pg_proc_all_cols_pri_.InitializeRow(buffer);
      bool UNUSED_ATTRIBUTE visible = procs_->Select(txn, tuple, table_pr);

      // "PROARGTYPES ... represents the call signature of the function". Check only input arguments.
      // https://www.postgresql.org/docs/12/catalog-pg-proc.html
      storage::VarlenEntry index_all_arg_types = *reinterpret_cast<storage::VarlenEntry *>(
          table_pr->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROARGTYPES_COL_OID]));

      // variadic functions will match any argument types as long as there one or more arguments
      if (index_all_arg_types == all_arg_types_varlen ||
          (index_all_arg_types == variadic_varlen && !arg_types.empty())) {
        proc_oid_t proc_oid = *reinterpret_cast<proc_oid_t *>(
            table_pr->AccessForceNotNull(pg_proc_all_cols_prm_[PgProc::PROOID_COL_OID]));
        matching_functions.push_back(proc_oid);
        break;
      }
    }
    if (variadic_varlen.NeedReclaim()) {
      delete[] variadic_varlen.Content();
    }
  }

  if (name_varlen.NeedReclaim()) {
    delete[] name_varlen.Content();
  }

  if (all_arg_types_varlen.NeedReclaim()) {
    delete[] all_arg_types_varlen.Content();
  }

  delete[] buffer;

  if (matching_functions.size() == 1) {
    ret = matching_functions[0];
  } else if (matching_functions.size() > 1) {
    // TODO(Joe Koshakow) would be nice to to include the parsed arg types of the function and the arg types that it
    // matches with
    throw BINDER_EXCEPTION(
        fmt::format(
            "Ambiguous function \"{}\", with given types. It matches multiple function signatures in the catalog",
            procname),
        common::ErrorCode::ERRCODE_DUPLICATE_FUNCTION);
  }

  return ret;
}

void PgProcImpl::BootstrapProcs(const common::ManagedPointer<transaction::TransactionContext> txn,
                                const common::ManagedPointer<DatabaseCatalog> dbc) {
  auto dec_type = dbc->GetTypeOidForType(type::TypeId::DECIMAL);
  auto int_type = dbc->GetTypeOidForType(type::TypeId::INTEGER);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "exp", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"num"}, {dec_type}, {dec_type}, {}, dec_type, "",
                  true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "atan2", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {dec_type, dec_type}, {dec_type, dec_type},
                  {}, dec_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "abs", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y"}, {dec_type}, {dec_type}, {}, dec_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "abs", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y"}, {int_type}, {int_type}, {}, int_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "mod", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {dec_type, dec_type}, {dec_type, dec_type},
                  {}, dec_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "mod", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {int_type, int_type}, {int_type, int_type},
                  {}, int_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "round", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {dec_type, int_type}, {dec_type, int_type},
                  {}, dec_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "pow", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {dec_type, dec_type}, {dec_type, dec_type},
                  {}, dec_type, "", true);

#define BOOTSTRAP_TRIG_FN(str_name, builtin)                                                                         \
  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, str_name, PgLanguage::INTERNAL_LANGUAGE_OID,                    \
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"theta"}, {dec_type}, {dec_type}, {}, dec_type, "", \
                  true);

  BOOTSTRAP_TRIG_FN("acos", execution::ast::Builtin::ACos)

  BOOTSTRAP_TRIG_FN("asin", execution::ast::Builtin::ASin)

  BOOTSTRAP_TRIG_FN("atan", execution::ast::Builtin::ATan)

  BOOTSTRAP_TRIG_FN("cos", execution::ast::Builtin::Cos)

  BOOTSTRAP_TRIG_FN("sin", execution::ast::Builtin::Sin)

  BOOTSTRAP_TRIG_FN("tan", execution::ast::Builtin::Tan)

  BOOTSTRAP_TRIG_FN("cosh", execution::ast::Builtin::Cosh)

  BOOTSTRAP_TRIG_FN("sinh", execution::ast::Builtin::Sinh)

  BOOTSTRAP_TRIG_FN("tanh", execution::ast::Builtin::Tanh)

  BOOTSTRAP_TRIG_FN("cot", execution::ast::Builtin::Cot)

  BOOTSTRAP_TRIG_FN("ceil", execution::ast::Builtin::Ceil)

  BOOTSTRAP_TRIG_FN("floor", execution::ast::Builtin::Floor)

  BOOTSTRAP_TRIG_FN("truncate", execution::ast::Builtin::Truncate)

  BOOTSTRAP_TRIG_FN("log10", execution::ast::Builtin::Log10)

  BOOTSTRAP_TRIG_FN("log2", execution::ast::Builtin::Log2)

  BOOTSTRAP_TRIG_FN("sqrt", execution::ast::Builtin::Sqrt)

  BOOTSTRAP_TRIG_FN("cbrt", execution::ast::Builtin::Cbrt)

  BOOTSTRAP_TRIG_FN("round", execution::ast::Builtin::Round)

#undef BOOTSTRAP_TRIG_FN

  const auto str_type = dbc->GetTypeOidForType(type::TypeId::VARCHAR);
  const auto real_type = dbc->GetTypeOidForType(type::TypeId::DECIMAL);
  const auto date_type = dbc->GetTypeOidForType(type::TypeId::DATE);
  const auto bool_type = dbc->GetTypeOidForType(type::TypeId::BOOLEAN);
  const auto variadic_type = dbc->GetTypeOidForType(type::TypeId::VARIADIC);

  CreateProcedure(
      txn, proc_oid_t{dbc->next_oid_++}, "nprunnersemitint", PgLanguage::INTERNAL_LANGUAGE_OID,
      PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"num_tuples", "num_cols", "num_int_cols", "num_real_cols"},
      {int_type, int_type, int_type, int_type}, {int_type, int_type, int_type, int_type},
      {PgProc::ArgModes::IN, PgProc::ArgModes::IN, PgProc::ArgModes::IN, PgProc::ArgModes::IN}, int_type, "", false);

  CreateProcedure(
      txn, proc_oid_t{dbc->next_oid_++}, "nprunnersemitreal", PgLanguage::INTERNAL_LANGUAGE_OID,
      PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"num_tuples", "num_cols", "num_int_cols", "num_real_cols"},
      {int_type, int_type, int_type, int_type}, {int_type, int_type, int_type, int_type},
      {PgProc::ArgModes::IN, PgProc::ArgModes::IN, PgProc::ArgModes::IN, PgProc::ArgModes::IN}, real_type, "", false);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "nprunnersdummyint", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {}, {}, {}, {}, int_type, "", false);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "nprunnersdummyreal", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {}, {}, {}, {}, real_type, "", false);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "ascii", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, int_type, "",
                  true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "chr", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"num"}, {int_type}, {int_type}, {}, str_type, "",
                  true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "char_length", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, int_type, "",
                  true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "lower", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "",
                  true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "upper", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "",
                  true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "initcap", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "",
                  true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "version", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {}, {}, {}, {}, str_type, "", false);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "split_part", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "delim", "field"},
                  {str_type, str_type, int_type}, {str_type, str_type, int_type}, {}, str_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "length", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, int_type, "",
                  true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "starts_with", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "start"}, {str_type, str_type},
                  {str_type, str_type}, {}, bool_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "substr", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "pos", "len"}, {str_type, int_type, int_type},
                  {str_type, int_type, int_type}, {}, str_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "reverse", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "",
                  true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "left", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "int"}, {str_type, int_type},
                  {str_type, int_type}, {}, str_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "right", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "int"}, {str_type, int_type},
                  {str_type, int_type}, {}, str_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "repeat", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "int"}, {str_type, int_type},
                  {str_type, int_type}, {}, str_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "btrim", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "",
                  true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "btrim", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "str"}, {str_type, str_type},
                  {str_type, str_type}, {}, str_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "concat", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {variadic_type}, {variadic_type}, {}, str_type,
                  "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "date_part", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"date, date_part_type"}, {date_type, int_type},
                  {date_type, int_type}, {}, int_type, "", false);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "position", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str1", "str2"}, {str_type, str_type},
                  {str_type, str_type}, {}, int_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "lpad", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "len", "pad"}, {str_type, int_type, str_type},
                  {str_type, int_type, str_type}, {}, str_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "lpad", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "len"}, {str_type, int_type},
                  {str_type, int_type}, {}, str_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "ltrim", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "chars"}, {str_type, str_type},
                  {str_type, str_type}, {}, str_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "ltrim", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "",
                  true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "rpad", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "len", "pad"}, {str_type, int_type, str_type},
                  {str_type, int_type, str_type}, {}, str_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "rpad", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "len"}, {str_type, int_type},
                  {str_type, int_type}, {}, str_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "rtrim", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "chars"}, {str_type, str_type},
                  {str_type, str_type}, {}, str_type, "", true);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "rtrim", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "",
                  true);

  BootstrapProcContexts(txn, dbc);
}

void PgProcImpl::BootstrapProcContext(const common::ManagedPointer<transaction::TransactionContext> txn,
                                      const common::ManagedPointer<DatabaseCatalog> dbc, std::string &&func_name,
                                      const type::TypeId func_ret_type, std::vector<type::TypeId> &&arg_types,
                                      const execution::ast::Builtin builtin, const bool is_exec_ctx_required) {
  std::vector<type_oid_t> arg_type_oids;
  arg_type_oids.reserve(arg_types.size());
  for (const auto type : arg_types) {
    arg_type_oids.emplace_back(dbc->GetTypeOidForType(type));
  }
  const proc_oid_t proc_oid =
      GetProcOid(txn, dbc, PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, func_name, arg_type_oids);
  const auto *const func_context = new execution::functions::FunctionContext(
      std::move(func_name), func_ret_type, std::move(arg_types), builtin, is_exec_ctx_required);
  const auto retval UNUSED_ATTRIBUTE = dbc->SetProcCtxPtr(txn, proc_oid, func_context);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
}

void PgProcImpl::BootstrapProcContexts(const common::ManagedPointer<transaction::TransactionContext> txn,
                                       const common::ManagedPointer<DatabaseCatalog> dbc) {
  BootstrapProcContext(txn, dbc, "atan2", type::TypeId::DECIMAL, {type::TypeId::DECIMAL, type::TypeId::DECIMAL},
                       execution::ast::Builtin::ATan2, false);

  BootstrapProcContext(txn, dbc, "abs", type::TypeId::DECIMAL, {type::TypeId::DECIMAL}, execution::ast::Builtin::Abs,
                       false);

  BootstrapProcContext(txn, dbc, "abs", type::TypeId::INTEGER, {type::TypeId::INTEGER}, execution::ast::Builtin::Abs,
                       false);

#define BOOTSTRAP_TRIG_FN(str_name, builtin) \
  BootstrapProcContext(txn, dbc, str_name, type::TypeId::DECIMAL, {type::TypeId::DECIMAL}, builtin, false);

  BOOTSTRAP_TRIG_FN("acos", execution::ast::Builtin::ACos)

  BOOTSTRAP_TRIG_FN("asin", execution::ast::Builtin::ASin)

  BOOTSTRAP_TRIG_FN("atan", execution::ast::Builtin::ATan)

  BOOTSTRAP_TRIG_FN("cos", execution::ast::Builtin::Cos)

  BOOTSTRAP_TRIG_FN("sin", execution::ast::Builtin::Sin)

  BOOTSTRAP_TRIG_FN("tan", execution::ast::Builtin::Tan)

  BOOTSTRAP_TRIG_FN("cosh", execution::ast::Builtin::Cosh)

  BOOTSTRAP_TRIG_FN("sinh", execution::ast::Builtin::Sinh)

  BOOTSTRAP_TRIG_FN("tanh", execution::ast::Builtin::Tanh)

  BOOTSTRAP_TRIG_FN("cot", execution::ast::Builtin::Cot)

  BOOTSTRAP_TRIG_FN("ceil", execution::ast::Builtin::Ceil)

  BOOTSTRAP_TRIG_FN("floor", execution::ast::Builtin::Floor)

  BOOTSTRAP_TRIG_FN("truncate", execution::ast::Builtin::Truncate)

  BOOTSTRAP_TRIG_FN("log10", execution::ast::Builtin::Log10)

  BOOTSTRAP_TRIG_FN("log2", execution::ast::Builtin::Log2)

  BOOTSTRAP_TRIG_FN("sqrt", execution::ast::Builtin::Sqrt)

  BOOTSTRAP_TRIG_FN("cbrt", execution::ast::Builtin::Cbrt)

  BOOTSTRAP_TRIG_FN("round", execution::ast::Builtin::Round)

#undef BOOTSTRAP_TRIG_FN

  BootstrapProcContext(txn, dbc, "round", type::TypeId::DECIMAL, {type::TypeId::DECIMAL, type::TypeId::INTEGER},
                       execution::ast::Builtin::Round2, false);

  BootstrapProcContext(txn, dbc, "exp", type::TypeId::DECIMAL, {type::TypeId::DECIMAL}, execution::ast::Builtin::Exp,
                       true);

  BootstrapProcContext(txn, dbc, "ascii", type::TypeId::INTEGER, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::ASCII, true);

  BootstrapProcContext(txn, dbc, "lower", type::TypeId::VARCHAR, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::Lower, true);

  BootstrapProcContext(txn, dbc, "initcap", type::TypeId::VARCHAR, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::InitCap, true);

  BootstrapProcContext(txn, dbc, "pow", type::TypeId::DECIMAL, {type::TypeId::DECIMAL, type::TypeId::DECIMAL},
                       execution::ast::Builtin::Pow, false);

  BootstrapProcContext(txn, dbc, "split_part", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::VARCHAR, type::TypeId::INTEGER},
                       execution::ast::Builtin::SplitPart, true);

  BootstrapProcContext(txn, dbc, "chr", type::TypeId::VARCHAR, {type::TypeId::INTEGER}, execution::ast::Builtin::Chr,
                       true);

  BootstrapProcContext(txn, dbc, "char_length", type::TypeId::INTEGER, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::CharLength, true);

  BootstrapProcContext(txn, dbc, "position", type::TypeId::INTEGER, {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
                       execution::ast::Builtin::Position, true);

  BootstrapProcContext(txn, dbc, "length", type::TypeId::INTEGER, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::Length, true);

  BootstrapProcContext(txn, dbc, "upper", type::TypeId::VARCHAR, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::Upper, true);

  BootstrapProcContext(txn, dbc, "version", type::TypeId::VARCHAR, {}, execution::ast::Builtin::Version, true);

  BootstrapProcContext(txn, dbc, "starts_with", type::TypeId::BOOLEAN, {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
                       execution::ast::Builtin::StartsWith, true);

  BootstrapProcContext(txn, dbc, "substr", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER, type::TypeId::INTEGER},
                       execution::ast::Builtin::Substring, true);

  BootstrapProcContext(txn, dbc, "reverse", type::TypeId::VARCHAR, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::Reverse, true);

  BootstrapProcContext(txn, dbc, "left", type::TypeId::VARCHAR, {type::TypeId::VARCHAR, type::TypeId::INTEGER},
                       execution::ast::Builtin::Left, true);

  BootstrapProcContext(txn, dbc, "right", type::TypeId::VARCHAR, {type::TypeId::VARCHAR, type::TypeId::INTEGER},
                       execution::ast::Builtin::Right, true);

  BootstrapProcContext(txn, dbc, "repeat", type::TypeId::VARCHAR, {type::TypeId::VARCHAR, type::TypeId::INTEGER},
                       execution::ast::Builtin::Repeat, true);

  BootstrapProcContext(txn, dbc, "btrim", type::TypeId::VARCHAR, {type::TypeId::VARCHAR}, execution::ast::Builtin::Trim,
                       true);

  BootstrapProcContext(txn, dbc, "btrim", type::TypeId::VARCHAR, {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
                       execution::ast::Builtin::Trim2, true);

  BootstrapProcContext(txn, dbc, "concat", type::TypeId::VARCHAR, {type::TypeId::VARIADIC},
                       execution::ast::Builtin::Concat, true);

  BootstrapProcContext(txn, dbc, "lpad", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER, type::TypeId::VARCHAR},
                       execution::ast::Builtin::Lpad, true);

  BootstrapProcContext(txn, dbc, "lpad", type::TypeId::VARCHAR, {type::TypeId::VARCHAR, type::TypeId::INTEGER},
                       execution::ast::Builtin::Lpad, true);

  BootstrapProcContext(txn, dbc, "ltrim", type::TypeId::VARCHAR, {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
                       execution::ast::Builtin::Ltrim, true);

  BootstrapProcContext(txn, dbc, "ltrim", type::TypeId::VARCHAR, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::Ltrim, true);

  BootstrapProcContext(txn, dbc, "rpad", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER, type::TypeId::VARCHAR},
                       execution::ast::Builtin::Rpad, true);

  BootstrapProcContext(txn, dbc, "rpad", type::TypeId::VARCHAR, {type::TypeId::VARCHAR, type::TypeId::INTEGER},
                       execution::ast::Builtin::Rpad, true);

  BootstrapProcContext(txn, dbc, "rtrim", type::TypeId::VARCHAR, {type::TypeId::VARCHAR, type::TypeId::VARCHAR},
                       execution::ast::Builtin::Rtrim, true);

  BootstrapProcContext(txn, dbc, "rtrim", type::TypeId::VARCHAR, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::Rtrim, true);

  BootstrapProcContext(txn, dbc, "mod", type::TypeId::DECIMAL, {type::TypeId::DECIMAL, type::TypeId::DECIMAL},
                       execution::ast::Builtin::Mod, false);

  BootstrapProcContext(txn, dbc, "mod", type::TypeId::INTEGER, {type::TypeId::INTEGER, type::TypeId::INTEGER},
                       execution::ast::Builtin::Mod, false);

  BootstrapProcContext(txn, dbc, "nprunnersemitint", type::TypeId::INTEGER,
                       {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER},
                       execution::ast::Builtin::NpRunnersEmitInt, true);

  BootstrapProcContext(txn, dbc, "nprunnersemitreal", type::TypeId::DECIMAL,
                       {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER},
                       execution::ast::Builtin::NpRunnersEmitReal, true);

  BootstrapProcContext(txn, dbc, "nprunnersdummyint", type::TypeId::INTEGER, {},
                       execution::ast::Builtin::NpRunnersDummyInt, true);

  BootstrapProcContext(txn, dbc, "nprunnersdummyreal", type::TypeId::DECIMAL, {},
                       execution::ast::Builtin::NpRunnersDummyReal, true);

  BootstrapProcContext(txn, dbc, "date_part", type::TypeId::INTEGER, {type::TypeId::DATE, type::TypeId::INTEGER},
                       execution::ast::Builtin::DatePart, false);
}

}  // namespace noisepage::catalog::postgres
