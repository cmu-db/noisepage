#include "catalog/postgres/pg_proc_impl.h"

#include <vector>

#include "catalog/database_catalog.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_proc.h"
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
  UNUSED_ATTRIBUTE bool retval;

  retval = dbc->CreateTableEntry(txn, postgres::PgProc::PRO_TABLE_OID, postgres::NAMESPACE_CATALOG_NAMESPACE_OID,
                                 "pg_proc", postgres::Builder::GetProcTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetTablePointer(txn, postgres::PgProc::PRO_TABLE_OID, procs_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::PgProc::PRO_TABLE_OID,
                                 postgres::PgProc::PRO_OID_INDEX_OID, "pg_proc_oid_index",
                                 postgres::Builder::GetProcOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, postgres::PgProc::PRO_OID_INDEX_OID, procs_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::PgProc::PRO_TABLE_OID,
                                 postgres::PgProc::PRO_NAME_INDEX_OID, "pg_proc_name_index",
                                 postgres::Builder::GetProcNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, postgres::PgProc::PRO_NAME_INDEX_OID, procs_name_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  BootstrapProcs(dbc, txn);
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
  return [func_contexts]() {
    for (auto ctx : func_contexts) {
      delete ctx;
    }
  };
}

bool PgProcImpl::CreateProcedure(const common::ManagedPointer<transaction::TransactionContext> txn,
                                 const proc_oid_t oid, const std::string &procname, const language_oid_t language_oid,
                                 const namespace_oid_t procns, const std::vector<std::string> &args,
                                 const std::vector<type_oid_t> &arg_types, const std::vector<type_oid_t> &all_arg_types,
                                 const std::vector<postgres::PgProc::ArgModes> &arg_modes, const type_oid_t rettype,
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

  auto *const redo = txn->StageWrite(db_oid_, postgres::PgProc::PRO_TABLE_OID, pg_proc_all_cols_pri_);
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PRONAME_COL_OID]))) = name_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(redo->Delta()->AccessForceNotNull(
      pg_proc_all_cols_prm_[postgres::PgProc::PROARGNAMES_COL_OID]))) = arg_names_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(redo->Delta()->AccessForceNotNull(
      pg_proc_all_cols_prm_[postgres::PgProc::PROARGTYPES_COL_OID]))) = arg_types_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(redo->Delta()->AccessForceNotNull(
      pg_proc_all_cols_prm_[postgres::PgProc::PROALLARGTYPES_COL_OID]))) = all_arg_types_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(redo->Delta()->AccessForceNotNull(
      pg_proc_all_cols_prm_[postgres::PgProc::PROARGMODES_COL_OID]))) = arg_modes_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PROSRC_COL_OID]))) = src_varlen;

  *(reinterpret_cast<proc_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PROOID_COL_OID]))) = oid;
  *(reinterpret_cast<language_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PROLANG_COL_OID]))) = language_oid;
  *(reinterpret_cast<namespace_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PRONAMESPACE_COL_OID]))) = procns;
  *(reinterpret_cast<type_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PRORETTYPE_COL_OID]))) = rettype;

  *(reinterpret_cast<uint16_t *>(redo->Delta()->AccessForceNotNull(
      pg_proc_all_cols_prm_[postgres::PgProc::PRONARGS_COL_OID]))) = static_cast<uint16_t>(args.size());

  // setting zero default args
  *(reinterpret_cast<uint16_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PRONARGDEFAULTS_COL_OID]))) = 0;
  redo->Delta()->SetNull(pg_proc_all_cols_prm_[postgres::PgProc::PROARGDEFAULTS_COL_OID]);

  *reinterpret_cast<bool *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PROISAGG_COL_OID])) = is_aggregate;

  // setting defaults of unexposed attributes
  // proiswindow, proisstrict, provolatile, provariadic, prorows, procost, proconfig

  // postgres documentation says this should be 0 if no variadics are there
  *(reinterpret_cast<type_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PROVARIADIC_COL_OID]))) = type_oid_t{0};

  *(reinterpret_cast<bool *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PROISWINDOW_COL_OID]))) = false;

  // stable by default
  *(reinterpret_cast<char *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PROVOLATILE_COL_OID]))) = 's';

  // strict by default
  *(reinterpret_cast<bool *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PROISSTRICT_COL_OID]))) = true;

  *(reinterpret_cast<double *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PROROWS_COL_OID]))) = 0;

  *(reinterpret_cast<double *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PROCOST_COL_OID]))) = 0;

  redo->Delta()->SetNull(pg_proc_all_cols_prm_[postgres::PgProc::PROCONFIG_COL_OID]);
  redo->Delta()->SetNull(pg_proc_all_cols_prm_[postgres::PgProc::PRO_CTX_PTR_COL_OID]);

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
  txn->StageDelete(db_oid_, postgres::LANGUAGE_TABLE_OID, to_delete_slot);

  if (!procs_->Delete(txn, to_delete_slot)) {
    // Someone else has a write-lock. Free the buffer and return false to indicate failure
    delete[] buffer;
    return false;
  }

  procs_oid_index_->Delete(txn, *oid_pr, to_delete_slot);

  auto table_pr = pg_proc_all_cols_pri_.InitializeRow(buffer);
  bool UNUSED_ATTRIBUTE visible = procs_->Select(txn, to_delete_slot, table_pr);

  auto name_varlen = *reinterpret_cast<storage::VarlenEntry *>(
      table_pr->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PRONAME_COL_OID]));
  auto proc_ns = *reinterpret_cast<namespace_oid_t *>(
      table_pr->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PRONAMESPACE_COL_OID]));

  auto ctx_ptr = table_pr->AccessWithNullCheck(pg_proc_all_cols_prm_[postgres::PgProc::PRO_CTX_PTR_COL_OID]);

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
  auto *const update_redo = txn->StageWrite(db_oid_, postgres::PgProc::PRO_TABLE_OID, pg_proc_ptr_pri_);
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
  if (ptr_ptr == nullptr) {
    ptr = nullptr;
  } else {
    ptr = *reinterpret_cast<execution::functions::FunctionContext **>(ptr_ptr);
  }

  delete[] buffer;
  return common::ManagedPointer<execution::functions::FunctionContext>(ptr);
}

proc_oid_t PgProcImpl::GetProcOid(const common::ManagedPointer<DatabaseCatalog> dbc,
                                  const common::ManagedPointer<transaction::TransactionContext> txn,
                                  namespace_oid_t procns, const std::string &procname,
                                  const std::vector<type_oid_t> &arg_types) {
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
      storage::VarlenEntry index_all_arg_types = *reinterpret_cast<storage::VarlenEntry *>(
          table_pr->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PROALLARGTYPES_COL_OID]));
      // variadic functions will match any argument types as long as there one or more arguments
      if (index_all_arg_types == all_arg_types_varlen ||
          (index_all_arg_types == variadic_varlen && !arg_types.empty())) {
        proc_oid_t proc_oid = *reinterpret_cast<proc_oid_t *>(
            table_pr->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PgProc::PROOID_COL_OID]));
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

void PgProcImpl::BootstrapProcs(const common::ManagedPointer<DatabaseCatalog> dbc,
                                const common::ManagedPointer<transaction::TransactionContext> txn) {
  auto dec_type = dbc->GetTypeOidForType(type::TypeId::DECIMAL);
  auto int_type = dbc->GetTypeOidForType(type::TypeId::INTEGER);

  dbc->CreateProcedure(txn, postgres::PgProc::EXP_PRO_OID, "exp", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"num"}, {dec_type}, {dec_type}, {}, dec_type, "",
                       true);

  dbc->CreateProcedure(txn, postgres::PgProc::ATAN2_PRO_OID, "atan2", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {dec_type, dec_type},
                       {dec_type, dec_type}, {}, dec_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::ABS_REAL_PRO_OID, "abs", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y"}, {dec_type}, {dec_type}, {}, dec_type, "",
                       true);

  dbc->CreateProcedure(txn, postgres::PgProc::ABS_INT_PRO_OID, "abs", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y"}, {int_type}, {int_type}, {}, int_type, "",
                       true);

  dbc->CreateProcedure(txn, postgres::PgProc::MOD_PRO_OID, "mod", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {dec_type, dec_type},
                       {dec_type, dec_type}, {}, dec_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::INTMOD_PRO_OID, "mod", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {int_type, int_type},
                       {int_type, int_type}, {}, int_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::ROUND2_PRO_OID, "round", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {dec_type, int_type},
                       {dec_type, int_type}, {}, dec_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::POW_PRO_OID, "pow", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {dec_type, dec_type},
                       {dec_type, dec_type}, {}, dec_type, "", true);

#define BOOTSTRAP_TRIG_FN(str_name, pro_oid, builtin)                                                                  \
  dbc->CreateProcedure(txn, pro_oid, str_name, postgres::INTERNAL_LANGUAGE_OID,                                        \
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"theta"}, {dec_type}, {dec_type}, {}, dec_type, "", \
                       true);

  BOOTSTRAP_TRIG_FN("acos", postgres::PgProc::ACOS_PRO_OID, execution::ast::Builtin::ACos)

  BOOTSTRAP_TRIG_FN("asin", postgres::PgProc::ASIN_PRO_OID, execution::ast::Builtin::ASin)

  BOOTSTRAP_TRIG_FN("atan", postgres::PgProc::ATAN_PRO_OID, execution::ast::Builtin::ATan)

  BOOTSTRAP_TRIG_FN("cos", postgres::PgProc::COS_PRO_OID, execution::ast::Builtin::Cos)

  BOOTSTRAP_TRIG_FN("sin", postgres::PgProc::SIN_PRO_OID, execution::ast::Builtin::Sin)

  BOOTSTRAP_TRIG_FN("tan", postgres::PgProc::TAN_PRO_OID, execution::ast::Builtin::Tan)

  BOOTSTRAP_TRIG_FN("cosh", postgres::PgProc::COSH_PRO_OID, execution::ast::Builtin::Cosh)

  BOOTSTRAP_TRIG_FN("sinh", postgres::PgProc::SINH_PRO_OID, execution::ast::Builtin::Sinh)

  BOOTSTRAP_TRIG_FN("tanh", postgres::PgProc::TANH_PRO_OID, execution::ast::Builtin::Tanh)

  BOOTSTRAP_TRIG_FN("cot", postgres::PgProc::COT_PRO_OID, execution::ast::Builtin::Cot)

  BOOTSTRAP_TRIG_FN("ceil", postgres::PgProc::CEIL_PRO_OID, execution::ast::Builtin::Ceil)

  BOOTSTRAP_TRIG_FN("floor", postgres::PgProc::FLOOR_PRO_OID, execution::ast::Builtin::Floor)

  BOOTSTRAP_TRIG_FN("truncate", postgres::PgProc::TRUNCATE_PRO_OID, execution::ast::Builtin::Truncate)

  BOOTSTRAP_TRIG_FN("log10", postgres::PgProc::LOG10_PRO_OID, execution::ast::Builtin::Log10)

  BOOTSTRAP_TRIG_FN("log2", postgres::PgProc::LOG2_PRO_OID, execution::ast::Builtin::Log2)

  BOOTSTRAP_TRIG_FN("sqrt", postgres::PgProc::SQRT_PRO_OID, execution::ast::Builtin::Sqrt)

  BOOTSTRAP_TRIG_FN("cbrt", postgres::PgProc::CBRT_PRO_OID, execution::ast::Builtin::Cbrt)

  BOOTSTRAP_TRIG_FN("round", postgres::PgProc::ROUND_PRO_OID, execution::ast::Builtin::Round)

#undef BOOTSTRAP_TRIG_FN

  const auto str_type = dbc->GetTypeOidForType(type::TypeId::VARCHAR);
  const auto real_type = dbc->GetTypeOidForType(type::TypeId::DECIMAL);
  const auto date_type = dbc->GetTypeOidForType(type::TypeId::DATE);
  const auto bool_type = dbc->GetTypeOidForType(type::TypeId::BOOLEAN);
  const auto variadic_type = dbc->GetTypeOidForType(type::TypeId::VARIADIC);

  dbc->CreateProcedure(txn, postgres::PgProc::NP_RUNNERS_EMIT_INT_PRO_OID, "nprunnersemitint",
                       postgres::INTERNAL_LANGUAGE_OID, postgres::NAMESPACE_DEFAULT_NAMESPACE_OID,
                       {"num_tuples", "num_cols", "num_int_cols", "num_real_cols"},
                       {int_type, int_type, int_type, int_type}, {int_type, int_type, int_type, int_type},
                       {postgres::PgProc::ArgModes::IN, postgres::PgProc::ArgModes::IN, postgres::PgProc::ArgModes::IN,
                        postgres::PgProc::ArgModes::IN},
                       int_type, "", false);

  dbc->CreateProcedure(txn, postgres::PgProc::NP_RUNNERS_EMIT_REAL_PRO_OID, "nprunnersemitreal",
                       postgres::INTERNAL_LANGUAGE_OID, postgres::NAMESPACE_DEFAULT_NAMESPACE_OID,
                       {"num_tuples", "num_cols", "num_int_cols", "num_real_cols"},
                       {int_type, int_type, int_type, int_type}, {int_type, int_type, int_type, int_type},
                       {postgres::PgProc::ArgModes::IN, postgres::PgProc::ArgModes::IN, postgres::PgProc::ArgModes::IN,
                        postgres::PgProc::ArgModes::IN},
                       real_type, "", false);

  dbc->CreateProcedure(txn, postgres::PgProc::NP_RUNNERS_DUMMY_INT_PRO_OID, "nprunnersdummyint",
                       postgres::INTERNAL_LANGUAGE_OID, postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {}, {}, {}, {},
                       int_type, "", false);

  dbc->CreateProcedure(txn, postgres::PgProc::NP_RUNNERS_DUMMY_REAL_PRO_OID, "nprunnersdummyreal",
                       postgres::INTERNAL_LANGUAGE_OID, postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {}, {}, {}, {},
                       real_type, "", false);

  dbc->CreateProcedure(txn, postgres::PgProc::ASCII_PRO_OID, "ascii", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, int_type, "",
                       true);

  dbc->CreateProcedure(txn, postgres::PgProc::CHR_PRO_OID, "chr", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"num"}, {int_type}, {int_type}, {}, str_type, "",
                       true);

  dbc->CreateProcedure(txn, postgres::PgProc::CHARLENGTH_PRO_OID, "char_length", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, int_type, "",
                       true);

  dbc->CreateProcedure(txn, postgres::PgProc::LOWER_PRO_OID, "lower", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "",
                       true);

  dbc->CreateProcedure(txn, postgres::PgProc::UPPER_PRO_OID, "upper", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "",
                       true);

  dbc->CreateProcedure(txn, postgres::PgProc::INITCAP_PRO_OID, "initcap", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "",
                       true);

  dbc->CreateProcedure(txn, postgres::PgProc::VERSION_PRO_OID, "version", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {}, {}, {}, {}, str_type, "", false);

  dbc->CreateProcedure(txn, postgres::PgProc::SPLIT_PART_PRO_OID, "split_part", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "delim", "field"},
                       {str_type, str_type, int_type}, {str_type, str_type, int_type}, {}, str_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::LENGTH_PRO_OID, "length", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, int_type, "",
                       true);

  dbc->CreateProcedure(txn, postgres::PgProc::STARTSWITH_PRO_OID, "starts_with", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "start"}, {str_type, str_type},
                       {str_type, str_type}, {}, bool_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::SUBSTR_PRO_OID, "substr", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "pos", "len"}, {str_type, int_type, int_type},
                       {str_type, int_type, int_type}, {}, str_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::REVERSE_PRO_OID, "reverse", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "",
                       true);

  dbc->CreateProcedure(txn, postgres::PgProc::LEFT_PRO_OID, "left", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "int"}, {str_type, int_type},
                       {str_type, int_type}, {}, str_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::RIGHT_PRO_OID, "right", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "int"}, {str_type, int_type},
                       {str_type, int_type}, {}, str_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::REPEAT_PRO_OID, "repeat", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "int"}, {str_type, int_type},
                       {str_type, int_type}, {}, str_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::TRIM_PRO_OID, "btrim", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "",
                       true);

  dbc->CreateProcedure(txn, postgres::PgProc::TRIM2_PRO_OID, "btrim", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "str"}, {str_type, str_type},
                       {str_type, str_type}, {}, str_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::CONCAT_PRO_OID, "concat", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {variadic_type}, {variadic_type}, {},
                       str_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::DATE_PART_PRO_OID, "date_part", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"date, date_part_type"}, {date_type, int_type},
                       {date_type, int_type}, {}, int_type, "", false);

  dbc->CreateProcedure(txn, postgres::PgProc::POSITION_PRO_OID, "position", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str1", "str2"}, {str_type, str_type},
                       {str_type, str_type}, {}, int_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::LPAD_PRO_OID, "lpad", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "len", "pad"}, {str_type, dec_type, str_type},
                       {str_type, int_type, str_type}, {}, str_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::LPAD2_PRO_OID, "lpad", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "len"}, {str_type, dec_type},
                       {str_type, int_type}, {}, str_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::LTRIM2ARG_PRO_OID, "ltrim", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "chars"}, {str_type, str_type},
                       {str_type, str_type}, {}, str_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::LTRIM1ARG_PRO_OID, "ltrim", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "",
                       true);

  dbc->CreateProcedure(txn, postgres::PgProc::RPAD_PRO_OID, "rpad", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "len", "pad"}, {str_type, dec_type, str_type},
                       {str_type, int_type, str_type}, {}, str_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::RPAD2_PRO_OID, "rpad", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "len"}, {str_type, dec_type},
                       {str_type, int_type}, {}, str_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::RTRIM2ARG_PRO_OID, "rtrim", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "chars"}, {str_type, str_type},
                       {str_type, str_type}, {}, str_type, "", true);

  dbc->CreateProcedure(txn, postgres::PgProc::RTRIM1ARG_PRO_OID, "rtrim", postgres::INTERNAL_LANGUAGE_OID,
                       postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "",
                       true);

  BootstrapProcContexts(dbc, txn);
}

void PgProcImpl::BootstrapProcContext(common::ManagedPointer<DatabaseCatalog> dbc,
                                      const common::ManagedPointer<transaction::TransactionContext> txn,
                                      const proc_oid_t proc_oid, std::string &&func_name,
                                      const type::TypeId func_ret_type, std::vector<type::TypeId> &&args_type,
                                      const execution::ast::Builtin builtin, const bool is_exec_ctx_required) {
  const auto *const func_context = new execution::functions::FunctionContext(
      std::move(func_name), func_ret_type, std::move(args_type), builtin, is_exec_ctx_required);
  const auto retval UNUSED_ATTRIBUTE = dbc->SetProcCtxPtr(txn, proc_oid, func_context);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
}

void PgProcImpl::BootstrapProcContexts(common::ManagedPointer<DatabaseCatalog> dbc,
                                       const common::ManagedPointer<transaction::TransactionContext> txn) {
  BootstrapProcContext(dbc, txn, postgres::PgProc::ATAN2_PRO_OID, "atan2", type::TypeId::DECIMAL,
                       {type::TypeId::DECIMAL, type::TypeId::DECIMAL}, execution::ast::Builtin::ATan2, false);

  BootstrapProcContext(dbc, txn, postgres::PgProc::ABS_REAL_PRO_OID, "abs", type::TypeId::DECIMAL,
                       {type::TypeId::DECIMAL}, execution::ast::Builtin::Abs, false);

  BootstrapProcContext(dbc, txn, postgres::PgProc::ABS_INT_PRO_OID, "abs", type::TypeId::INTEGER,
                       {type::TypeId::INTEGER}, execution::ast::Builtin::Abs, false);

#define BOOTSTRAP_TRIG_FN(str_name, pro_oid, builtin) \
  BootstrapProcContext(dbc, txn, pro_oid, str_name, type::TypeId::DECIMAL, {type::TypeId::DECIMAL}, builtin, false);

  BOOTSTRAP_TRIG_FN("acos", postgres::PgProc::ACOS_PRO_OID, execution::ast::Builtin::ACos)

  BOOTSTRAP_TRIG_FN("asin", postgres::PgProc::ASIN_PRO_OID, execution::ast::Builtin::ASin)

  BOOTSTRAP_TRIG_FN("atan", postgres::PgProc::ATAN_PRO_OID, execution::ast::Builtin::ATan)

  BOOTSTRAP_TRIG_FN("cos", postgres::PgProc::COS_PRO_OID, execution::ast::Builtin::Cos)

  BOOTSTRAP_TRIG_FN("sin", postgres::PgProc::SIN_PRO_OID, execution::ast::Builtin::Sin)

  BOOTSTRAP_TRIG_FN("tan", postgres::PgProc::TAN_PRO_OID, execution::ast::Builtin::Tan)

  BOOTSTRAP_TRIG_FN("cosh", postgres::PgProc::COSH_PRO_OID, execution::ast::Builtin::Cosh)

  BOOTSTRAP_TRIG_FN("sinh", postgres::PgProc::SINH_PRO_OID, execution::ast::Builtin::Sinh)

  BOOTSTRAP_TRIG_FN("tanh", postgres::PgProc::TANH_PRO_OID, execution::ast::Builtin::Tanh)

  BOOTSTRAP_TRIG_FN("cot", postgres::PgProc::COT_PRO_OID, execution::ast::Builtin::Cot)

  BOOTSTRAP_TRIG_FN("ceil", postgres::PgProc::CEIL_PRO_OID, execution::ast::Builtin::Ceil)

  BOOTSTRAP_TRIG_FN("floor", postgres::PgProc::FLOOR_PRO_OID, execution::ast::Builtin::Floor)

  BOOTSTRAP_TRIG_FN("truncate", postgres::PgProc::TRUNCATE_PRO_OID, execution::ast::Builtin::Truncate)

  BOOTSTRAP_TRIG_FN("log10", postgres::PgProc::LOG10_PRO_OID, execution::ast::Builtin::Log10)

  BOOTSTRAP_TRIG_FN("log2", postgres::PgProc::LOG2_PRO_OID, execution::ast::Builtin::Log2)

  BOOTSTRAP_TRIG_FN("sqrt", postgres::PgProc::SQRT_PRO_OID, execution::ast::Builtin::Sqrt)

  BOOTSTRAP_TRIG_FN("cbrt", postgres::PgProc::CBRT_PRO_OID, execution::ast::Builtin::Cbrt)

  BOOTSTRAP_TRIG_FN("round", postgres::PgProc::ROUND_PRO_OID, execution::ast::Builtin::Round)

#undef BOOTSTRAP_TRIG_FN

  BootstrapProcContext(dbc, txn, postgres::PgProc::ROUND2_PRO_OID, "round", type::TypeId::DECIMAL,
                       {type::TypeId::DECIMAL, type::TypeId::INTEGER}, execution::ast::Builtin::Round2, false);

  BootstrapProcContext(dbc, txn, postgres::PgProc::EXP_PRO_OID, "exp", type::TypeId::DECIMAL, {type::TypeId::DECIMAL},
                       execution::ast::Builtin::Exp, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::ASCII_PRO_OID, "ascii", type::TypeId::INTEGER,
                       {type::TypeId::VARCHAR}, execution::ast::Builtin::ASCII, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::LOWER_PRO_OID, "lower", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR}, execution::ast::Builtin::Lower, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::INITCAP_PRO_OID, "initcap", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR}, execution::ast::Builtin::InitCap, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::POW_PRO_OID, "pow", type::TypeId::DECIMAL, {type::TypeId::DECIMAL},
                       execution::ast::Builtin::Pow, false);

  BootstrapProcContext(dbc, txn, postgres::PgProc::SPLIT_PART_PRO_OID, "split_part", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::VARCHAR, type::TypeId::INTEGER},
                       execution::ast::Builtin::SplitPart, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::CHR_PRO_OID, "chr", type::TypeId::VARCHAR, {type::TypeId::INTEGER},
                       execution::ast::Builtin::Chr, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::CHARLENGTH_PRO_OID, "char_length", type::TypeId::INTEGER,
                       {type::TypeId::VARCHAR}, execution::ast::Builtin::CharLength, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::POSITION_PRO_OID, "position", type::TypeId::INTEGER,
                       {type::TypeId::VARCHAR, type::TypeId::VARCHAR}, execution::ast::Builtin::Position, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::LENGTH_PRO_OID, "length", type::TypeId::INTEGER,
                       {type::TypeId::VARCHAR}, execution::ast::Builtin::Length, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::UPPER_PRO_OID, "upper", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR}, execution::ast::Builtin::Upper, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::VERSION_PRO_OID, "version", type::TypeId::VARCHAR, {},
                       execution::ast::Builtin::Version, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::STARTSWITH_PRO_OID, "starts_with", type::TypeId::BOOLEAN,
                       {type::TypeId::VARCHAR, type::TypeId::VARCHAR}, execution::ast::Builtin::StartsWith, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::SUBSTR_PRO_OID, "substr", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER, type::TypeId::INTEGER},
                       execution::ast::Builtin::Substring, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::REVERSE_PRO_OID, "reverse", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR}, execution::ast::Builtin::Reverse, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::LEFT_PRO_OID, "left", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER}, execution::ast::Builtin::Left, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::RIGHT_PRO_OID, "right", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER}, execution::ast::Builtin::Right, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::REPEAT_PRO_OID, "repeat", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER}, execution::ast::Builtin::Repeat, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::TRIM_PRO_OID, "btrim", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR}, execution::ast::Builtin::Trim, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::TRIM2_PRO_OID, "btrim", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::VARCHAR}, execution::ast::Builtin::Trim2, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::CONCAT_PRO_OID, "concat", type::TypeId::VARCHAR,
                       {type::TypeId::VARIADIC}, execution::ast::Builtin::Concat, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::LPAD_PRO_OID, "lpad", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER, type::TypeId::VARCHAR},
                       execution::ast::Builtin::Lpad, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::LPAD2_PRO_OID, "lpad", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER}, execution::ast::Builtin::Lpad, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::LTRIM2ARG_PRO_OID, "ltrim", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::VARCHAR}, execution::ast::Builtin::Ltrim, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::LTRIM1ARG_PRO_OID, "ltrim", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR}, execution::ast::Builtin::Ltrim, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::RPAD_PRO_OID, "rpad", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER, type::TypeId::VARCHAR},
                       execution::ast::Builtin::Rpad, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::RPAD2_PRO_OID, "rpad", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER}, execution::ast::Builtin::Rpad, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::RTRIM2ARG_PRO_OID, "rtrim", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::VARCHAR}, execution::ast::Builtin::Rtrim, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::RTRIM1ARG_PRO_OID, "rtrim", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR}, execution::ast::Builtin::Rtrim, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::MOD_PRO_OID, "mod", type::TypeId::DECIMAL,
                       {type::TypeId::DECIMAL, type::TypeId::DECIMAL}, execution::ast::Builtin::Mod, false);

  BootstrapProcContext(dbc, txn, postgres::PgProc::INTMOD_PRO_OID, "mod", type::TypeId::INTEGER,
                       {type::TypeId::INTEGER, type::TypeId::INTEGER}, execution::ast::Builtin::Mod, false);

  BootstrapProcContext(dbc, txn, postgres::PgProc::NP_RUNNERS_EMIT_INT_PRO_OID, "NpRunnersEmitInt",
                       type::TypeId::INTEGER,
                       {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER},
                       execution::ast::Builtin::NpRunnersEmitInt, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::NP_RUNNERS_EMIT_REAL_PRO_OID, "NpRunnersEmitReal",
                       type::TypeId::DECIMAL,
                       {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER},
                       execution::ast::Builtin::NpRunnersEmitReal, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::NP_RUNNERS_DUMMY_INT_PRO_OID, "NpRunnersDummyInt",
                       type::TypeId::INTEGER, {}, execution::ast::Builtin::NpRunnersDummyInt, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::NP_RUNNERS_DUMMY_REAL_PRO_OID, "NpRunnersDummyReal",
                       type::TypeId::DECIMAL, {}, execution::ast::Builtin::NpRunnersDummyReal, true);

  BootstrapProcContext(dbc, txn, postgres::PgProc::DATE_PART_PRO_OID, "date_part", type::TypeId::INTEGER,
                       {type::TypeId::DATE, type::TypeId::INTEGER}, execution::ast::Builtin::DatePart, false);
}

}  // namespace noisepage::catalog::postgres
