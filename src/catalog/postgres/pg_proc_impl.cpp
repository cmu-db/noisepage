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

  const std::vector<col_oid_t> set_pg_proc_ptr_oids{PgProc::PRO_CTX_PTR.oid_};
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

  const std::vector<col_oid_t> pg_proc_contexts{PgProc::PRO_CTX_PTR.oid_};
  const auto pci = procs_->InitializerForProjectedColumns(pg_proc_contexts, DatabaseCatalog::TEARDOWN_MAX_TUPLES);
  byte *buffer = common::AllocationUtil::AllocateAligned(pci.ProjectedColumnsSize());
  auto pc = pci.Initialize(buffer);

  auto ctxs = reinterpret_cast<execution::functions::FunctionContext **>(pc->ColumnStart(0));

  // Collect all the non-nullptr contexts in pg_proc.
  auto table_iter = procs_->begin();
  while (table_iter != procs_->end()) {
    procs_->Scan(txn, &table_iter, pc);

    for (uint32_t i = 0; i < pc->NumTuples(); i++) {
      if (ctxs[i] == nullptr) {
        continue;
      }
      func_contexts.emplace_back(ctxs[i]);
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
                                 const namespace_oid_t procns, const type_oid_t variadic_type,
                                 const std::vector<std::string> &args, const std::vector<type_oid_t> &arg_types,
                                 const std::vector<type_oid_t> &all_arg_types,
                                 const std::vector<PgProc::ArgMode> &arg_modes, const type_oid_t rettype,
                                 const std::string &src, const bool is_aggregate) {
  NOISEPAGE_ASSERT(args.size() < UINT16_MAX, "Number of arguments must fit in a SMALLINT");
  NOISEPAGE_ASSERT(args.size() == arg_types.size(), "Every input arg needs a type.");
  NOISEPAGE_ASSERT(
      arg_modes.empty() || (!(std::all_of(arg_modes.cbegin(), arg_modes.cend(),
                                          [](PgProc::ArgMode mode) { return mode == PgProc::ArgMode::IN; })) &&
                            arg_modes.size() >= args.size()),
      "argmodes should be empty unless there are modes other than IN, in which case arg_modes must be at "
      "least equal to the size of args.");
  NOISEPAGE_ASSERT(all_arg_types.size() == arg_modes.size(),
                   "If a mode is defined for each arg, then a type must be defined for each arg as well.");

  const auto name_varlen = storage::StorageUtil::CreateVarlen(procname);

  auto *const redo = txn->StageWrite(db_oid_, PgProc::PRO_TABLE_OID, pg_proc_all_cols_pri_);
  const auto delta = common::ManagedPointer(redo->Delta());
  const auto &pm = pg_proc_all_cols_prm_;

  // Prepare the PR for insertion.
  {
    PgProc::PROOID.Set(delta, pm, oid);                 // Procedure OID.
    PgProc::PRONAME.Set(delta, pm, name_varlen);        // Procedure name.
    PgProc::PRONAMESPACE.Set(delta, pm, procns);        // Namespace of procedure.
    PgProc::PROLANG.Set(delta, pm, language_oid);       // Language for procedure.
    PgProc::PROCOST.Set(delta, pm, 0);                  // Estimated cost per row returned. // TODO(Matt): unused field?
    PgProc::PROROWS.Set(delta, pm, 0);                  // Estimated number of result rows. // TODO(Matt): unused field?
    PgProc::PROVARIADIC.Set(delta, pm, variadic_type);  // Type of the variadic argument, or 0
    PgProc::PROISAGG.Set(delta, pm, is_aggregate);      // Whether aggregate or not. // TODO(Matt): unused field?
    PgProc::PROISWINDOW.Set(delta, pm, false);          // Not window. // TODO(Matt): unused field?
    PgProc::PROISSTRICT.Set(delta, pm, true);           // Strict. // TODO(Matt): unused field?
    PgProc::PRORETSET.Set(delta, pm, false);            // Doesn't return a set. // TODO(Matt): unused field?
    PgProc::PROVOLATILE.Set(delta, pm,
                            static_cast<char>(PgProc::ProVolatile::STABLE));  // Stable. // TODO(Matt): unused field?
    PgProc::PRONARGS.Set(delta, pm, static_cast<uint16_t>(args.size()));      // Num args.
    PgProc::PRONARGDEFAULTS.Set(delta, pm, 0);   // Assume no default args. // TODO(Matt): unused field?
    PgProc::PRORETTYPE.Set(delta, pm, rettype);  // Return type.

    if (arg_types.empty()) {
      PgProc::PROARGTYPES.SetNull(delta, pm);
    } else {
      const auto arg_types_varlen = storage::StorageUtil::CreateVarlen(arg_types);
      PgProc::PROARGTYPES.Set(delta, pm, arg_types_varlen);  // Arg types.
    }

    if (all_arg_types.empty()) {
      PgProc::PROALLARGTYPES.SetNull(delta, pm);
    } else {
      const auto all_arg_types_varlen = storage::StorageUtil::CreateVarlen(all_arg_types);
      PgProc::PROALLARGTYPES.Set(delta, pm, all_arg_types_varlen);
    }

    if (arg_modes.empty()) {
      PgProc::PROARGMODES.SetNull(delta, pm);
    } else {
      const auto arg_modes_varlen = storage::StorageUtil::CreateVarlen(arg_modes);
      PgProc::PROARGMODES.Set(delta, pm, arg_modes_varlen);
    }

    delta->SetNull(pm.at(PgProc::PROARGDEFAULTS.oid_));  // Assume no default args. // TODO(Matt): unused field?

    if (args.empty()) {
      PgProc::PROARGNAMES.SetNull(delta, pm);
    } else {
      const auto arg_names_varlen = storage::StorageUtil::CreateVarlen(args);
      PgProc::PROARGNAMES.Set(delta, pm, arg_names_varlen);
    }

    const auto src_varlen = storage::StorageUtil::CreateVarlen(src);
    PgProc::PROSRC.Set(delta, pm, src_varlen);  // Source code.

    delta->SetNull(pm.at(
        PgProc::PROCONFIG.oid_));  // Assume no procedure local run-time configuration. // TODO(Matt): unused field?
    delta->SetNull(pm.at(PgProc::PRO_CTX_PTR.oid_));  // Pointer to procedure context.
  }

  const auto tuple_slot = procs_->Insert(txn, redo);

  const auto &oid_pri = procs_oid_index_->GetProjectedRowInitializer();
  const auto &name_pri = procs_name_index_->GetProjectedRowInitializer();
  byte *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());

  // Insert into pg_proc_name_index.
  {
    auto name_pr = name_pri.InitializeRow(buffer);
    const auto &name_map = procs_name_index_->GetKeyOidToOffsetMap();
    name_pr->Set<namespace_oid_t, false>(name_map.at(indexkeycol_oid_t(1)), procns, false);
    name_pr->Set<storage::VarlenEntry, false>(name_map.at(indexkeycol_oid_t(2)), name_varlen, false);

    if (auto result = procs_name_index_->Insert(txn, *name_pr, tuple_slot); !result) {
      return false;
    }
  }

  // Insert into pg_proc_oid_index.
  {
    auto oid_pr = oid_pri.InitializeRow(buffer);
    oid_pr->Set<proc_oid_t, false>(0, oid, false);
    bool UNUSED_ATTRIBUTE result = procs_oid_index_->InsertUnique(txn, *oid_pr, tuple_slot);
    NOISEPAGE_ASSERT(result, "Oid insertion should be unique");
  }

  return true;
}

bool PgProcImpl::DropProcedure(const common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc) {
  NOISEPAGE_ASSERT(proc != INVALID_PROC_OID, "DropProcedure called with invalid procedure OID");

  const auto &name_pri = procs_name_index_->GetProjectedRowInitializer();
  const auto &oid_pri = procs_oid_index_->GetProjectedRowInitializer();
  byte *const buffer = common::AllocationUtil::AllocateAligned(pg_proc_all_cols_pri_.ProjectedRowSize());

  auto oid_pr = oid_pri.InitializeRow(buffer);
  oid_pr->Set<proc_oid_t, false>(0, proc, false);

  // Look for the procedure in pg_proc_oid_index.
  std::vector<storage::TupleSlot> results;
  {
    procs_oid_index_->ScanKey(*txn, *oid_pr, &results);
    if (results.empty()) {
      delete[] buffer;
      return false;
    }
  }

  NOISEPAGE_ASSERT(results.size() == 1, "More than one non-unique result found in unique index.");

  auto to_delete_slot = results[0];

  // Delete from pg_proc.
  {
    txn->StageDelete(db_oid_, PgLanguage::LANGUAGE_TABLE_OID, to_delete_slot);
    if (!procs_->Delete(txn, to_delete_slot)) {
      // Someone else has a write-lock. Free the buffer and return false to indicate failure.
      delete[] buffer;
      return false;
    }
  }

  // Delete from pg_proc_oid_index. Reuses oid_pr from the index scan above.
  { procs_oid_index_->Delete(txn, *oid_pr, to_delete_slot); }

  // Look for the procedure in pg_proc.
  auto table_pr = pg_proc_all_cols_pri_.InitializeRow(buffer);
  bool UNUSED_ATTRIBUTE visible = procs_->Select(txn, to_delete_slot, table_pr);

  auto &proc_pm = pg_proc_all_cols_prm_;
  auto name_varlen = *table_pr->Get<storage::VarlenEntry, false>(proc_pm[PgProc::PRONAME.oid_], nullptr);
  auto proc_ns = *table_pr->Get<namespace_oid_t, false>(proc_pm[PgProc::PRONAMESPACE.oid_], nullptr);

  auto *ptr_ptr = reinterpret_cast<void **>(table_pr->AccessWithNullCheck(proc_pm[PgProc::PRO_CTX_PTR.oid_]));
  NOISEPAGE_ASSERT(ptr_ptr != nullptr, "DropProcedure called on an invalid OID or before SetFunctionContext.");
  auto *ctx_ptr = *reinterpret_cast<execution::functions::FunctionContext **>(ptr_ptr);

  // Delete from pg_proc_name_index.
  {
    auto name_pr = name_pri.InitializeRow(buffer);
    const auto &name_map = procs_name_index_->GetKeyOidToOffsetMap();
    name_pr->Set<namespace_oid_t, false>(name_map.at(indexkeycol_oid_t(1)), proc_ns, false);
    name_pr->Set<storage::VarlenEntry, false>(name_map.at(indexkeycol_oid_t(2)), name_varlen, false);
    procs_name_index_->Delete(txn, *name_pr, to_delete_slot);
  }

  // Clean up the procedure context.
  if (ctx_ptr != nullptr) {
    txn->RegisterCommitAction([=](transaction::DeferredActionManager *deferred_action_manager) {
      deferred_action_manager->RegisterDeferredAction(
          [=]() { deferred_action_manager->RegisterDeferredAction([=]() { delete ctx_ptr; }); });
    });
  }

  delete[] buffer;
  return true;
}

bool PgProcImpl::SetProcCtxPtr(common::ManagedPointer<transaction::TransactionContext> txn, const proc_oid_t proc_oid,
                               const execution::functions::FunctionContext *func_context) {
  const auto &oid_pri = procs_oid_index_->GetProjectedRowInitializer();
  auto *const index_buffer = common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize());

  // Look for the procedure in pg_proc_oid_index.
  std::vector<storage::TupleSlot> index_results;
  {
    auto *const key_pr = oid_pri.InitializeRow(index_buffer);
    key_pr->Set<proc_oid_t, false>(0, proc_oid, false);
    procs_oid_index_->ScanKey(*txn, *key_pr, &index_results);
    NOISEPAGE_ASSERT(
        index_results.size() == 1,
        "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function "
        "was called with an oid that doesn't exist in the Catalog, which implies a programmer error. There's no "
        "reasonable code path for this to be called on an oid that isn't present.");
  }

  delete[] index_buffer;

  // Update pg_proc.
  auto *const update_redo = txn->StageWrite(db_oid_, PgProc::PRO_TABLE_OID, pg_proc_ptr_pri_);
  update_redo->Delta()->Set<const execution::functions::FunctionContext *, false>(0, func_context, false);
  update_redo->SetTupleSlot(index_results[0]);
  return procs_->Update(txn, update_redo);
}

common::ManagedPointer<execution::functions::FunctionContext> PgProcImpl::GetProcCtxPtr(
    common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid) {
  const auto &oid_pri = procs_oid_index_->GetProjectedRowInitializer();
  auto *const buffer = common::AllocationUtil::AllocateAligned(pg_proc_ptr_pri_.ProjectedRowSize());

  // Look for the procedure in pg_proc_oid_index.
  std::vector<storage::TupleSlot> index_results;
  {
    auto *const key_pr = oid_pri.InitializeRow(buffer);
    key_pr->Set<proc_oid_t, false>(0, proc_oid, false);
    procs_oid_index_->ScanKey(*txn, *key_pr, &index_results);
    NOISEPAGE_ASSERT(
        index_results.size() == 1,
        "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function "
        "was called with an oid that doesn't exist in the Catalog, which implies a programmer error. There's no"
        "reasonable code path for this to be called on an oid that isn't present.");
  }

  auto *select_pr = pg_proc_ptr_pri_.InitializeRow(buffer);
  const auto result UNUSED_ATTRIBUTE = procs_->Select(txn, index_results[0], select_pr);
  NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");

  auto *ptr_ptr = (reinterpret_cast<void **>(select_pr->AccessWithNullCheck(0)));
  NOISEPAGE_ASSERT(nullptr != ptr_ptr, "GetFunctionContext called on an invalid OID or before SetFunctionContext.");
  execution::functions::FunctionContext *ptr = *reinterpret_cast<execution::functions::FunctionContext **>(ptr_ptr);

  delete[] buffer;
  return common::ManagedPointer<execution::functions::FunctionContext>(ptr);
}

proc_oid_t PgProcImpl::GetProcOid(const common::ManagedPointer<transaction::TransactionContext> txn,
                                  const common::ManagedPointer<DatabaseCatalog> dbc, const namespace_oid_t procns,
                                  const std::string &procname, const std::vector<type_oid_t> &input_arg_types) {
  const auto &name_pri = procs_name_index_->GetProjectedRowInitializer();
  byte *const buffer = common::AllocationUtil::AllocateAligned(pg_proc_all_cols_pri_.ProjectedRowSize());

  // Look for the procedure in pg_proc_name_index.
  std::vector<storage::TupleSlot> results;
  {
    auto name_pr = name_pri.InitializeRow(buffer);
    const auto &name_map = procs_name_index_->GetKeyOidToOffsetMap();

    auto name_varlen = storage::StorageUtil::CreateVarlen(procname);
    name_pr->Set<namespace_oid_t, false>(name_map.at(indexkeycol_oid_t(1)), procns, false);
    name_pr->Set<storage::VarlenEntry, false>(name_map.at(indexkeycol_oid_t(2)), name_varlen, false);
    procs_name_index_->ScanKey(*txn, *name_pr, &results);

    if (name_varlen.NeedReclaim()) {
      delete[] name_varlen.Content();
    }
  }

  proc_oid_t ret = INVALID_PROC_OID;
  std::vector<proc_oid_t> matching_functions;

  if (!results.empty()) {
    const auto &pm = pg_proc_all_cols_prm_;
    auto table_pr = pg_proc_all_cols_pri_.InitializeRow(buffer);

    // Search through the results and check if any match the parsed function by argument types.
    for (const auto &candidate_proc_slot : results) {
      bool UNUSED_ATTRIBUTE visible = procs_->Select(txn, candidate_proc_slot, table_pr);
      NOISEPAGE_ASSERT(visible, "Index scan should have already verified visibility.");

      // "PROARGTYPES ... represents the call signature of the function". Check only input arguments.
      // https://www.postgresql.org/docs/12/catalog-pg-proc.html

      bool match = false;

      const auto candidate_proc_nargs = *(table_pr->Get<uint16_t, false>(pm.at(PgProc::PRONARGS.oid_), nullptr));
      if (candidate_proc_nargs <= input_arg_types.size()) {
        // input has enough arguments to try to match types
        if (candidate_proc_nargs == 0) {
          // function has no args, check the length of the input args
          if (input_arg_types.empty()) {
            // both had an empty argument list
            match = true;
          } else {
            // function has no args, but input arg list is non-empty. Cannot be a match.
            continue;
          }
        } else {
          // Right now we know that both arg lists (function's and input) are non-empty, and that the input args is at
          // least as long as the function's args. Start trying to match types

          // Read the arg types out of the table
          bool null_arg = false;
          const auto *candidate_proc_args =
              table_pr->Get<storage::VarlenEntry, true>(pm.at(PgProc::PROARGTYPES.oid_), &null_arg);
          NOISEPAGE_ASSERT(!null_arg, "This shouldn't be NULL if nargs > 0.");

          const auto candidate_proc_args_vector = candidate_proc_args->DeserializeArray<
              type_oid_t>();  // TODO(Matt): could maybe elide this copy with
                              // VarlenEntry.Content() shenanigans, but then need a bespoke way to compare against input
                              // args in their vector. Previous version of this function constructed a VarlenEntry of
                              // input args to compare, which seems worse.
          if (candidate_proc_args_vector == input_arg_types) {
            // argument signature matches exactly
            match = true;
          } else {
            // not an exact signature match, check if the function is variadic
            const auto candidate_proc_variadic =
                *(table_pr->Get<type_oid_t, false>(pm.at(PgProc::PROVARIADIC.oid_), nullptr));
            if (candidate_proc_variadic != INVALID_TYPE_OID) {
              // it's variadic. match if:
              // 1) function's args are a prefix of input args
              // 2) remaining input args are all the same, and same type as variadic in function signature

              // check condition 1
              bool prefix_match = true;
              for (uint16_t i = 0; i < candidate_proc_nargs && prefix_match; i++) {
                if (candidate_proc_args_vector[i] != input_arg_types[i]) {
                  // type mismatch, bail out
                  prefix_match = false;
                }
              }
              if (prefix_match) {
                // condition 1 satisfied, check condition 2
                NOISEPAGE_ASSERT(candidate_proc_variadic == candidate_proc_args_vector.back(),
                                 "Last argument of function signature should be the variadic.");
                bool variadic_args_match = true;
                for (uint16_t i = candidate_proc_nargs; i < input_arg_types.size() && variadic_args_match; i++) {
                  if (input_arg_types[i] != candidate_proc_variadic) {
                    variadic_args_match = false;
                  }
                }
                if (variadic_args_match) {
                  // we satisfied both conditions described above, this function can match the input arguments
                  match = true;
                }
              }
            }
          }
        }
      }

      if (match) {
        const auto proc_oid = *table_pr->Get<proc_oid_t, false>(pm.at(PgProc::PROOID.oid_), nullptr);
        matching_functions.push_back(proc_oid);
      }
    }
  }

  delete[] buffer;

  if (matching_functions.size() == 1) {
    ret = matching_functions[0];
  } else if (matching_functions.size() > 1) {
    // TODO(WAN): See #1361 for a discussion on whether we can avoid this
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
  const auto INT = dbc->GetTypeOidForType(execution::sql::SqlTypeId::Integer);   // NOLINT
  const auto STR = dbc->GetTypeOidForType(execution::sql::SqlTypeId::Varchar);   // NOLINT
  const auto REAL = dbc->GetTypeOidForType(execution::sql::SqlTypeId::Double);   // NOLINT
  const auto DATE = dbc->GetTypeOidForType(execution::sql::SqlTypeId::Date);     // NOLINT
  const auto BOOL = dbc->GetTypeOidForType(execution::sql::SqlTypeId::Boolean);  // NOLINT

  auto create_fn = [&](const std::string &procname, const type_oid_t variadic_type,
                       const std::vector<std::string> &args, const std::vector<type_oid_t> &arg_types,
                       type_oid_t rettype) {
    std::vector<PgProc::ArgMode> arg_modes;
    std::vector<type_oid_t> all_arg_types;
    if (variadic_type != INVALID_TYPE_OID) {
      all_arg_types = arg_types;
      arg_modes.resize(arg_types.size(), PgProc::ArgMode::IN);  // we dont' support OUT or INOUT args right now
      arg_modes.back() = PgProc::ArgMode::VARIADIC;             // variadic must be the last arg
    }
    CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, procname, PgLanguage::INTERNAL_LANGUAGE_OID,
                    PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, variadic_type, args, arg_types, all_arg_types,
                    arg_modes, rettype, "", false);
  };

  // Math functions.
  create_fn("abs", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("abs", INVALID_TYPE_OID, {"n"}, {INT}, INT);
  create_fn("ceil", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("cbrt", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("exp", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("floor", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("log10", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("log2", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("mod", INVALID_TYPE_OID, {"a", "b"}, {REAL, REAL}, REAL);
  create_fn("mod", INVALID_TYPE_OID, {"a", "b"}, {INT, INT}, INT);
  create_fn("pow", INVALID_TYPE_OID, {"x", "y"}, {REAL, REAL}, REAL);
  create_fn("round", INVALID_TYPE_OID, {"x", "n"}, {REAL, INT}, REAL);
  create_fn("round", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("sqrt", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("truncate", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);

  // Trig functions.
  create_fn("acos", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("asin", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("atan", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("atan2", INVALID_TYPE_OID, {"y", "x"}, {REAL, REAL}, REAL);
  create_fn("cos", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("cosh", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("cot", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("sin", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("sinh", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("tan", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);
  create_fn("tanh", INVALID_TYPE_OID, {"x"}, {REAL}, REAL);

  // String functions.
  create_fn("ascii", INVALID_TYPE_OID, {"s"}, {STR}, INT);
  create_fn("btrim", INVALID_TYPE_OID, {"s"}, {STR}, STR);
  create_fn("btrim", INVALID_TYPE_OID, {"s", "s"}, {STR, STR}, STR);
  create_fn("char_length", INVALID_TYPE_OID, {"s"}, {STR}, INT);
  create_fn("chr", INVALID_TYPE_OID, {"n"}, {INT}, STR);
  create_fn("concat", STR, {"s"}, {STR}, STR);
  create_fn("initcap", INVALID_TYPE_OID, {"s"}, {STR}, STR);
  create_fn("lower", INVALID_TYPE_OID, {"s"}, {STR}, STR);
  create_fn("left", INVALID_TYPE_OID, {"s", "n"}, {STR, INT}, STR);
  create_fn("length", INVALID_TYPE_OID, {"s"}, {STR}, INT);
  create_fn("lpad", INVALID_TYPE_OID, {"s", "len"}, {STR, INT}, STR);
  create_fn("lpad", INVALID_TYPE_OID, {"s", "len", "pad"}, {STR, INT, STR}, STR);
  create_fn("ltrim", INVALID_TYPE_OID, {"s"}, {STR}, STR);
  create_fn("ltrim", INVALID_TYPE_OID, {"s", "chars"}, {STR, STR}, STR);
  create_fn("position", INVALID_TYPE_OID, {"s1", "s2"}, {STR, STR}, INT);
  create_fn("repeat", INVALID_TYPE_OID, {"s", "n"}, {STR, INT}, STR);
  create_fn("reverse", INVALID_TYPE_OID, {"s"}, {STR}, STR);
  create_fn("right", INVALID_TYPE_OID, {"s", "n"}, {STR, INT}, STR);
  create_fn("rpad", INVALID_TYPE_OID, {"s", "len"}, {STR, INT}, STR);
  create_fn("rpad", INVALID_TYPE_OID, {"s", "len", "pad"}, {STR, INT, STR}, STR);
  create_fn("rtrim", INVALID_TYPE_OID, {"s"}, {STR}, STR);
  create_fn("rtrim", INVALID_TYPE_OID, {"s", "chars"}, {STR, STR}, STR);
  create_fn("split_part", INVALID_TYPE_OID, {"s", "delim", "field"}, {STR, STR, INT}, STR);
  create_fn("starts_with", INVALID_TYPE_OID, {"s", "start"}, {STR, STR}, BOOL);
  create_fn("substr", INVALID_TYPE_OID, {"s", "pos", "len"}, {STR, INT, INT}, STR);
  create_fn("upper", INVALID_TYPE_OID, {"s"}, {STR}, STR);

  // Replication.
  create_fn("replication_get_last_txn_id", INVALID_TYPE_OID, {}, {}, INT);

  // Other functions.
  create_fn("date_part", INVALID_TYPE_OID, {"date", "date_part_type"}, {DATE, INT}, INT);
  create_fn("version", INVALID_TYPE_OID, {}, {}, STR);

  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "nprunnersemitint", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, INVALID_TYPE_OID,
                  {"num_tuples", "num_cols", "num_int_cols", "num_real_cols"}, {INT, INT, INT, INT}, {}, {}, INT, "",
                  false);
  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "nprunnersemitreal", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, INVALID_TYPE_OID,
                  {"num_tuples", "num_cols", "num_int_cols", "num_real_cols"}, {INT, INT, INT, INT}, {}, {}, REAL, "",
                  false);
  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "nprunnersdummyint", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, INVALID_TYPE_OID, {}, {}, {}, {}, INT, "", false);
  CreateProcedure(txn, proc_oid_t{dbc->next_oid_++}, "nprunnersdummyreal", PgLanguage::INTERNAL_LANGUAGE_OID,
                  PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, INVALID_TYPE_OID, {}, {}, {}, {}, REAL, "", false);

  BootstrapProcContexts(txn, dbc);
}

void PgProcImpl::BootstrapProcContext(const common::ManagedPointer<transaction::TransactionContext> txn,
                                      const common::ManagedPointer<DatabaseCatalog> dbc, std::string &&func_name,
                                      const execution::sql::SqlTypeId func_ret_type,
                                      std::vector<execution::sql::SqlTypeId> &&arg_types,
                                      const execution::ast::Builtin builtin, const bool is_exec_ctx_required) {
  std::vector<type_oid_t> arg_type_oids;
  arg_type_oids.reserve(arg_types.size());
  for (const auto type : arg_types) {
    arg_type_oids.emplace_back(dbc->GetTypeOidForType(type));
  }
  const proc_oid_t proc_oid =
      GetProcOid(txn, dbc, PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID, func_name, arg_type_oids);
  NOISEPAGE_ASSERT(proc_oid != INVALID_PROC_OID,
                   "GetProcOid returned no results during bootstrap. Something failed earlier.");
  const auto *const func_context = new execution::functions::FunctionContext(
      std::move(func_name), func_ret_type, std::move(arg_types), builtin, is_exec_ctx_required);
  const auto retval UNUSED_ATTRIBUTE = dbc->SetFunctionContext(txn, proc_oid, func_context);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
}

void PgProcImpl::BootstrapProcContexts(const common::ManagedPointer<transaction::TransactionContext> txn,
                                       const common::ManagedPointer<DatabaseCatalog> dbc) {
  constexpr auto REAL = execution::sql::SqlTypeId::Double;  // NOLINT
  constexpr auto INT = execution::sql::SqlTypeId::Integer;  // NOLINT
  constexpr auto VAR = execution::sql::SqlTypeId::Varchar;  // NOLINT

  auto create_fn = [&](std::string &&func_name, execution::sql::SqlTypeId func_ret_type,
                       std::vector<execution::sql::SqlTypeId> &&arg_types, execution::ast::Builtin builtin,
                       bool is_exec_ctx_required) {
    BootstrapProcContext(txn, dbc, std::move(func_name), func_ret_type, std::move(arg_types), builtin,
                         is_exec_ctx_required);
  };

  // Math functions.
  create_fn("abs", REAL, {REAL}, execution::ast::Builtin::Abs, false);
  create_fn("abs", INT, {INT}, execution::ast::Builtin::Abs, false);
  create_fn("ceil", REAL, {REAL}, execution::ast::Builtin::Ceil, false);
  create_fn("cbrt", REAL, {REAL}, execution::ast::Builtin::Cbrt, false);
  create_fn("exp", REAL, {REAL}, execution::ast::Builtin::Exp, true);
  create_fn("floor", REAL, {REAL}, execution::ast::Builtin::Floor, false);
  create_fn("log10", REAL, {REAL}, execution::ast::Builtin::Log10, false);
  create_fn("log2", REAL, {REAL}, execution::ast::Builtin::Log2, false);
  create_fn("mod", REAL, {REAL, REAL}, execution::ast::Builtin::Mod, false);
  create_fn("mod", INT, {INT, INT}, execution::ast::Builtin::Mod, false);
  create_fn("pow", REAL, {REAL, REAL}, execution::ast::Builtin::Pow, false);
  create_fn("round", REAL, {REAL}, execution::ast::Builtin::Round, false);
  create_fn("round", REAL, {REAL, INT}, execution::ast::Builtin::Round2, false);
  create_fn("sqrt", REAL, {REAL}, execution::ast::Builtin::Sqrt, false);
  create_fn("truncate", REAL, {REAL}, execution::ast::Builtin::Truncate, false);

  // Trig functions.
  create_fn("acos", REAL, {REAL}, execution::ast::Builtin::ACos, false);
  create_fn("asin", REAL, {REAL}, execution::ast::Builtin::ASin, false);
  create_fn("atan", REAL, {REAL}, execution::ast::Builtin::ATan, false);
  create_fn("atan2", REAL, {REAL, REAL}, execution::ast::Builtin::ATan2, false);
  create_fn("cos", REAL, {REAL}, execution::ast::Builtin::Cos, false);
  create_fn("cosh", REAL, {REAL}, execution::ast::Builtin::Cosh, false);
  create_fn("cot", REAL, {REAL}, execution::ast::Builtin::Cot, false);
  create_fn("sin", REAL, {REAL}, execution::ast::Builtin::Sin, false);
  create_fn("sinh", REAL, {REAL}, execution::ast::Builtin::Sinh, false);
  create_fn("tan", REAL, {REAL}, execution::ast::Builtin::Tan, false);
  create_fn("tanh", REAL, {REAL}, execution::ast::Builtin::Tanh, false);

  // String functions.
  create_fn("ascii", INT, {VAR}, execution::ast::Builtin::ASCII, true);
  create_fn("btrim", VAR, {VAR}, execution::ast::Builtin::Trim, true);
  create_fn("btrim", VAR, {VAR, VAR}, execution::ast::Builtin::Trim2, true);
  create_fn("concat", VAR, {VAR}, execution::ast::Builtin::Concat, true);
  create_fn("char_length", INT, {VAR}, execution::ast::Builtin::CharLength, true);
  create_fn("chr", VAR, {INT}, execution::ast::Builtin::Chr, true);
  create_fn("initcap", VAR, {VAR}, execution::ast::Builtin::InitCap, true);
  create_fn("lower", VAR, {VAR}, execution::ast::Builtin::Lower, true);
  create_fn("left", VAR, {VAR, INT}, execution::ast::Builtin::Left, true);
  create_fn("length", INT, {VAR}, execution::ast::Builtin::Length, true);
  create_fn("lpad", VAR, {VAR, INT, VAR}, execution::ast::Builtin::Lpad, true);
  create_fn("lpad", VAR, {VAR, INT}, execution::ast::Builtin::Lpad, true);
  create_fn("ltrim", VAR, {VAR, VAR}, execution::ast::Builtin::Ltrim, true);
  create_fn("ltrim", VAR, {VAR}, execution::ast::Builtin::Ltrim, true);
  create_fn("position", INT, {VAR, VAR}, execution::ast::Builtin::Position, true);
  create_fn("repeat", VAR, {VAR, INT}, execution::ast::Builtin::Repeat, true);
  create_fn("right", VAR, {VAR, INT}, execution::ast::Builtin::Right, true);
  create_fn("reverse", VAR, {VAR}, execution::ast::Builtin::Reverse, true);
  create_fn("rpad", VAR, {VAR, INT, VAR}, execution::ast::Builtin::Rpad, true);
  create_fn("rpad", VAR, {VAR, INT}, execution::ast::Builtin::Rpad, true);
  create_fn("rtrim", VAR, {VAR, VAR}, execution::ast::Builtin::Rtrim, true);
  create_fn("rtrim", VAR, {VAR}, execution::ast::Builtin::Rtrim, true);
  create_fn("split_part", VAR, {VAR, VAR, INT}, execution::ast::Builtin::SplitPart, true);
  create_fn("starts_with", execution::sql::SqlTypeId::Boolean, {VAR, VAR}, execution::ast::Builtin::StartsWith, true);
  create_fn("substr", VAR, {VAR, INT, INT}, execution::ast::Builtin::Substring, true);
  create_fn("upper", VAR, {VAR}, execution::ast::Builtin::Upper, true);

  // Replication.
  create_fn("replication_get_last_txn_id", INT, {}, execution::ast::Builtin::ReplicationGetLastTransactionId, true);

  // Other functions.
  create_fn("date_part", INT, {execution::sql::SqlTypeId::Date, INT}, execution::ast::Builtin::DatePart, false);
  create_fn("version", VAR, {}, execution::ast::Builtin::Version, true);

  create_fn("nprunnersemitint", INT, {INT, INT, INT, INT}, execution::ast::Builtin::NpRunnersEmitInt, true);
  create_fn("nprunnersemitreal", REAL, {INT, INT, INT, INT}, execution::ast::Builtin::NpRunnersEmitReal, true);
  create_fn("nprunnersdummyint", INT, {}, execution::ast::Builtin::NpRunnersDummyInt, true);
  create_fn("nprunnersdummyreal", REAL, {}, execution::ast::Builtin::NpRunnersDummyReal, true);
}

}  // namespace noisepage::catalog::postgres
