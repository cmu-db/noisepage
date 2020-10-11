#include "execution/compiler/operator/index_create_translator.h"

#include "catalog/catalog_accessor.h"
#include "execution/ast/context.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/work_context.h"
#include "execution/sql/ddl_executors.h"
#include "execution/sql/table_vector_iterator.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression_util.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace terrier::execution::compiler {

IndexCreateTranslator::IndexCreateTranslator(const planner::CreateIndexPlanNode &plan,
                                             CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::CREATE_INDEX),
      codegen_(compilation_context->GetCodeGen()),
      tvi_var_(codegen_->MakeFreshIdentifier("tvi")),
      vpi_var_(codegen_->MakeFreshIdentifier("vpi")),
      slot_var_(codegen_->MakeFreshIdentifier("slot")),
      table_oid_(GetPlanAs<planner::CreateIndexPlanNode>().GetTableOid()),
      table_schema_(codegen_->GetCatalogAccessor()->GetSchema(table_oid_)),
      all_oids_(AllColOids(table_schema_)),
      index_oid_(
          codegen_->GetCatalogAccessor()->GetIndexOid(GetPlanAs<planner::CreateIndexPlanNode>().GetIndexName())) {
  const auto &index_schema = codegen_->GetCatalogAccessor()->GetIndexSchema(index_oid_);
  for (const auto &index_col : index_schema.GetColumns()) {
    compilation_context->Prepare(*index_col.StoredExpression());
  }
  pipeline->RegisterSource(this, Pipeline::Parallelism::Parallel);

  // col_oids is a global array
  ast::Expr *arr_type = codegen_->ArrayType(all_oids_.size(), ast::BuiltinType::Kind::Uint32);
  global_col_oids_ = compilation_context->GetQueryState()->DeclareStateEntry(codegen_, "global_col_oids", arr_type);
  // storage interface is local to pipeline
  ast::Expr *storage_interface_type = codegen_->BuiltinType(ast::BuiltinType::StorageInterface);
  local_storage_interface_ = pipeline->DeclarePipelineStateEntry("local_storage_interface", storage_interface_type);
  // index pr is local to pipeline
  ast::Expr *index_pr_type = codegen_->BuiltinType(ast::BuiltinType::ProjectedRow);
  local_index_pr_ = pipeline->DeclarePipelineStateEntry("local_index_pr", codegen_->PointerType(index_pr_type));
  // tuple slot is local to pipeline
  // TODO(wuwenw): do we really need a local copy of tuple slot?
  ast::Expr *tuple_slot_type = codegen_->BuiltinType(ast::BuiltinType::TupleSlot);
  local_tuple_slot_ = pipeline->DeclarePipelineStateEntry("local_tuple_slot", tuple_slot_type);

  num_inserts_ = CounterDeclare("num_inserts", pipeline);

  if (GetPipeline()->IsParallel() && IsPipelineMetricsEnabled()) {
    parallel_build_post_hook_fn_ =
        GetCodeGen()->MakeFreshIdentifier(GetPipeline()->CreatePipelineFunctionName("PostHook"));
  }
}

void IndexCreateTranslator::InitializeCounters(const Pipeline &pipeline, FunctionBuilder *function) const {
  CounterSet(function, num_inserts_, 0);
}

void IndexCreateTranslator::RecordCounters(const Pipeline &pipeline, FunctionBuilder *function) const {
  FeatureRecord(function, brain::ExecutionOperatingUnitType::CREATE_INDEX,
                brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, pipeline, CounterVal(num_inserts_));
  FeatureRecord(function, brain::ExecutionOperatingUnitType::CREATE_INDEX,
                brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, pipeline, CounterVal(num_inserts_));
}

void IndexCreateTranslator::InitializeStorageInterface(FunctionBuilder *function,
                                                       ast::Expr *storage_interface_ptr) const {
  // @storageInterfaceInit(&local_storage_interface, execCtx, table_oid, global_col_oids, false)
  ast::Expr *table_oid_expr = codegen_->Const64(static_cast<int64_t>(table_oid_.UnderlyingValue()));
  ast::Expr *col_oids_expr = global_col_oids_.Get(codegen_);
  ast::Expr *need_indexes_expr = codegen_->ConstBool(false);

  std::vector<ast::Expr *> args{storage_interface_ptr, GetExecutionContext(), table_oid_expr, col_oids_expr,
                                need_indexes_expr};

  function->Append(codegen_->CallBuiltin(ast::Builtin::StorageInterfaceInit, args));
}

void IndexCreateTranslator::TearDownStorageInterface(FunctionBuilder *function,
                                                     ast::Expr *storage_interface_ptr) const {
  // Call @storageInterfaceFree(&local_storage_interface)
  function->Append(codegen_->CallBuiltin(ast::Builtin::StorageInterfaceFree, {storage_interface_ptr}));
}

void IndexCreateTranslator::InitializeQueryState(FunctionBuilder *function) const {
  // Set up global col oid array
  SetGlobalOids(function, global_col_oids_.Get(codegen_));
}

void IndexCreateTranslator::InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  // Thread local member
  InitializeStorageInterface(function, local_storage_interface_.GetPtr(codegen_));
  DeclareIndexPR(function);

  if (pipeline.IsParallel() && IsPipelineMetricsEnabled()) {
    pipeline.DeclareTLSDependentFunction(GenerateEndHookFunction());
  }
}

void IndexCreateTranslator::TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  TearDownStorageInterface(function, local_storage_interface_.GetPtr(codegen_));
}

util::RegionVector<ast::FieldDecl *> IndexCreateTranslator::GetWorkerParams() const {
  // Parameters for the scanner
  auto *codegen = GetCodeGen();
  auto *tvi_type = codegen->PointerType(ast::BuiltinType::TableVectorIterator);
  return codegen->MakeFieldList({codegen->MakeField(tvi_var_, tvi_type)});
}

void IndexCreateTranslator::LaunchWork(FunctionBuilder *function, ast::Identifier work_func) const {
  auto *exec_ctx = GetExecutionContext();
  auto *codegen = GetCodeGen();
  if (IsPipelineMetricsEnabled()) {
    auto num_hooks = static_cast<uint32_t>(sql::TableVectorIterator::HookOffsets::NUM_HOOKS);
    auto post = static_cast<uint32_t>(sql::TableVectorIterator::HookOffsets::EndHook);
    function->Append(codegen->ExecCtxInitHooks(exec_ctx, num_hooks));
    function->Append(codegen->ExecCtxRegisterHook(exec_ctx, post, parallel_build_post_hook_fn_));
  }

  function->Append(GetCodeGen()->IterateTableParallel(table_oid_, global_col_oids_.Get(codegen), GetQueryStatePtr(),
                                                      exec_ctx, work_func));

  if (IsPipelineMetricsEnabled()) {
    function->Append(codegen->ExecCtxClearHooks(exec_ctx));
  }
}

void IndexCreateTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  const bool declare_local_tvi = !GetPipeline()->IsParallel();
  if (declare_local_tvi) {
    DeclareTVI(function);
  }
  // Scan it.
  ScanTable(context, function);

  // Close TVI, if need be.
  if (declare_local_tvi) {
    function->Append(codegen_->TableIterClose(codegen_->MakeExpr(tvi_var_)));
    if (IsPipelineMetricsEnabled()) {
      auto *codegen = GetCodeGen();

      // Get Memory Use
      auto *get_mem = codegen->CallBuiltin(ast::Builtin::StorageInterfaceGetIndexHeapSize,
                                           {local_storage_interface_.GetPtr(codegen_)});
      auto *record =
          codegen->CallBuiltin(ast::Builtin::ExecutionContextSetMemoryUseOverride, {GetExecutionContext(), get_mem});
      function->Append(codegen->MakeStmt(record));
      RecordCounters(*GetPipeline(), function);
    }
  } else if (IsPipelineMetricsEnabled()) {
    // For parallel, just record 0 --- for the memory use.
    // The model should be able to identify that for non-zero concurrent, memory = 0
    auto *zero = codegen_->Const32(0);
    auto *record =
        codegen_->CallBuiltin(ast::Builtin::ExecutionContextSetMemoryUseOverride, {GetExecutionContext(), zero});
    function->Append(codegen_->MakeStmt(record));
  }
}

void IndexCreateTranslator::SetGlobalOids(FunctionBuilder *function, ast::Expr *global_col_oids) const {
  for (uint64_t i = 0; i < all_oids_.size(); i++) {
    // col_oids_var_[i] = col_oid
    ast::Expr *lhs = codegen_->GetAstContext()->GetNodeFactory()->NewIndexExpr(codegen_->GetPosition(), global_col_oids,
                                                                               codegen_->Const64(i));
    ast::Expr *rhs = codegen_->Const32(all_oids_[i].UnderlyingValue());
    function->Append(codegen_->Assign(lhs, rhs));
  }
}

void IndexCreateTranslator::DeclareIndexPR(FunctionBuilder *function) const {
  // var local_index_pr = @getIndexPR(&local_storage_interface, index_oid)
  std::vector<ast::Expr *> pr_call_args{local_storage_interface_.GetPtr(codegen_),
                                        codegen_->Const32(index_oid_.UnderlyingValue())};
  auto get_index_pr_call = codegen_->CallBuiltin(ast::Builtin::GetIndexPR, pr_call_args);
  function->Append(codegen_->Assign(local_index_pr_.Get(codegen_), get_index_pr_call));
}

void IndexCreateTranslator::DeclareTVI(FunctionBuilder *function) const {
  auto codegen = GetCodeGen();
  // var tviBase: TableVectorIterator
  // var tvi = &tviBase
  auto tvi_base = codegen->MakeFreshIdentifier("tviBase");
  function->Append(codegen->DeclareVarNoInit(tvi_base, ast::BuiltinType::TableVectorIterator));
  function->Append(codegen->DeclareVarWithInit(tvi_var_, codegen->AddressOf(tvi_base)));
  // @tableIterInit(tvi, exec_ctx, table_oid, global_col_oids)
  ast::Expr *table_iter_init = codegen_->CallBuiltin(
      ast::Builtin::TableIterInit, {codegen->MakeExpr(tvi_var_), GetExecutionContext(),
                                    codegen_->Const32(table_oid_.UnderlyingValue()), global_col_oids_.Get(codegen_)});
  table_iter_init->SetType(ast::BuiltinType::Get(codegen_->GetAstContext().Get(), ast::BuiltinType::Nil));
  function->Append(table_iter_init);
}

std::vector<catalog::col_oid_t> IndexCreateTranslator::AllColOids(const catalog::Schema &table_schema) const {
  std::vector<catalog::col_oid_t> oids;
  for (const auto &col : table_schema.GetColumns()) {
    oids.emplace_back(col.Oid());
  }
  return oids;
}

void IndexCreateTranslator::ScanTable(WorkContext *ctx, FunctionBuilder *function) const {
  // for (@tableIterAdvance(tvi))
  Loop tvi_loop(function, codegen_->TableIterAdvance(codegen_->MakeExpr(tvi_var_)));
  {
    // var vpi = @tableIterGetVPI(tvi)
    auto vpi = codegen_->MakeExpr(vpi_var_);
    function->Append(codegen_->DeclareVarWithInit(vpi_var_, codegen_->TableIterGetVPI(codegen_->MakeExpr(tvi_var_))));
    ScanVPI(ctx, function, vpi);
  }
  tvi_loop.EndLoop();
}

void IndexCreateTranslator::ScanVPI(WorkContext *ctx, FunctionBuilder *function, ast::Expr *vpi) const {
  auto gen_vpi_loop = [&](bool is_filtered) {
    Loop vpi_loop(function, nullptr, codegen_->VPIHasNext(vpi, is_filtered),
                  codegen_->MakeStmt(codegen_->VPIAdvance(vpi, is_filtered)));
    {
      // var slot = @tableIterGetSlot(vpi)
      auto make_slot = codegen_->CallBuiltin(ast::Builtin::VPIGetSlot, {codegen_->MakeExpr(vpi_var_)});
      auto assign = codegen_->Assign(local_tuple_slot_.Get(codegen_), make_slot);
      function->Append(assign);
      IndexInsert(ctx, function);
      // We expect create index to be the end of a pipeline, so no need to push to parent
      CounterAdd(function, num_inserts_, 1);
    }
    vpi_loop.EndLoop();
  };
  gen_vpi_loop(false);
}

void IndexCreateTranslator::IndexInsert(WorkContext *ctx, FunctionBuilder *function) const {
  const auto &index = codegen_->GetCatalogAccessor()->GetIndex(index_oid_);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = codegen_->GetCatalogAccessor()->GetIndexSchema(index_oid_);
  auto *index_pr_expr = local_index_pr_.Get(codegen_);

  std::unordered_map<catalog::col_oid_t, uint16_t> oid_offset;
  for (uint16_t i = 0; i < all_oids_.size(); i++) {
    oid_offset[all_oids_[i]] = i;
  }

  for (const auto &index_col : index_schema.GetColumns()) {
    auto stored_expr = index_col.StoredExpression();
    TERRIER_ASSERT(stored_expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE,
                   "CREATE INDEX supported on base columns only");

    // col_expr comes from the base table so we need to use TableSchema to get the correct scan_offset.
    // col_expr = @VPIGet(vpi_var_, attr_sql_type, true, oid)
    auto cve = stored_expr.CastManagedPointerTo<const parser::ColumnValueExpression>();
    TERRIER_ASSERT(cve->GetColumnOid() != catalog::INVALID_COLUMN_OID, "CREATE INDEX column oid not bound");
    TERRIER_ASSERT(oid_offset.find(cve->GetColumnOid()) != oid_offset.end(), "CREATE INDEX missing column scan");
    auto &tbl_col = table_schema_.GetColumn(cve->GetColumnOid());
    auto sql_type = sql::GetTypeId(tbl_col.Type());
    auto scan_offset = oid_offset[cve->GetColumnOid()];
    const auto &col_expr = codegen_->VPIGet(codegen_->MakeExpr(vpi_var_), sql_type, tbl_col.Nullable(), scan_offset);

    // @prSet(insert_index_pr, attr_type, attr_idx, nullable, attr_index, col_expr, false)
    uint16_t attr_offset = index_pm.at(index_col.Oid());
    type::TypeId attr_type = index_col.Type();
    bool nullable = index_col.Nullable();
    auto *set_key_call = codegen_->PRSet(index_pr_expr, attr_type, nullable, attr_offset, col_expr, false);
    function->Append(codegen_->MakeStmt(set_key_call));
  }

  // if (!@IndexInsertWithSlot(&local_storage_interface, &local_tuple_slot, unique)) { Abort(); }
  auto *index_insert_call = codegen_->CallBuiltin(
      ast::Builtin::IndexInsertWithSlot, {local_storage_interface_.GetPtr(codegen_), local_tuple_slot_.GetPtr(codegen_),
                                          codegen_->ConstBool(index_schema.Unique())});
  auto *cond = codegen_->UnaryOp(parsing::Token::Type::BANG, index_insert_call);
  If success(function, cond);
  { function->Append(codegen_->AbortTxn(GetExecutionContext())); }
  success.EndIf();
}

ast::FunctionDecl *IndexCreateTranslator::GenerateEndHookFunction() const {
  auto *codegen = GetCodeGen();
  auto *pipeline = GetPipeline();
  auto params = GetHookParams(*pipeline, nullptr, nullptr);

  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Nil);
  FunctionBuilder builder(codegen, parallel_build_post_hook_fn_, std::move(params), ret_type);
  {
    auto *exec_ctx = GetExecutionContext();
    pipeline->InjectStartResourceTracker(&builder, true);

    auto num_tuples = codegen->MakeFreshIdentifier("num_tuples");
    auto *idx_size = codegen->CallBuiltin(ast::Builtin::IndexGetSize, {local_storage_interface_.GetPtr(codegen_)});
    builder.Append(codegen->DeclareVarWithInit(num_tuples, idx_size));

    FeatureRecord(&builder, brain::ExecutionOperatingUnitType::CREATE_INDEX_MAIN,
                  brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, *pipeline, codegen->MakeExpr(num_tuples));
    FeatureRecord(&builder, brain::ExecutionOperatingUnitType::CREATE_INDEX_MAIN,
                  brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, *pipeline, codegen->MakeExpr(num_tuples));

    auto heap = codegen->MakeFreshIdentifier("heap_size");
    auto *heap_size = codegen->CallBuiltin(ast::Builtin::StorageInterfaceGetIndexHeapSize,
                                           {local_storage_interface_.GetPtr(codegen_)});
    builder.Append(codegen->DeclareVarWithInit(heap, heap_size));
    builder.Append(
        codegen->CallBuiltin(ast::Builtin::ExecutionContextSetMemoryUseOverride, {exec_ctx, codegen->MakeExpr(heap)}));

    // End Tracker
    pipeline->InjectEndResourceTracker(&builder, pipeline->GetQueryId(), true);
  }
  return builder.Finish();
}

}  // namespace terrier::execution::compiler
