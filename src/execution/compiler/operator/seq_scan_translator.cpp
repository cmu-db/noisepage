#include "execution/compiler/operator/seq_scan_translator.h"

#include "catalog/catalog_accessor.h"
#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression_util.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "storage/sql_table.h"

namespace noisepage::execution::compiler {

SeqScanTranslator::SeqScanTranslator(const planner::SeqScanPlanNode &plan, CompilationContext *compilation_context,
                                     Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, selfdriving::ExecutionOperatingUnitType::SEQ_SCAN),
      tvi_var_(GetCodeGen()->MakeFreshIdentifier("tvi")),
      vpi_var_(GetCodeGen()->MakeFreshIdentifier("vpi")),
      col_oids_var_(GetCodeGen()->MakeFreshIdentifier("col_oids")),
      slot_var_(GetCodeGen()->MakeFreshIdentifier("slot")),
      col_oids_(MakeInputOids(
          GetCodeGen()->GetCatalogAccessor()->GetSchema(GetPlanAs<planner::SeqScanPlanNode>().GetTableOid()),
          GetPlanAs<planner::SeqScanPlanNode>())) {
  pipeline->RegisterSource(this, Pipeline::Parallelism::Parallel);
  // If there's a predicate, prepare the expression and register a filter manager.
  if (HasPredicate()) {
    compilation_context->Prepare(*plan.GetScanPredicate());

    ast::Expr *fm_type = GetCodeGen()->BuiltinType(ast::BuiltinType::FilterManager);
    local_filter_manager_ = pipeline->DeclarePipelineStateEntry("filterManager", fm_type);
  }

  num_scans_ = CounterDeclare("num_scans", pipeline);
}

bool SeqScanTranslator::HasPredicate() const {
  return GetPlanAs<planner::SeqScanPlanNode>().GetScanPredicate() != nullptr;
}

catalog::table_oid_t SeqScanTranslator::GetTableOid() const {
  return GetPlanAs<planner::SeqScanPlanNode>().GetTableOid();
}

void SeqScanTranslator::GenerateGenericTerm(FunctionBuilder *function,
                                            common::ManagedPointer<parser::AbstractExpression> term,
                                            ast::Expr *vector_proj, ast::Expr *tid_list) {
  auto *codegen = GetCodeGen();

  // var vpiBase: VectorProjectionIterator
  // var vpi = &vpiBase
  auto vpi_base = codegen->MakeFreshIdentifier("vpiBase");
  function->Append(codegen->DeclareVarNoInit(vpi_base, ast::BuiltinType::VectorProjectionIterator));
  function->Append(codegen->DeclareVarWithInit(vpi_var_, codegen->AddressOf(codegen->MakeExpr(vpi_base))));

  // @vpiInit()
  auto vpi = codegen->MakeExpr(vpi_var_);
  function->Append(codegen->VPIInit(vpi, vector_proj, tid_list));

  auto gen_body = [&](const bool is_filtered) {
    Loop vpi_loop(function, nullptr,                                          // No init;
                  codegen->VPIHasNext(vpi, is_filtered),                      // @vpiHasNext[Filtered]();
                  codegen->MakeStmt(codegen->VPIAdvance(vpi, is_filtered)));  // @vpiAdvance[Filtered]()
    {
      WorkContext context(GetCompilationContext(), *GetPipeline());
      auto cond_translator = GetCompilationContext()->LookupTranslator(*term);
      auto match = cond_translator->DeriveValue(&context, this);
      function->Append(codegen->VPIMatch(vpi, match));
    }
    vpi_loop.EndLoop();
  };

  If check_filtered(function, codegen->VPIIsFiltered(vpi));
  { gen_body(true); }
  check_filtered.Else();
  { gen_body(false); }
  check_filtered.EndIf();
}

void SeqScanTranslator::GenerateFilterClauseFunctions(util::RegionVector<ast::FunctionDecl *> *decls,
                                                      common::ManagedPointer<parser::AbstractExpression> predicate,
                                                      std::vector<ast::Identifier> *curr_clause,
                                                      bool seen_conjunction) {
  // The top-most disjunctions in the tree form separate clauses in the filter manager.
  // For a SQL statement like "SELECT * FROM tbl WHERE a=1 OR b=2 OR c=3;", its predicate is an AbstractExpression
  // with type CONJUNCTION_OR, and it has 3 children expression with type COMPARE_EQUAL. For each child, this function
  // is recursively called to append the predicate to the filter. seen_conjunction is used to indicate if DNF is
  // violated, if so (such as an OR nested within an AND), then we treat it as a generic term.
  if (!seen_conjunction && predicate->GetExpressionType() == parser::ExpressionType::CONJUNCTION_OR) {
    for (size_t idx = 0; idx < predicate->GetChildrenSize() - 1; ++idx) {
      std::vector<ast::Identifier> next_clause;
      GenerateFilterClauseFunctions(decls, predicate->GetChild(idx), &next_clause, false);
      filters_.emplace_back(std::move(next_clause));
    }
    // Last predicate is handled separately to keep api unitform
    GenerateFilterClauseFunctions(decls, predicate->GetChild(predicate->GetChildrenSize() - 1), curr_clause, false);
    return;
  }

  // Consecutive conjunctions are part of the same clause.
  if (predicate->GetExpressionType() == parser::ExpressionType::CONJUNCTION_AND) {
    for (const auto &child : predicate->GetChildren()) {
      GenerateFilterClauseFunctions(decls, child, curr_clause, true);
    }
    return;
  }

  // At this point, we create a term.
  // Signature: (execCtx: *ExecutionContext, vp: *VectorProjection, tids: *TupleIdList, ctx: *uint8) -> nil
  auto *codegen = GetCodeGen();
  auto fn_name = codegen->MakeFreshIdentifier(GetPipeline()->CreatePipelineFunctionName("FilterClause"));
  util::RegionVector<ast::FieldDecl *> params = codegen->MakeFieldList({
      codegen->MakeField(codegen->MakeIdentifier("execCtx"), codegen->PointerType(ast::BuiltinType::ExecutionContext)),
      codegen->MakeField(codegen->MakeIdentifier("vp"), codegen->PointerType(ast::BuiltinType::VectorProjection)),
      codegen->MakeField(codegen->MakeIdentifier("tids"), codegen->PointerType(ast::BuiltinType::TupleIdList)),
      codegen->MakeField(codegen->MakeIdentifier("context"), codegen->PointerType(ast::BuiltinType::Uint8)),
  });
  FunctionBuilder builder(codegen, fn_name, std::move(params), codegen->Nil());
  {
    ast::Expr *exec_ctx = builder.GetParameterByPosition(0);
    ast::Expr *vector_proj = builder.GetParameterByPosition(1);
    ast::Expr *tid_list = builder.GetParameterByPosition(2);
    if (parser::ExpressionUtil::IsColumnCompareWithConst(*predicate)) {
      auto cve = predicate->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
      auto translator = GetCompilationContext()->LookupTranslator(*predicate->GetChild(1));
      auto col_index = GetColOidIndex(cve->GetColumnOid());
      auto const_val = translator->DeriveValue(nullptr, nullptr);
      auto cmp_type = predicate->GetExpressionType();
      cmp_type = cmp_type == parser::ExpressionType::COMPARE_IN ? parser::ExpressionType::COMPARE_EQUAL : cmp_type;
      builder.Append(codegen->VPIFilter(exec_ctx,     // The execution context
                                        vector_proj,  // The vector projection
                                        cmp_type,     // Comparison type
                                        col_index,    // Column index
                                        const_val,    // Constant value
                                        tid_list));   // TID list
    } else if (parser::ExpressionUtil::IsColumnCompareWithParam(*predicate)) {
      // TODO(WAN): temporary hacky implementation, poke Prashanth...
      auto cve = predicate->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
      auto col_index = GetColOidIndex(cve->GetColumnOid());

      auto param_val = predicate->GetChild(1).CastManagedPointerTo<parser::ParameterValueExpression>();
      auto param_idx = param_val->GetValueIdx();
      ast::Builtin builtin;
      switch (param_val->GetReturnValueType()) {
        case type::TypeId::BOOLEAN:
          builtin = ast::Builtin::GetParamBool;
          break;
        case type::TypeId::TINYINT:
          builtin = ast::Builtin::GetParamTinyInt;
          break;
        case type::TypeId::SMALLINT:
          builtin = ast::Builtin::GetParamSmallInt;
          break;
        case type::TypeId::INTEGER:
          builtin = ast::Builtin::GetParamInt;
          break;
        case type::TypeId::BIGINT:
          builtin = ast::Builtin::GetParamBigInt;
          break;
        case type::TypeId::REAL:
          builtin = ast::Builtin::GetParamDouble;
          break;
        case type::TypeId::DATE:
          builtin = ast::Builtin::GetParamDate;
          break;
        case type::TypeId::TIMESTAMP:
          builtin = ast::Builtin::GetParamTimestamp;
          break;
        case type::TypeId::VARCHAR:
          builtin = ast::Builtin::GetParamString;
          break;
        default:
          UNREACHABLE("Unsupported parameter type");
      }
      auto const_val = codegen->CallBuiltin(
          builtin, {codegen->MakeExpr(codegen->MakeIdentifier("execCtx")), codegen->Const32(param_idx)});
      builder.Append(codegen->VPIFilter(exec_ctx,                        // The execution context
                                        vector_proj,                     // The vector projection
                                        predicate->GetExpressionType(),  // Comparison type
                                        col_index,                       // Column index
                                        const_val,                       // Constant value
                                        tid_list));                      // TID list
    } else if (parser::ExpressionUtil::IsConstCompareWithColumn(*predicate)) {
      throw NOT_IMPLEMENTED_EXCEPTION("const <op> col vector filter comparison not implemented");
    } else {
      // If we ever reach this point, the current node in the expression tree
      // violates strict DNF. Its subtree is treated as a generic,
      // non-vectorized filter.
      GenerateGenericTerm(&builder, predicate, vector_proj, tid_list);
    }
  }
  curr_clause->push_back(fn_name);
  decls->push_back(builder.Finish());
}

void SeqScanTranslator::DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) {
  if (HasPredicate()) {
    std::vector<ast::Identifier> curr_clause;
    auto root_expr = GetPlanAs<planner::SeqScanPlanNode>().GetScanPredicate();
    GenerateFilterClauseFunctions(decls, root_expr, &curr_clause, false);
    filters_.emplace_back(std::move(curr_clause));
  }
}

void SeqScanTranslator::ScanVPI(WorkContext *ctx, FunctionBuilder *function, ast::Expr *vpi) const {
  auto *codegen = GetCodeGen();

  auto gen_vpi_loop = [&](bool is_filtered) {
    Loop vpi_loop(function, nullptr, codegen->VPIHasNext(vpi, is_filtered),
                  codegen->MakeStmt(codegen->VPIAdvance(vpi, is_filtered)));
    {
      // var slot = @tableIterGetSlot(vpi)
      auto make_slot = codegen->CallBuiltin(ast::Builtin::VPIGetSlot, {codegen->MakeExpr(vpi_var_)});
      auto assign = codegen->Assign(codegen->MakeExpr(slot_var_), make_slot);
      function->Append(assign);
      // Push to parent.
      ctx->Push(function);
    }
    vpi_loop.EndLoop();
  };
  // TODO(Amadou): What if the predicate doesn't filter out anything?
  gen_vpi_loop(HasPredicate());

  // var vpi_num_tuples = @tableIterGetNumTuples(tvi)
  ast::Identifier vpi_num_tuples = codegen->MakeFreshIdentifier("vpi_num_tuples");
  function->Append(codegen->DeclareVarWithInit(
      vpi_num_tuples, codegen->CallBuiltin(ast::Builtin::TableIterGetVPINumTuples, {codegen->MakeExpr(tvi_var_)})));
  CounterAdd(function, num_scans_, vpi_num_tuples);
}

void SeqScanTranslator::ScanTable(WorkContext *ctx, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  // for (@tableIterAdvance(tvi))
  Loop tvi_loop(function, codegen->TableIterAdvance(codegen->MakeExpr(tvi_var_)));
  {
    // var vpi = @tableIterGetVPI(tvi)
    auto vpi = codegen->MakeExpr(vpi_var_);
    function->Append(codegen->DeclareVarWithInit(vpi_var_, codegen->TableIterGetVPI(codegen->MakeExpr(tvi_var_))));

    // if (predicate)
    if (HasPredicate()) {
      auto filter_manager = local_filter_manager_.GetPtr(codegen);
      function->Append(codegen->FilterManagerRunFilters(filter_manager, vpi, GetExecutionContext()));
    }

    if (!ctx->GetPipeline().IsVectorized()) {
      ScanVPI(ctx, function, vpi);
    }
  }
  tvi_loop.EndLoop();
}

void SeqScanTranslator::InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  if (HasPredicate()) {
    function->Append(codegen->FilterManagerInit(local_filter_manager_.GetPtr(codegen), GetExecutionContext()));
    for (const auto &clause : filters_) {
      function->Append(codegen->FilterManagerInsert(local_filter_manager_.GetPtr(codegen), clause));
    }
  }

  InitializeCounters(pipeline, function);
}

void SeqScanTranslator::TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (HasPredicate()) {
    auto filter_manager = local_filter_manager_.GetPtr(GetCodeGen());
    function->Append(GetCodeGen()->FilterManagerFree(filter_manager));
  }
}

void SeqScanTranslator::InitializeCounters(const Pipeline &pipeline, FunctionBuilder *function) const {
  CounterSet(function, num_scans_, 0);
}

void SeqScanTranslator::RecordCounters(const Pipeline &pipeline, FunctionBuilder *function) const {
  FeatureRecord(function, selfdriving::ExecutionOperatingUnitType::SEQ_SCAN,
                selfdriving::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, pipeline, CounterVal(num_scans_));
  FeatureRecord(function, selfdriving::ExecutionOperatingUnitType::SEQ_SCAN,
                selfdriving::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, pipeline, CounterVal(num_scans_));
  FeatureArithmeticRecordMul(function, pipeline, GetTranslatorId(), CounterVal(num_scans_));
}

void SeqScanTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  const bool declare_local_tvi = !GetPipeline()->IsParallel() || !GetPipeline()->IsDriver(this);
  if (declare_local_tvi) {
    // var tviBase: TableVectorIterator
    // var tvi = &tviBase
    auto tvi_base = codegen->MakeFreshIdentifier("tviBase");
    function->Append(codegen->DeclareVarNoInit(tvi_base, ast::BuiltinType::TableVectorIterator));
    function->Append(codegen->DeclareVarWithInit(tvi_var_, codegen->AddressOf(tvi_base)));
    // Declare the col_oids variable.
    DeclareColOids(function);
    // @tableIterInit(tvi, exec_ctx, table_oid, col_oids)
    function->Append(
        codegen->TableIterInit(codegen->MakeExpr(tvi_var_), GetExecutionContext(), GetTableOid(), col_oids_var_));
  }

  auto declare_slot = codegen->DeclareVarNoInit(slot_var_, ast::BuiltinType::TupleSlot);
  function->Append(declare_slot);

  // Scan it.
  ScanTable(context, function);

  // Close TVI, if need be.
  if (declare_local_tvi) {
    function->Append(codegen->TableIterClose(codegen->MakeExpr(tvi_var_)));
  }

  if (!GetPipeline()->IsParallel()) {
    RecordCounters(*GetPipeline(), function);
  }
}

util::RegionVector<ast::FieldDecl *> SeqScanTranslator::GetWorkerParams() const {
  auto *codegen = GetCodeGen();
  auto *tvi_type = codegen->PointerType(ast::BuiltinType::TableVectorIterator);
  return codegen->MakeFieldList({codegen->MakeField(tvi_var_, tvi_type)});
}

void SeqScanTranslator::LaunchWork(FunctionBuilder *function, ast::Identifier work_func) const {
  DeclareColOids(function);
  function->Append(GetCodeGen()->IterateTableParallel(GetTableOid(), col_oids_var_, GetQueryStatePtr(),
                                                      GetExecutionContext(), work_func));
}

ast::Expr *SeqScanTranslator::GetTableColumn(catalog::col_oid_t col_oid) const {
  const auto &schema = GetCodeGen()->GetCatalogAccessor()->GetSchema(GetTableOid());
  auto type = schema.GetColumn(col_oid).Type();
  auto nullable = schema.GetColumn(col_oid).Nullable();
  auto col_index = GetColOidIndex(col_oid);
  return GetCodeGen()->VPIGet(GetCodeGen()->MakeExpr(vpi_var_), sql::GetTypeId(type), nullable, col_index);
}

ast::Expr *SeqScanTranslator::GetSlotAddress() const { return GetCodeGen()->AddressOf(slot_var_); }

ast::Expr *SeqScanTranslator::GetVPI() const { return GetCodeGen()->MakeExpr(vpi_var_); }

void SeqScanTranslator::DeclareColOids(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  const auto &col_oids = col_oids_;

  // var col_oids: [num_cols]uint32
  ast::Expr *arr_type = codegen->ArrayType(col_oids.size(), ast::BuiltinType::Kind::Uint32);
  function->Append(codegen->DeclareVarNoInit(col_oids_var_, arr_type));

  // For each oid, set col_oids[i] = col_oid
  for (uint16_t i = 0; i < col_oids.size(); i++) {
    ast::Expr *lhs = codegen->ArrayAccess(col_oids_var_, i);
    ast::Expr *rhs = codegen->Const32(col_oids[i].UnderlyingValue());
    function->Append(codegen->Assign(lhs, rhs));
  }
}

uint32_t SeqScanTranslator::GetColOidIndex(catalog::col_oid_t col_oid) const {
  // TODO(WAN): this is sad code. How can we avoid doing this?
  const auto &col_oids = col_oids_;
  for (uint32_t i = 0; i < col_oids.size(); ++i) {
    if (col_oids[i] == col_oid) {
      return i;
    }
  }
  throw EXECUTION_EXCEPTION(fmt::format("Seq scan translator: col OID {} not found.", col_oid.UnderlyingValue()),
                            common::ErrorCode::ERRCODE_INTERNAL_ERROR);
}

std::vector<catalog::col_oid_t> SeqScanTranslator::MakeInputOids(const catalog::Schema &schema,
                                                                 const planner::SeqScanPlanNode &op) {
  if (op.GetColumnOids().empty()) {
    return {schema.GetColumn(0).Oid()};
  }
  return op.GetColumnOids();
}

}  // namespace noisepage::execution::compiler
