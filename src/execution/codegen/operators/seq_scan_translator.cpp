#include "execution/codegen/operators/seq_scan_translator.h"

#include "common/exception.h"
#include "execution/codegen/codegen.h"
#include "execution/codegen/compilation_context.h"
#include "execution/codegen/function_builder.h"
#include "execution/codegen/if.h"
#include "execution/codegen/loop.h"
#include "execution/codegen/pipeline.h"
#include "execution/codegen/work_context.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression_util.h"
#include "planner/plannodes/seq_scan_plan_node.h"

namespace terrier::execution::codegen {

SeqScanTranslator::SeqScanTranslator(const planner::SeqScanPlanNode &plan, CompilationContext *compilation_context,
                                     Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::SEQ_SCAN),
      tvi_var_(GetCodeGen()->MakeFreshIdentifier("tvi")),
      vpi_var_(GetCodeGen()->MakeFreshIdentifier("vpi")) {
  pipeline->RegisterSource(this, Pipeline::Parallelism::Parallel);
  // If there's a predicate, prepare the expression and register a filter manager.
  if (HasPredicate()) {
    compilation_context->Prepare(*plan.GetScanPredicate());

    ast::Expr *fm_type = GetCodeGen()->BuiltinType(ast::BuiltinType::FilterManager);
    local_filter_manager_ = pipeline->DeclarePipelineStateEntry("filterManager", fm_type);
  }
}

bool SeqScanTranslator::HasPredicate() const {
  return GetPlanAs<planner::SeqScanPlanNode>().GetScanPredicate() != nullptr;
}

std::string_view SeqScanTranslator::GetTableName() const {
  // TODO(WAN): ???
  UNREACHABLE("We shouldn't be doing catalog lookups at this point!! Go stuff it into the plan.");
  // const auto table_oid = GetPlanAs<planner::SeqScanPlanNode>().GetTableOid();
  // return Catalog::Instance()->LookupTableById(table_oid)->GetName();
}

void SeqScanTranslator::GenerateGenericTerm(FunctionBuilder *function,
                                            common::ManagedPointer<parser::AbstractExpression> term,
                                            ast::Expr *vector_proj, ast::Expr *tid_list) {
  auto codegen = GetCodeGen();

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
  gen_body(true);
  check_filtered.Else();
  gen_body(false);
  check_filtered.EndIf();
}

void SeqScanTranslator::GenerateFilterClauseFunctions(util::RegionVector<ast::FunctionDecl *> *decls,
                                                      common::ManagedPointer<parser::AbstractExpression> predicate,
                                                      std::vector<ast::Identifier> *curr_clause,
                                                      bool seen_conjunction) {
  // The top-most disjunctions in the tree form separate clauses in the filter manager.
  if (!seen_conjunction && predicate->GetExpressionType() == parser::ExpressionType::CONJUNCTION_OR) {
    std::vector<ast::Identifier> next_clause;
    GenerateFilterClauseFunctions(decls, predicate->GetChild(0), &next_clause, false);
    filters_.emplace_back(std::move(next_clause));
    GenerateFilterClauseFunctions(decls, predicate->GetChild(1), curr_clause, false);
    return;
  }

  // Consecutive conjunctions are part of the same clause.
  if (predicate->GetExpressionType() == parser::ExpressionType::CONJUNCTION_AND) {
    GenerateFilterClauseFunctions(decls, predicate->GetChild(0), curr_clause, true);
    GenerateFilterClauseFunctions(decls, predicate->GetChild(1), curr_clause, true);
    return;
  }

  // At this point, we create a term.
  // Signature: (vp: *VectorProjection, tids: *TupleIdList, ctx: *uint8) -> nil
  auto codegen = GetCodeGen();
  auto fn_name = codegen->MakeFreshIdentifier(GetPipeline()->CreatePipelineFunctionName("FilterClause"));
  util::RegionVector<ast::FieldDecl *> params = codegen->MakeFieldList({
      codegen->MakeField(codegen->MakeIdentifier("vp"), codegen->PointerType(ast::BuiltinType::VectorProjection)),
      codegen->MakeField(codegen->MakeIdentifier("tids"), codegen->PointerType(ast::BuiltinType::TupleIdList)),
      codegen->MakeField(codegen->MakeIdentifier("context"), codegen->PointerType(ast::BuiltinType::Uint8)),
  });
  FunctionBuilder builder(codegen, fn_name, std::move(params), codegen->Nil());
  {
    ast::Expr *vector_proj = builder.GetParameterByPosition(0);
    ast::Expr *tid_list = builder.GetParameterByPosition(1);
    if (parser::ExpressionUtil::IsColumnCompareWithConst(*predicate)) {
      auto cve = predicate->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
      auto translator = GetCompilationContext()->LookupTranslator(*predicate->GetChild(1));
      auto const_val = translator->DeriveValue(nullptr, nullptr);
      builder.Append(codegen->VPIFilter(vector_proj,                     // The vector projection
                                        predicate->GetExpressionType(),  // Comparison type
                                        !cve->GetColumnOid(),            // Column index
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
  CodeGen *codegen = GetCodeGen();

  auto gen_vpi_loop = [&](bool is_filtered) {
    Loop vpi_loop(function, nullptr, codegen->VPIHasNext(vpi, is_filtered),
                  codegen->MakeStmt(codegen->VPIAdvance(vpi, is_filtered)));
    {
      // Push to parent.
      ctx->Push(function);
    }
    vpi_loop.EndLoop();
  };
  // TODO(Amadou): What if the predicate doesn't filter out anything?
  gen_vpi_loop(HasPredicate());
}

void SeqScanTranslator::ScanTable(WorkContext *ctx, FunctionBuilder *function) const {
  CodeGen *codegen = GetCodeGen();
  Loop tvi_loop(function, codegen->TableIterAdvance(codegen->MakeExpr(tvi_var_)));
  {
    // var vpi = @tableIterGetVPI()
    auto vpi = codegen->MakeExpr(vpi_var_);
    function->Append(codegen->DeclareVarWithInit(vpi_var_, codegen->TableIterGetVPI(codegen->MakeExpr(tvi_var_))));

    if (HasPredicate()) {
      auto filter_manager = local_filter_manager_.GetPtr(codegen);
      function->Append(codegen->FilterManagerRunFilters(filter_manager, vpi));
    }

    if (!ctx->GetPipeline().IsVectorized()) {
      ScanVPI(ctx, function, vpi);
    }
  }
  tvi_loop.EndLoop();
}

void SeqScanTranslator::InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (HasPredicate()) {
    CodeGen *codegen = GetCodeGen();
    function->Append(codegen->FilterManagerInit(local_filter_manager_.GetPtr(codegen)));
    for (const auto &clause : filters_) {
      function->Append(codegen->FilterManagerInsert(local_filter_manager_.GetPtr(codegen), clause));
    }
  }
}

void SeqScanTranslator::TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (HasPredicate()) {
    auto filter_manager = local_filter_manager_.GetPtr(GetCodeGen());
    function->Append(GetCodeGen()->FilterManagerFree(filter_manager));
  }
}

void SeqScanTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  CodeGen *codegen = GetCodeGen();

  const bool declare_local_tvi = !GetPipeline()->IsParallel() || !GetPipeline()->IsDriver(this);
  if (declare_local_tvi) {
    // var tviBase: TableVectorIterator
    // var tvi = &tviBase
    auto tvi_base = codegen->MakeFreshIdentifier("tviBase");
    function->Append(codegen->DeclareVarNoInit(tvi_base, ast::BuiltinType::TableVectorIterator));
    function->Append(codegen->DeclareVarWithInit(tvi_var_, codegen->AddressOf(tvi_base)));
    function->Append(codegen->TableIterInit(codegen->MakeExpr(tvi_var_), GetTableName()));
  }

  // Scan it.
  ScanTable(context, function);

  // Close TVI, if need be.
  if (declare_local_tvi) {
    function->Append(codegen->TableIterClose(codegen->MakeExpr(tvi_var_)));
  }
}

util::RegionVector<ast::FieldDecl *> SeqScanTranslator::GetWorkerParams() const {
  auto codegen = GetCodeGen();
  auto tvi_type = codegen->PointerType(ast::BuiltinType::TableVectorIterator);
  return codegen->MakeFieldList({codegen->MakeField(tvi_var_, tvi_type)});
}

void SeqScanTranslator::LaunchWork(FunctionBuilder *function, ast::Identifier work_func) const {
  function->Append(
      GetCodeGen()->IterateTableParallel(GetTableName(), GetQueryStatePtr(), GetThreadStateContainer(), work_func));
}

ast::Expr *SeqScanTranslator::GetTableColumn(catalog::col_oid_t col_oid) const {
  // TODO(WAN): ??
  UNREACHABLE("Should we still be doing catalog lookups here?");
#if 0
  const auto table_oid = GetPlanAs<planner::SeqScanPlanNode>().GetTableOid();
  const auto schema = &Catalog::Instance()->LookupTableById(table_oid)->GetSchema();
  auto type = schema->GetColumnInfo(col_oid)->sql_type.GetPrimitiveTypeId();
  auto nullable = schema->GetColumnInfo(col_oid)->sql_type.IsNullable();
  return GetCodeGen()->VPIGet(GetCodeGen()->MakeExpr(vpi_var_), type, nullable, col_oid);
#endif
}

}  // namespace terrier::execution::codegen
