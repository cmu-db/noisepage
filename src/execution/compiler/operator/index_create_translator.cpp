#include "execution/compiler/operator/index_create_translator.h"

#include "execution/sql/ddl_executors.h"

#include "catalog/catalog_accessor.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/work_context.h"
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
      inserter_(codegen_->MakeFreshIdentifier("inserter")),
      index_pr_(codegen_->MakeFreshIdentifier("index_pr")),
      tvi_var_(codegen_->MakeFreshIdentifier("tvi")),
      vpi_var_(codegen_->MakeFreshIdentifier("vpi")),
      col_oids_var_(codegen_->MakeFreshIdentifier("col_oids")),
      slot_var_(codegen_->MakeFreshIdentifier("slot")),
      table_schema_(codegen_->GetCatalogAccessor()->GetSchema(GetPlanAs<planner::CreateIndexPlanNode>().GetTableOid())),
      all_oids_(AllColOids(table_schema_)),
      index_oid_(
          codegen_->GetCatalogAccessor()->GetIndexOid(GetPlanAs<planner::CreateIndexPlanNode>().GetIndexName())) {
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);
}

void IndexCreateTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  InitScan(function);

  PrepareContext(context, function);
  // Scan it.
  ScanTable(context, function);

  // Close TVI, if need be.
  function->Append(codegen_->TableIterClose(codegen_->MakeExpr(tvi_var_)));

  FreeInserter(function);
}

void IndexCreateTranslator::InitScan(FunctionBuilder *function) const {
  // Init inserter, index pr, table pr, create index, and init tvi
  DeclareInserter(function);
  DeclareIndexPR(function);
  DeclareTVI(function);
  DeclareSlot(function);
}

void IndexCreateTranslator::DeclareInserter(FunctionBuilder *function) const {
  // var col_oids: [num_cols]uint32
  // col_oids[i] = ...
  SetOids(function);
  // var inserter : StorageInterface
  auto *storage_interface_type = codegen_->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  function->Append(codegen_->DeclareVar(inserter_, storage_interface_type, nullptr));
  // @storageInterfaceInit(inserter, execCtx, table_oid, col_oids_var_, false)
  ast::Expr *inserter_setup = codegen_->StorageInterfaceInit(
      inserter_, GetExecutionContext(), uint32_t(GetPlanAs<planner::CreateIndexPlanNode>().GetTableOid()),
      col_oids_var_, false);
  function->Append(codegen_->MakeStmt(inserter_setup));
}

void IndexCreateTranslator::SetOids(FunctionBuilder *function) const {
  // var col_oids_var_: [num_cols]uint32
  ast::Expr *arr_type = codegen_->ArrayType(all_oids_.size(), ast::BuiltinType::Kind::Uint32);
  function->Append(codegen_->DeclareVar(col_oids_var_, arr_type, nullptr));

  for (uint16_t i = 0; i < all_oids_.size(); i++) {
    // col_oids_var_[i] = col_oid
    ast::Expr *lhs = codegen_->ArrayAccess(col_oids_var_, i);
    ast::Expr *rhs = codegen_->Const32(uint32_t(all_oids_[i]));
    function->Append(codegen_->Assign(lhs, rhs));
  }
}

void IndexCreateTranslator::DeclareIndexPR(FunctionBuilder *function) const {
  // var index_pr = @getIndexPR(&inserter, oid)
  std::vector<ast::Expr *> pr_call_args{codegen_->AddressOf(inserter_), codegen_->Const32(uint32_t(index_oid_))};
  auto get_index_pr_call = codegen_->CallBuiltin(ast::Builtin::GetIndexPR, pr_call_args);
  function->Append(codegen_->DeclareVar(index_pr_, nullptr, get_index_pr_call));
}

void IndexCreateTranslator::DeclareTVI(FunctionBuilder *function) const {
  auto codegen = GetCodeGen();
  // var tviBase: TableVectorIterator
  // var tvi = &tviBase
  auto tvi_base = codegen->MakeFreshIdentifier("tviBase");
  function->Append(codegen->DeclareVarNoInit(tvi_base, ast::BuiltinType::TableVectorIterator));
  function->Append(codegen->DeclareVarWithInit(tvi_var_, codegen->AddressOf(tvi_base)));
  // @tableIterInit(tvi, exec_ctx, table_oid, col_oids)
  function->Append(codegen->TableIterInit(codegen->MakeExpr(tvi_var_), GetExecutionContext(),
                                          GetPlanAs<planner::CreateIndexPlanNode>().GetTableOid(), col_oids_var_));
}

void IndexCreateTranslator::DeclareSlot(FunctionBuilder *function) const {
  auto declare_slot = codegen_->DeclareVarNoInit(slot_var_, ast::BuiltinType::TupleSlot);
  function->Append(declare_slot);
}

std::vector<catalog::col_oid_t> IndexCreateTranslator::AllColOids(const catalog::Schema &table_schema) const {
  std::vector<catalog::col_oid_t> oids;
  for (const auto &col : table_schema.GetColumns()) {
    oids.emplace_back(col.Oid());
  }
  return oids;
}

void IndexCreateTranslator::PrepareContext(WorkContext *context, FunctionBuilder *function) const {
  const auto &index_schema = codegen_->GetCatalogAccessor()->GetIndexSchema(index_oid_);
  for (const auto &index_col : index_schema.GetColumns()) {
    context->GetCompilationContext()->Prepare(*index_col.StoredExpression());
  }
}

void IndexCreateTranslator::ScanTable(WorkContext *ctx, FunctionBuilder *function) const {
  // for (@tableIterAdvance(tvi))
  Loop tvi_loop(function, codegen_->TableIterAdvance(codegen_->MakeExpr(tvi_var_)));
  {
    // var vpi = @tableIterGetVPI(tvi)
    auto vpi = codegen_->MakeExpr(vpi_var_);
    function->Append(codegen_->DeclareVarWithInit(vpi_var_, codegen_->TableIterGetVPI(codegen_->MakeExpr(tvi_var_))));

    if (!ctx->GetPipeline().IsVectorized()) {
      ScanVPI(ctx, function, vpi);
    }
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
      auto assign = codegen_->Assign(codegen_->MakeExpr(slot_var_), make_slot);
      function->Append(assign);
      IndexInsert(ctx, function);
      // Push to parent.
      // ctx->Push(function);
    }
    vpi_loop.EndLoop();
  };
  gen_vpi_loop(false);
}

void IndexCreateTranslator::IndexInsert(WorkContext *ctx, FunctionBuilder *function) const {
  const auto &index = codegen_->GetCatalogAccessor()->GetIndex(index_oid_);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = codegen_->GetCatalogAccessor()->GetIndexSchema(index_oid_);
  auto *index_pr_expr = codegen_->MakeExpr(index_pr_);

  for (const auto &index_col : index_schema.GetColumns()) {
    // @prSet(insert_index_pr, attr_idx, @VPIGet(vpi_var_, attr_sql_type, true, oid), true)
    uint16_t attr_offset = index_pm.at(index_col.Oid());
    type::TypeId attr_type = index_col.Type();
    auto attr_sql_type = sql::GetTypeId(index_col.Type());
    bool nullable = index_col.Nullable();
    const auto &col_expr = codegen_->VPIGet(codegen_->MakeExpr(vpi_var_), attr_sql_type, nullable, attr_offset);
    auto *set_key_call = codegen_->PRSet(index_pr_expr, attr_type, nullable, attr_offset, col_expr, false);
    function->Append(codegen_->MakeStmt(set_key_call));
  }

  // if (!@IndexInsertWithSlot(&inserter, &slot_var_, unique)) { Abort(); }
  auto *index_insert_call = codegen_->CallBuiltin(
      ast::Builtin::IndexInsertWithSlot,
      {codegen_->AddressOf(inserter_), codegen_->AddressOf(slot_var_), codegen_->ConstBool(index_schema.Unique())});
  auto *cond = codegen_->UnaryOp(parsing::Token::Type::BANG, index_insert_call);
  If success(function, cond);
  { function->Append(codegen_->AbortTxn(GetExecutionContext())); }
  success.EndIf();
}

void IndexCreateTranslator::FreeInserter(FunctionBuilder *function) const {
  // Call @storageInterfaceFree
  ast::Expr *inserter_free =
      GetCodeGen()->CallBuiltin(ast::Builtin::StorageInterfaceFree, {GetCodeGen()->AddressOf(inserter_)});
  function->Append(GetCodeGen()->MakeStmt(inserter_free));
}

}  // namespace terrier::execution::compiler
