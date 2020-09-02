#include "execution/compiler/operator/insert_translator.h"

#include <vector>

#include "catalog/catalog_accessor.h"
#include "catalog/schema.h"
#include "execution/ast/builtins.h"
#include "execution/ast/type.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/insert_plan_node.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace terrier::execution::compiler {
InsertTranslator::InsertTranslator(const planner::InsertPlanNode &plan, CompilationContext *compilation_context,
                                   Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::INSERT),
      inserter_(GetCodeGen()->MakeFreshIdentifier("inserter")),
      insert_pr_(GetCodeGen()->MakeFreshIdentifier("insert_pr")),
      col_oids_(GetCodeGen()->MakeFreshIdentifier("col_oids")),
      table_schema_(GetCodeGen()->GetCatalogAccessor()->GetSchema(GetPlanAs<planner::InsertPlanNode>().GetTableOid())),
      all_oids_(AllColOids(table_schema_)),
      table_pm_(GetCodeGen()
                    ->GetCatalogAccessor()
                    ->GetTable(GetPlanAs<planner::InsertPlanNode>().GetTableOid())
                    ->ProjectionMapForOids(all_oids_)) {
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);
  for (uint32_t idx = 0; idx < plan.GetBulkInsertCount(); idx++) {
    const auto &node_vals = GetPlanAs<planner::InsertPlanNode>().GetValues(idx);
    for (const auto &node_val : node_vals) {
      compilation_context->Prepare(*node_val);
    }
  }
  for (auto &index_oid : GetCodeGen()->GetCatalogAccessor()->GetIndexOids(plan.GetTableOid())) {
    const auto &index_schema = GetCodeGen()->GetCatalogAccessor()->GetIndexSchema(index_oid);
    for (const auto &index_col : index_schema.GetColumns()) {
      compilation_context->Prepare(*index_col.StoredExpression());
    }
  }

  num_inserts_ = CounterDeclare("num_inserts");
}

void InsertTranslator::InitializeQueryState(FunctionBuilder *function) const { CounterSet(function, num_inserts_, 0); }

void InsertTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  // var col_oids: [num_cols]uint32
  // col_oids[i] = ...
  // var inserter : StorageInterface
  // @storageInterfaceInit(inserter, execCtx, table_oid, col_oids, true)
  DeclareInserter(function);
  // var insert_pr : *ProjectedRow
  DeclareInsertPR(function);

  for (uint32_t idx = 0; idx < GetPlanAs<planner::InsertPlanNode>().GetBulkInsertCount(); idx++) {
    // var insert_pr = @getTablePR(&inserter)
    GetInsertPR(function);
    // For each attribute, @prSet(insert_pr, ...)
    GenSetTablePR(function, context, idx);
    // var insert_slot = @tableInsert(&inserter)
    GenTableInsert(function);
    function->Append(GetCodeGen()->ExecCtxAddRowsAffected(GetExecutionContext(), 1));
    const auto &table_oid = GetPlanAs<planner::InsertPlanNode>().GetTableOid();
    const auto &index_oids = GetCodeGen()->GetCatalogAccessor()->GetIndexOids(table_oid);
    for (const auto &index_oid : index_oids) {
      GenIndexInsert(context, function, index_oid);
    }
  }

  FeatureRecord(function, brain::ExecutionOperatingUnitType::INSERT,
                brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, context->GetPipeline(),
                CounterVal(num_inserts_));
  FeatureRecord(function, brain::ExecutionOperatingUnitType::INSERT,
                brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, context->GetPipeline(),
                CounterVal(num_inserts_));
  FeatureArithmeticRecordMul(function, context->GetPipeline(), GetTranslatorId(), CounterVal(num_inserts_));

  GenInserterFree(function);
}

void InsertTranslator::DeclareInserter(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var col_oids: [num_cols]uint32
  // col_oids[i] = ...
  SetOids(builder);
  // var inserter : StorageInterface
  auto *storage_interface_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  builder->Append(GetCodeGen()->DeclareVar(inserter_, storage_interface_type, nullptr));
  // @storageInterfaceInit(inserter, execCtx, table_oid, col_oids, true)
  ast::Expr *inserter_setup = GetCodeGen()->StorageInterfaceInit(
      inserter_, GetExecutionContext(), GetPlanAs<planner::InsertPlanNode>().GetTableOid().UnderlyingValue(), col_oids_,
      true);
  builder->Append(GetCodeGen()->MakeStmt(inserter_setup));
}

void InsertTranslator::GenInserterFree(terrier::execution::compiler::FunctionBuilder *builder) const {
  // Call @storageInterfaceFree
  ast::Expr *inserter_free =
      GetCodeGen()->CallBuiltin(ast::Builtin::StorageInterfaceFree, {GetCodeGen()->AddressOf(inserter_)});
  builder->Append(GetCodeGen()->MakeStmt(inserter_free));
}

ast::Expr *InsertTranslator::GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const {
  TERRIER_ASSERT(child_idx == 0, "Insert plan can only have one child");

  return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
}

ast::Expr *InsertTranslator::GetTableColumn(catalog::col_oid_t col_oid) const {
  auto column = table_schema_.GetColumn(col_oid);
  auto type = column.Type();
  auto nullable = column.Nullable();
  auto attr_index = table_pm_.find(col_oid)->second;
  return GetCodeGen()->PRGet(GetCodeGen()->MakeExpr(insert_pr_), type, nullable, attr_index);
}

void InsertTranslator::SetOids(FunctionBuilder *builder) const {
  // var col_oids: [num_cols]uint32
  ast::Expr *arr_type = GetCodeGen()->ArrayType(all_oids_.size(), ast::BuiltinType::Kind::Uint32);
  builder->Append(GetCodeGen()->DeclareVar(col_oids_, arr_type, nullptr));

  for (uint16_t i = 0; i < all_oids_.size(); i++) {
    // col_oids[i] = col_oid
    ast::Expr *lhs = GetCodeGen()->ArrayAccess(col_oids_, i);
    ast::Expr *rhs = GetCodeGen()->Const32(all_oids_[i].UnderlyingValue());
    builder->Append(GetCodeGen()->Assign(lhs, rhs));
  }
}

void InsertTranslator::DeclareInsertPR(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var insert_pr : *ProjectedRow
  auto *pr_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::ProjectedRow);
  builder->Append(GetCodeGen()->DeclareVar(insert_pr_, GetCodeGen()->PointerType(pr_type), nullptr));
}

void InsertTranslator::GetInsertPR(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var insert_pr = @getTablePR(&inserter)
  auto *get_pr_call = GetCodeGen()->CallBuiltin(ast::Builtin::GetTablePR, {GetCodeGen()->AddressOf(inserter_)});
  builder->Append(GetCodeGen()->Assign(GetCodeGen()->MakeExpr(insert_pr_), get_pr_call));
}

void InsertTranslator::GenSetTablePR(FunctionBuilder *builder, WorkContext *context, uint32_t idx) const {
  const auto &node_vals = GetPlanAs<planner::InsertPlanNode>().GetValues(idx);
  for (size_t i = 0; i < node_vals.size(); i++) {
    // @prSet(insert_pr, ...)
    const auto &val = node_vals[i];
    auto *src = context->DeriveValue(*val.Get(), this);

    const auto &table_col_oid = all_oids_[i];
    const auto &table_col = table_schema_.GetColumn(table_col_oid);
    const auto &pr_set_call =
        GetCodeGen()->PRSet(GetCodeGen()->MakeExpr(insert_pr_), table_col.Type(), table_col.Nullable(),
                            table_pm_.find(table_col_oid)->second, src, true);
    builder->Append(GetCodeGen()->MakeStmt(pr_set_call));
  }
}

void InsertTranslator::GenTableInsert(FunctionBuilder *builder) const {
  // var insert_slot = @tableInsert(&inserter)
  const auto &insert_slot = GetCodeGen()->MakeFreshIdentifier("insert_slot");
  auto *insert_call = GetCodeGen()->CallBuiltin(ast::Builtin::TableInsert, {GetCodeGen()->AddressOf(inserter_)});
  builder->Append(GetCodeGen()->DeclareVar(insert_slot, nullptr, insert_call));

  CounterAdd(builder, num_inserts_, 1);
}

void InsertTranslator::GenIndexInsert(WorkContext *context, FunctionBuilder *builder,
                                      const catalog::index_oid_t &index_oid) const {
  // var insert_index_pr = @getIndexPR(&inserter, oid)
  const auto &insert_index_pr = GetCodeGen()->MakeFreshIdentifier("insert_index_pr");
  std::vector<ast::Expr *> pr_call_args{GetCodeGen()->AddressOf(inserter_),
                                        GetCodeGen()->Const32(index_oid.UnderlyingValue())};
  auto *get_index_pr_call = GetCodeGen()->CallBuiltin(ast::Builtin::GetIndexPR, pr_call_args);
  builder->Append(GetCodeGen()->DeclareVar(insert_index_pr, nullptr, get_index_pr_call));

  const auto &index = GetCodeGen()->GetCatalogAccessor()->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = GetCodeGen()->GetCatalogAccessor()->GetIndexSchema(index_oid);
  auto *index_pr_expr = GetCodeGen()->MakeExpr(insert_index_pr);

  for (const auto &index_col : index_schema.GetColumns()) {
    // @prSet(insert_index_pr, attr_idx, val, true)
    const auto &col_expr = context->DeriveValue(*index_col.StoredExpression().Get(), this);
    uint16_t attr_offset = index_pm.at(index_col.Oid());
    type::TypeId attr_type = index_col.Type();
    bool nullable = index_col.Nullable();
    auto *set_key_call = GetCodeGen()->PRSet(index_pr_expr, attr_type, nullable, attr_offset, col_expr, false);
    builder->Append(GetCodeGen()->MakeStmt(set_key_call));
  }

  // if (!@indexInsert(&inserter)) { Abort(); }
  const auto &builtin = index_schema.Unique() ? ast::Builtin::IndexInsertUnique : ast::Builtin::IndexInsert;
  auto *index_insert_call = GetCodeGen()->CallBuiltin(builtin, {GetCodeGen()->AddressOf(inserter_)});
  auto *cond = GetCodeGen()->UnaryOp(parsing::Token::Type::BANG, index_insert_call);
  If success(builder, cond);
  { builder->Append(GetCodeGen()->AbortTxn(GetExecutionContext())); }
  success.EndIf();
}

std::vector<catalog::col_oid_t> InsertTranslator::AllColOids(const catalog::Schema &table_schema) {
  std::vector<catalog::col_oid_t> oids;
  for (const auto &col : table_schema.GetColumns()) {
    oids.emplace_back(col.Oid());
  }
  return oids;
}

}  // namespace terrier::execution::compiler
