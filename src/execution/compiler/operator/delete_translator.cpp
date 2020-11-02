#include "execution/compiler/operator/delete_translator.h"

#include <vector>

#include "catalog/catalog_accessor.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/delete_plan_node.h"
#include "storage/index/index.h"

namespace noisepage::execution::compiler {
DeleteTranslator::DeleteTranslator(const planner::DeletePlanNode &plan, CompilationContext *compilation_context,
                                   Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::DELETE),
      deleter_(GetCodeGen()->MakeFreshIdentifier("deleter")),
      col_oids_(GetCodeGen()->MakeFreshIdentifier("col_oids")) {
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);
  // Prepare the child.
  compilation_context->Prepare(*plan.GetChild(0), pipeline);

  for (auto &index_oid : GetCodeGen()->GetCatalogAccessor()->GetIndexOids(plan.GetTableOid())) {
    const auto &index_schema = GetCodeGen()->GetCatalogAccessor()->GetIndexSchema(index_oid);
    for (const auto &index_col : index_schema.GetColumns()) {
      compilation_context->Prepare(*index_col.StoredExpression());
    }
  }

  num_deletes_ = CounterDeclare("num_deletes", pipeline);
}

void DeleteTranslator::InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  CounterSet(function, num_deletes_, 0);
}

void DeleteTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  // Delete from table
  DeclareDeleter(function);
  GenTableDelete(function);
  function->Append(GetCodeGen()->ExecCtxAddRowsAffected(GetExecutionContext(), 1));

  // Delete from every index
  const auto &op = GetPlanAs<planner::DeletePlanNode>();
  const auto &indexes = GetCodeGen()->GetCatalogAccessor()->GetIndexOids(op.GetTableOid());
  for (const auto &index_oid : indexes) {
    GenIndexDelete(function, context, index_oid);
  }

  GenDeleterFree(function);
}

void DeleteTranslator::FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
  FeatureRecord(function, brain::ExecutionOperatingUnitType::DELETE,
                brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, pipeline, CounterVal(num_deletes_));
  FeatureRecord(function, brain::ExecutionOperatingUnitType::DELETE,
                brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, pipeline, CounterVal(num_deletes_));
  FeatureArithmeticRecordMul(function, pipeline, GetTranslatorId(), CounterVal(num_deletes_));
}

void DeleteTranslator::DeclareDeleter(FunctionBuilder *builder) const {
  // var col_oids : [0]uint32
  SetOids(builder);
  // var deleter : StorageInterface
  auto *storage_interface_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  builder->Append(GetCodeGen()->DeclareVarNoInit(deleter_, storage_interface_type));
  // @storageInterfaceInit(&deleter, execCtx, table_oid, col_oids, true)
  const auto &op = GetPlanAs<planner::DeletePlanNode>();
  ast::Expr *deleter_setup = GetCodeGen()->StorageInterfaceInit(deleter_, GetExecutionContext(),
                                                                op.GetTableOid().UnderlyingValue(), col_oids_, true);
  builder->Append(GetCodeGen()->MakeStmt(deleter_setup));
}

void DeleteTranslator::GenDeleterFree(FunctionBuilder *builder) const {
  // @storageInterfaceFree(&deleter)
  ast::Expr *deleter_free =
      GetCodeGen()->CallBuiltin(ast::Builtin::StorageInterfaceFree, {GetCodeGen()->AddressOf(deleter_)});
  builder->Append(GetCodeGen()->MakeStmt(deleter_free));
}

void DeleteTranslator::GenTableDelete(FunctionBuilder *builder) const {
  // if (!@tableDelete(&deleter, &slot)) { Abort(); }
  const auto &op = GetPlanAs<planner::DeletePlanNode>();
  const auto &child = GetCompilationContext()->LookupTranslator(*op.GetChild(0));
  NOISEPAGE_ASSERT(child != nullptr, "delete should have a child");
  const auto &delete_slot = child->GetSlotAddress();
  std::vector<ast::Expr *> delete_args{GetCodeGen()->AddressOf(deleter_), delete_slot};
  auto *delete_call = GetCodeGen()->CallBuiltin(ast::Builtin::TableDelete, delete_args);
  auto *delete_failed = GetCodeGen()->UnaryOp(parsing::Token::Type::BANG, delete_call);
  If check(builder, delete_failed);
  {
    // The delete was not successful; abort the transaction.
    builder->Append(GetCodeGen()->AbortTxn(GetExecutionContext()));
  }
  check.Else();
  { CounterAdd(builder, num_deletes_, 1); }
  check.EndIf();
}

void DeleteTranslator::GenIndexDelete(FunctionBuilder *builder, WorkContext *context,
                                      const catalog::index_oid_t &index_oid) const {
  // var delete_index_pr = @getIndexPR(&deleter, oid)
  auto delete_index_pr = GetCodeGen()->MakeFreshIdentifier("delete_index_pr");
  std::vector<ast::Expr *> pr_call_args{GetCodeGen()->AddressOf(deleter_),
                                        GetCodeGen()->Const32(index_oid.UnderlyingValue())};
  auto *get_index_pr_call = GetCodeGen()->CallBuiltin(ast::Builtin::GetIndexPR, pr_call_args);
  builder->Append(GetCodeGen()->DeclareVar(delete_index_pr, nullptr, get_index_pr_call));

  auto index = GetCodeGen()->GetCatalogAccessor()->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = GetCodeGen()->GetCatalogAccessor()->GetIndexSchema(index_oid);
  const auto &index_cols = index_schema.GetColumns();

  const auto &op = GetPlanAs<planner::DeletePlanNode>();
  const auto &child = GetCompilationContext()->LookupTranslator(*op.GetChild(0));
  for (const auto &index_col : index_cols) {
    // @prSetCall(delete_index_pr, type, nullable, attr_idx, val)
    // NOTE: index expressions refer to columns in the child translator.
    // For example, if the child is a seq scan, the index expressions would contain ColumnValueExpressions
    const auto &val = context->DeriveValue(*index_col.StoredExpression().Get(), child);
    auto *pr_set_call = GetCodeGen()->PRSet(GetCodeGen()->MakeExpr(delete_index_pr), index_col.Type(),
                                            index_col.Nullable(), index_pm.at(index_col.Oid()), val, true);
    builder->Append(GetCodeGen()->MakeStmt(pr_set_call));
  }

  // @indexDelete(&deleter)
  std::vector<ast::Expr *> delete_args{GetCodeGen()->AddressOf(deleter_), child->GetSlotAddress()};
  auto *index_delete_call = GetCodeGen()->CallBuiltin(ast::Builtin::IndexDelete, delete_args);
  builder->Append(GetCodeGen()->MakeStmt(index_delete_call));
}

void DeleteTranslator::SetOids(FunctionBuilder *builder) const {
  // var col_oids: [0]uint32
  ast::Expr *arr_type = GetCodeGen()->ArrayType(0, ast::BuiltinType::Kind::Uint32);
  builder->Append(GetCodeGen()->DeclareVar(col_oids_, arr_type, nullptr));
}

}  // namespace noisepage::execution::compiler
