#include "execution/compiler/operator/delete_translator.h"

#include <execution/compiler/if.h>

#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/work_context.h"
#include "storage/index/index.h"

namespace terrier::execution::compiler {
DeleteTranslator::DeleteTranslator(const planner::DeletePlanNode &plan, CompilationContext *compilation_context,
                                   Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::DELETE),
      deleter_(GetCodeGen()->MakeFreshIdentifier("deleter")),
      col_oids_(GetCodeGen()->MakeFreshIdentifier("col-oids")) {
  // Prepare the child.
  compilation_context->Prepare(*plan.GetChild(0), pipeline);

  for (auto &index_oid : GetCodeGen()->GetCatalogAccessor()->GetIndexOids(plan.GetTableOid())) {
    auto &index_schema = GetCodeGen()->GetCatalogAccessor()->GetIndexSchema(index_oid);
    for (const auto &index_col : index_schema.GetColumns()) {
      compilation_context->Prepare(*index_col.StoredExpression().Get());
    }
  }
}

// void DeleteTranslator::Produce(FunctionBuilder *builder) {
//  DeclareDeleter(builder);
//  child_translator_->Produce(builder);
//  GenDeleterFree(builder);
//}

void DeleteTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  // Delete from table
  DeclareDeleter(function);
  GenTableDelete(context, function);

  // Delete from every index
  auto &op = GetPlanAs<planner::DeletePlanNode>();
  const auto &indexes = GetCodeGen()->GetCatalogAccessor()->GetIndexOids(op.GetTableOid());
  for (auto &index_oid : indexes) {
    GenIndexDelete(function, context, index_oid);
  }
  GenDeleterFree(function);
}

void DeleteTranslator::DeclareDeleter(FunctionBuilder *builder) const {
  // Generate col oids
  SetOids(builder);
  // var deleter : StorageInterface
  auto storage_interface_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  builder->Append(GetCodeGen()->DeclareVar(deleter_, storage_interface_type, nullptr));
  // Call @storageInterfaceInit
  auto &op = GetPlanAs<planner::DeletePlanNode>();
  ast::Expr *deleter_setup =
      GetCodeGen()->StorageInterfaceInit(deleter_, GetExecutionContext(), !op.GetTableOid(), col_oids_, true);
  builder->Append(GetCodeGen()->MakeStmt(deleter_setup));
}

void DeleteTranslator::GenDeleterFree(FunctionBuilder *builder) const {
  // Call @storageInterfaceFree
  ast::Expr *deleter_free =
      GetCodeGen()->CallBuiltin(ast::Builtin::StorageInterfaceFree, {GetCodeGen()->AddressOf(deleter_)});
  builder->Append(GetCodeGen()->MakeStmt(deleter_free));
}

// ast::Expr *DeleteTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) {
//  TERRIER_ASSERT(child_idx == 0, "Delete plan can only have one child");
//  GetCompilationContext()->LookupTranslator(GetPlanAs<planner::DeletePlanNode>().GetChild(0))
//  return child_translator_->GetOutput(attr_idx);
//}

void DeleteTranslator::GenTableDelete(WorkContext *context, FunctionBuilder *builder) const {
  // Delete from table
  // if (delete not successfull) { Abort(); }
  auto &op = GetPlanAs<planner::DeletePlanNode>();
  //  auto delete_slot = context->DeriveValue(op))
  auto child = GetCompilationContext()->LookupTranslator(*op.GetChild(0));
  TERRIER_ASSERT(child != nullptr, "delete should have a child");
  auto delete_slot = child->GetSlot();
  std::vector<ast::Expr *> delete_args{GetCodeGen()->AddressOf(deleter_), delete_slot};
  auto delete_call = GetCodeGen()->CallBuiltin(ast::Builtin::TableDelete, std::move(delete_args));
  auto cond = GetCodeGen()->UnaryOp(parsing::Token::Type::BANG, delete_call);
  If check(builder, cond);
  { builder->Append(GetCodeGen()->AbortTxn(GetExecutionContext())); }
  check.EndIf();
}

void DeleteTranslator::GenIndexDelete(FunctionBuilder *builder, WorkContext *context,
                                      const catalog::index_oid_t &index_oid) const {
  // var delete_index_pr = @getIndexPR(&deleter, oid)
  auto delete_index_pr = GetCodeGen()->MakeFreshIdentifier("delete_index_pr");
  std::vector<ast::Expr *> pr_call_args{GetCodeGen()->AddressOf(deleter_), GetCodeGen()->Const32(!index_oid)};
  auto get_index_pr_call = GetCodeGen()->CallBuiltin(ast::Builtin::GetIndexPR, std::move(pr_call_args));
  builder->Append(GetCodeGen()->DeclareVar(delete_index_pr, nullptr, get_index_pr_call));

  // Fill up the index pr
  auto index = GetCodeGen()->GetCatalogAccessor()->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = GetCodeGen()->GetCatalogAccessor()->GetIndexSchema(index_oid);
  const auto &index_cols = index_schema.GetColumns();

  auto &op = GetPlanAs<planner::DeletePlanNode>();
  auto child = GetCompilationContext()->LookupTranslator(*op.GetChild(0));
  for (const auto &index_col : index_cols) {
    // NOTE: index expressions refer to columns in the child translator.
    // For example, if the child is a seq scan, the index expressions would contain ColumnValueExpressions
    auto val = context->DeriveValue(*index_col.StoredExpression().Get(), child);
    auto pr_set_call = GetCodeGen()->PRSet(GetCodeGen()->MakeExpr(delete_index_pr), index_col.Type(),
                                           index_col.Nullable(), index_pm.at(index_col.Oid()), val);
    builder->Append(GetCodeGen()->MakeStmt(pr_set_call));
  }

  // Delete from index
  std::vector<ast::Expr *> delete_args{GetCodeGen()->AddressOf(deleter_), child->GetSlot()};
  auto index_delete_call = GetCodeGen()->CallBuiltin(ast::Builtin::IndexDelete, std::move(delete_args));
  builder->Append(GetCodeGen()->MakeStmt(index_delete_call));
}

void DeleteTranslator::SetOids(FunctionBuilder *builder) const {
  // Declare: var col_oids: [num_cols]uint32
  ast::Expr *arr_type = GetCodeGen()->ArrayType(0, ast::BuiltinType::Kind::Uint32);
  builder->Append(GetCodeGen()->DeclareVar(col_oids_, arr_type, nullptr));
}

}  // namespace terrier::execution::compiler
