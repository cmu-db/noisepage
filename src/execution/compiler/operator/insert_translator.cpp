#include "execution/compiler/operator/insert_translator.h"

#include <execution/ast/builtins.h>
#include <execution/ast/type.h>
#include <execution/compiler/if.h>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "planner/plannodes/insert_plan_node.h"

#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/work_context.h"
#include "storage/index/index.h"

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
  for (uint32_t idx = 0; idx < GetPlanAs<planner::InsertPlanNode>().GetBulkInsertCount(); idx++) {
    const auto &node_vals = GetPlanAs<planner::InsertPlanNode>().GetValues(idx);
    for (size_t i = 0; i < node_vals.size(); i++) {
      compilation_context->Prepare(*node_vals[i].Get());
    }
  }
}

// void InsertTranslator::Produce(FunctionBuilder *builder) {
//  DeclareInserter(builder);
//
//  if (op_->GetChildrenSize() != 0) {
//    // This is an insert into select so let children produce
//    child_translator_->Produce(builder);
//    GenInserterFree(builder);
//    return;
//  }
//
//  // Otherwise, this is a raw insert.
//  DeclareInsertPR(builder);
//  // For each set of values, insert into table and indexes
//  for (uint32_t idx = 0; idx < op_->GetBulkInsertCount(); idx++) {
//    // Get the table PR
//    GetInsertPR(builder);
//    // Set the table PR
//    GenSetTablePR(builder, idx);
//    // Insert into Table
//    GenTableInsert(builder);
//    // Insert into each index.
//    const auto &indexes = GetCodeGen()->GetCatalogAccessor()->GetIndexOids(op_->GetTableOid());
//    for (auto &index_oid : indexes) {
//      GenIndexInsert(builder, index_oid);
//    }
//  }
//  GenInserterFree(builder);
//}

// void InsertTranslator::Abort(FunctionBuilder *builder) const {
//  GenInserterFree(builder);
////  if (child_translator_ != nullptr) child_translator_->Abort(builder);
//  builder->Append(GetCodeGen()->Return(nullptr));
//}

void InsertTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  DeclareInserter(function);
  // Declare & Get table PR
  DeclareInsertPR(function);
  GetInsertPR(function);

  for (uint32_t idx = 0; idx < GetPlanAs<planner::InsertPlanNode>().GetBulkInsertCount(); idx++) {
    // Get the table PR
    GetInsertPR(function);
    // Set the table PR
    GenSetTablePR(function, context, idx);
    // Insert into Table
    GenTableInsert(function);
    // Insert into each index.
    const auto &indexes =
        GetCodeGen()->GetCatalogAccessor()->GetIndexOids(GetPlanAs<planner::InsertPlanNode>().GetTableOid());
    for (auto &index_oid : indexes) {
      GenIndexInsert(context, function, index_oid);
    }
  }

  GenInserterFree(function);
}

void InsertTranslator::DeclareInserter(terrier::execution::compiler::FunctionBuilder *builder) const {
  // Generate col oids
  SetOids(builder);
  // var inserter : StorageInterface
  auto storage_interface_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  builder->Append(GetCodeGen()->DeclareVar(inserter_, storage_interface_type, nullptr));
  // Call @storageInterfaceInit
  ast::Expr *inserter_setup = GetCodeGen()->StorageInterfaceInit(
      inserter_, GetExecutionContext(), !GetPlanAs<planner::InsertPlanNode>().GetTableOid(), col_oids_, true);
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
  //  return child_translator_->GetOutput(attr_idx);
  return nullptr;
}

ast::Expr *InsertTranslator::GetTableColumn(catalog::col_oid_t col_oid) const { return nullptr; }

void InsertTranslator::SetOids(FunctionBuilder *builder) const {
  // Declare: var col_oids: [num_cols]uint32
  ast::Expr *arr_type = GetCodeGen()->ArrayType(all_oids_.size(), ast::BuiltinType::Kind::Uint32);
  builder->Append(GetCodeGen()->DeclareVar(col_oids_, arr_type, nullptr));

  // For each oid, set col_oids[i] = col_oid
  for (uint16_t i = 0; i < all_oids_.size(); i++) {
    ast::Expr *lhs = GetCodeGen()->ArrayAccess(col_oids_, i);
    ast::Expr *rhs = GetCodeGen()->Const32(!all_oids_[i]);
    builder->Append(GetCodeGen()->Assign(lhs, rhs));
  }
}

void InsertTranslator::DeclareInsertPR(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var insert_pr : *ProjectedRow
  auto pr_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::ProjectedRow);
  builder->Append(GetCodeGen()->DeclareVar(insert_pr_, GetCodeGen()->PointerType(pr_type), nullptr));
}

void InsertTranslator::GetInsertPR(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var insert_pr = getTablePR(...)
  auto get_pr_call = GetCodeGen()->CallBuiltin(ast::Builtin::GetTablePR, {GetCodeGen()->AddressOf(inserter_)});
  builder->Append(GetCodeGen()->Assign(GetCodeGen()->MakeExpr(insert_pr_), get_pr_call));
}

void InsertTranslator::GenSetTablePR(FunctionBuilder *builder, WorkContext *context, uint32_t idx) const {
  const auto &node_vals = GetPlanAs<planner::InsertPlanNode>().GetValues(idx);
  for (size_t i = 0; i < node_vals.size(); i++) {
    auto &val = node_vals[i];
    auto *src = context->DeriveValue(*val.Get(), this);

    auto table_col_oid = all_oids_[i];
    const auto &table_col = table_schema_.GetColumn(table_col_oid);
    auto pr_set_call = GetCodeGen()->PRSet(GetCodeGen()->MakeExpr(insert_pr_), table_col.Type(), table_col.Nullable(),
                                           table_pm_.find(table_col_oid)->second, src, true);
    builder->Append(GetCodeGen()->MakeStmt(pr_set_call));
  }
}

void InsertTranslator::GenTableInsert(FunctionBuilder *builder) const {
  // var insert_slot = @tableInsert(&inserter_)
  auto insert_slot = GetCodeGen()->MakeFreshIdentifier("insert_slot");
  auto insert_call = GetCodeGen()->CallBuiltin(ast::Builtin::TableInsert, {GetCodeGen()->AddressOf(inserter_)});
  builder->Append(GetCodeGen()->DeclareVar(insert_slot, nullptr, insert_call));
}

void InsertTranslator::GenIndexInsert(WorkContext *context, FunctionBuilder *builder,
                                      const catalog::index_oid_t &index_oid) const {
  // var insert_index_pr = @getIndexPR(&inserter, oid)
  auto insert_index_pr = GetCodeGen()->MakeFreshIdentifier("insert_index_pr");
  std::vector<ast::Expr *> pr_call_args{GetCodeGen()->AddressOf(inserter_), GetCodeGen()->Const32(!index_oid)};
  auto get_index_pr_call = GetCodeGen()->CallBuiltin(ast::Builtin::GetIndexPR, std::move(pr_call_args));
  builder->Append(GetCodeGen()->DeclareVar(insert_index_pr, nullptr, get_index_pr_call));

  // Fill up the index pr
  auto index = GetCodeGen()->GetCatalogAccessor()->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  const auto &index_schema = GetCodeGen()->GetCatalogAccessor()->GetIndexSchema(index_oid);

  //  pr_filler_.GenFiller(index_pm, index_schema, GetCodeGen()->MakeExpr(insert_index_pr), builder);

  // Fill index_pr using table_pr
  for (const auto &index_col : index_schema.GetColumns()) {
    auto col_expr = context->DeriveValue(*index_col.StoredExpression().Get(), this);
    uint16_t attr_offset = index_pm.at(index_col.Oid());
    type::TypeId attr_type = index_col.Type();
    bool nullable = index_col.Nullable();
    auto set_key_call =
        GetCodeGen()->PRSet(GetCodeGen()->MakeExpr(insert_index_pr), attr_type, nullable, attr_offset, col_expr, true);
    builder->Append(GetCodeGen()->MakeStmt(set_key_call));
  }

  // Insert into index
  // if (insert not successfull) { Abort(); }
  //  auto index_insert_call = GetCodeGen()->CallBuiltin(
  //      index_schema.Unique() ? ast::Builtin::IndexInsertUnique : ast::Builtin::IndexInsert,
  //      {GetCodeGen()->AddressOf(inserter_)});
  //  auto cond = GetCodeGen()->UnaryOp(parsing::Token::Type::BANG, index_insert_call);
  //  If check_index_insert_call(builder, cond);
  //  {
  //    Abort(builder);
  //  }
  //  check_index_insert_call.EndIf()
}

void InsertTranslator::FillPRFromChild(terrier::execution::compiler::FunctionBuilder *builder) const {
  //  const auto &cols = table_schema_.GetColumns();
  //
  //  for (uint32_t i = 0; i < all_oids_.size(); i++) {
  //    const auto &table_col = cols[i];
  //    const auto &table_col_oid = all_oids_[i];
  //    auto val = GetChildOutput(0, i, table_col.Type());
  //    auto pr_set_call = GetCodeGen()->PRSet(GetCodeGen()->MakeExpr(insert_pr_), table_col.Type(),
  //    table_col.Nullable(),
  //                                       table_pm_.find(table_col_oid)->second, val, true);
  //    builder->Append(GetCodeGen()->MakeStmt(pr_set_call));
  //  }
}

}  // namespace terrier::execution::compiler
