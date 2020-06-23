//#include "execution/compiler/operator/insert_translator.h"
//
//#include <utility>
//#include <vector>
//
//#include "catalog/catalog_accessor.h"
//#include "execution/compiler/function_builder.h"
//#include "execution/compiler/translator_factory.h"
//#include "storage/index/index.h"
//
//namespace terrier::execution::compiler {
//InsertTranslator::InsertTranslator(const planner::InsertPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline)
//    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::INSERT)
//      inserter_(GetCodeGen()->MakeFreshIdentifier("inserter")),
//      insert_pr_(GetCodeGen()->MakeFreshIdentifier("insert_pr")),
//      col_oids_(GetCodeGen()->MakeFreshIdentifier("col_oids")),
//      table_schema_(GetCodeGen()->GetCatalogAccessor()->GetSchema(op_->GetTableOid())),
//      all_oids_(AllColOids(table_schema_)),
//      table_pm_(GetCodeGen()->GetCatalogAccessor()->GetTable(op_->GetTableOid())->ProjectionMapForOids(all_oids_)),
//      pr_filler_(GetCodeGen(), table_schema_, table_pm_, insert_pr_) {}
//
//void InsertTranslator::Produce(FunctionBuilder *builder) {
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
//    const auto &indexes = GetCodeGen()->Accessor()->GetIndexOids(op_->GetTableOid());
//    for (auto &index_oid : indexes) {
//      GenIndexInsert(builder, index_oid);
//    }
//  }
//  GenInserterFree(builder);
//}
//
//void InsertTranslator::Abort(FunctionBuilder *builder) {
//  GenInserterFree(builder);
//  if (child_translator_ != nullptr) child_translator_->Abort(builder);
//  builder->Append(GetCodeGen()->ReturnStmt(nullptr));
//}
//
//void InsertTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
//  // Declare & Get table PR
//  DeclareInsertPR(function);
//  GetInsertPR(function);
//
//  // Set the values to insert
//  FillPRFromChild(function);
//
//  // Insert into table
//  GenTableInsert(function);
//
//  // Insert into every index
//  const auto &indexes = GetCodeGen()->Accessor()->GetIndexOids(op_->GetTableOid());
//  for (auto &index_oid : indexes) {
//    GenIndexInsert(function, index_oid);
//  }
//}
//
//void InsertTranslator::DeclareInserter(terrier::execution::compiler::FunctionBuilder *builder) {
//  // Generate col oids
//  SetOids(builder);
//  // var inserter : StorageInterface
//  auto storage_interface_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
//  builder->Append(GetCodeGen()->DeclareVariable(inserter_, storage_interface_type, nullptr));
//  // Call @storageInterfaceInit
//  ast::Expr *inserter_setup = GetCodeGen()->StorageInterfaceInit(inserter_, !op_->GetTableOid(), col_oids_, true);
//  builder->Append(GetCodeGen()->MakeStmt(inserter_setup));
//}
//
//void InsertTranslator::GenInserterFree(terrier::execution::compiler::FunctionBuilder *builder) {
//  // Call @storageInterfaceFree
//  ast::Expr *inserter_free = GetCodeGen()->OneArgCall(ast::Builtin::StorageInterfaceFree, inserter_, true);
//  builder->Append(GetCodeGen()->MakeStmt(inserter_free));
//}
//
//ast::Expr *InsertTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) {
//  TERRIER_ASSERT(child_idx == 0, "Insert plan can only have one child");
//  return child_translator_->GetOutput(attr_idx);
//}
//
//void InsertTranslator::SetOids(FunctionBuilder *builder) {
//  // Declare: var col_oids: [num_cols]uint32
//  ast::Expr *arr_type = GetCodeGen()->ArrayType(all_oids_.size(), ast::BuiltinType::Kind::Uint32);
//  builder->Append(GetCodeGen()->DeclareVariable(col_oids_, arr_type, nullptr));
//
//  // For each oid, set col_oids[i] = col_oid
//  for (uint16_t i = 0; i < all_oids_.size(); i++) {
//    ast::Expr *lhs = GetCodeGen()->ArrayAccess(col_oids_, i);
//    ast::Expr *rhs = GetCodeGen()->IntLiteral(!all_oids_[i]);
//    builder->Append(GetCodeGen()->Assign(lhs, rhs));
//  }
//}
//
//void InsertTranslator::DeclareInsertPR(terrier::execution::compiler::FunctionBuilder *builder) const {
//  // var insert_pr : *ProjectedRow
//  auto pr_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::ProjectedRow);
//  builder->Append(GetCodeGen()->DeclareVariable(insert_pr_, GetCodeGen()->PointerType(pr_type), nullptr));
//}
//
//void InsertTranslator::GetInsertPR(terrier::execution::compiler::FunctionBuilder *builder) {
//  // var insert_pr = getTablePR(...)
//  auto get_pr_call = GetCodeGen()->OneArgCall(ast::Builtin::GetTablePR, inserter_, true);
//  builder->Append(GetCodeGen()->Assign(GetCodeGen()->MakeExpr(insert_pr_), get_pr_call));
//}
//
//void InsertTranslator::GenSetTablePR(FunctionBuilder *builder, uint32_t idx) {
//  const auto &node_vals = op_->GetValues(idx);
//  for (size_t i = 0; i < node_vals.size(); i++) {
//    auto &val = node_vals[i];
//    auto translator = TranslatorFactory::CreateExpressionTranslator(val.Get(), GetCodeGen());
//
//    auto *src = translator->DeriveExpr(this);
//    auto table_col_oid = all_oids_[i];
//    const auto &table_col = table_schema_.GetColumn(table_col_oid);
//    auto pr_set_call = GetCodeGen()->PRSet(GetCodeGen()->MakeExpr(insert_pr_), table_col.Type(), table_col.Nullable(),
//                                       table_pm_[table_col_oid], src, true);
//    builder->Append(GetCodeGen()->MakeStmt(pr_set_call));
//  }
//}
//
//void InsertTranslator::GenTableInsert(FunctionBuilder *builder) {
//  // var insert_slot = @tableInsert(&inserter_)
//  auto insert_slot = GetCodeGen()->NewIdentifier("insert_slot");
//  auto insert_call = GetCodeGen()->OneArgCall(ast::Builtin::TableInsert, inserter_, true);
//  builder->Append(GetCodeGen()->DeclareVariable(insert_slot, nullptr, insert_call));
//}
//
//void InsertTranslator::GenIndexInsert(FunctionBuilder *builder, const catalog::index_oid_t &index_oid) {
//  // var insert_index_pr = @getIndexPR(&inserter, oid)
//  auto insert_index_pr = GetCodeGen()->NewIdentifier("insert_index_pr");
//  std::vector<ast::Expr *> pr_call_args{GetCodeGen()->PointerTo(inserter_), GetCodeGen()->IntLiteral(!index_oid)};
//  auto get_index_pr_call = GetCodeGen()->BuiltinCall(ast::Builtin::GetIndexPR, std::move(pr_call_args));
//  builder->Append(GetCodeGen()->DeclareVariable(insert_index_pr, nullptr, get_index_pr_call));
//
//  // Fill up the index pr
//  auto index = GetCodeGen()->Accessor()->GetIndex(index_oid);
//  const auto &index_pm = index->GetKeyOidToOffsetMap();
//  const auto &index_schema = GetCodeGen()->Accessor()->GetIndexSchema(index_oid);
//
//  pr_filler_.GenFiller(index_pm, index_schema, GetCodeGen()->MakeExpr(insert_index_pr), builder);
//
//  // Insert into index
//  // if (insert not successfull) { Abort(); }
//  auto index_insert_call = GetCodeGen()->OneArgCall(
//      index_schema.Unique() ? ast::Builtin::IndexInsertUnique : ast::Builtin::IndexInsert, inserter_, true);
//  auto cond = GetCodeGen()->UnaryOp(parsing::Token::Type::BANG, index_insert_call);
//  builder->StartIfStmt(cond);
//  Abort(builder);
//  builder->FinishBlockStmt();
//}
//
//void InsertTranslator::FillPRFromChild(terrier::execution::compiler::FunctionBuilder *builder) {
//  const auto &cols = table_schema_.GetColumns();
//
//  for (uint32_t i = 0; i < all_oids_.size(); i++) {
//    const auto &table_col = cols[i];
//    const auto &table_col_oid = all_oids_[i];
//    auto val = GetChildOutput(0, i, table_col.Type());
//    auto pr_set_call = GetCodeGen()->PRSet(GetCodeGen()->MakeExpr(insert_pr_), table_col.Type(), table_col.Nullable(),
//                                       table_pm_[table_col_oid], val, true);
//    builder->Append(GetCodeGen()->MakeStmt(pr_set_call));
//  }
//}
//
//}  // namespace terrier::execution::compiler
