#include "execution/compiler/operator/cte_scan_leader_translator.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/constant_value_expression.h"
//
namespace terrier::execution::compiler {
//void CteScanLeaderTranslator::Produce(FunctionBuilder *builder) {
//  DeclareCteScanIterator(builder);
//  child_translator_->Produce(builder);
//}
//
parser::ConstantValueExpression DummyLeaderCVE() {
  return terrier::parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(0));
}
//
CteScanLeaderTranslator::CteScanLeaderTranslator(const planner::CteScanPlanNode &plan,
                                                 CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::CTE_SCAN),
      op_(&plan),
      col_types_(GetCodeGen()->MakeFreshIdentifier("col_types")),
      insert_pr_(GetCodeGen()->MakeFreshIdentifier("insert_pr")),
      build_pipeline_(this, Pipeline::Parallelism::Parallel){
  // ToDo(Gautam,Preetansh): Send the complete schema in the plan node.
  auto &all_columns = op_->GetTableOutputSchema()->GetColumns();

  auto cte_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::CteScanIterator);
  cte_scan_val_entry_ = compilation_context->GetQueryState()->DeclareStateEntry(GetCodeGen(),
                                                                                op_->GetCTETableName() + "val",
                                                                                cte_type);
  cte_scan_ptr_entry_ = compilation_context->GetQueryState()->DeclareStateEntry(GetCodeGen(),
                                                                                op_->GetCTETableName() + "ptr",
                                                                                GetCodeGen()->PointerType(cte_type));
  for (auto &col : all_columns) {
    all_types_.emplace_back(static_cast<int>(col.GetType()));
  }

  std::vector<catalog::Schema::Column> all_schema_columns;
  for (uint32_t i = 0; i < all_types_.size(); i++) {
    catalog::Schema::Column col("col" + std::to_string(i + 1), static_cast<type::TypeId>(all_types_[i]), false,
                                DummyLeaderCVE(), static_cast<catalog::col_oid_t>(i + 1));
    all_schema_columns.push_back(col);
    col_oids_.push_back(static_cast<catalog::col_oid_t>(i + 1));
    col_name_to_oid_[all_columns[i].GetName()] = i + 1;
  }

  // Create the table in the catalog.
  catalog::Schema schema(all_schema_columns);

  std::vector<uint16_t> attr_sizes;
  attr_sizes.reserve(storage::NUM_RESERVED_COLUMNS + schema.GetColumns().size());

  for (uint8_t i = 0; i < storage::NUM_RESERVED_COLUMNS; i++) {
    attr_sizes.emplace_back(8);
  }

  TERRIER_ASSERT(attr_sizes.size() == storage::NUM_RESERVED_COLUMNS,
                 "attr_sizes should be initialized with NUM_RESERVED_COLUMNS elements.");

  for (const auto &column : schema.GetColumns()) {
    attr_sizes.push_back(column.AttrSize());
  }

  auto offsets = storage::StorageUtil::ComputeBaseAttributeOffsets(attr_sizes, storage::NUM_RESERVED_COLUMNS);

  storage::ColumnMap col_oid_to_id;
  for (const auto &column : schema.GetColumns()) {
    switch (column.AttrSize()) {
      case storage::VARLEN_COLUMN:
        col_oid_to_id[column.Oid()] = {storage::col_id_t(offsets[0]++), column.Type()};
        break;
      case 8:
        col_oid_to_id[column.Oid()] = {storage::col_id_t(offsets[1]++), column.Type()};
        break;
      case 4:
        col_oid_to_id[column.Oid()] = {storage::col_id_t(offsets[1]++), column.Type()};
        break;
      case 2:
        col_oid_to_id[column.Oid()] = {storage::col_id_t(offsets[3]++), column.Type()};
        break;
      case 1:
        col_oid_to_id[column.Oid()] = {storage::col_id_t(offsets[4]++), column.Type()};
        break;
      default:
        throw std::runtime_error("unexpected switch case value");
    }
  }

  // Use std::map to effectively sort OIDs by their corresponding ID
  std::map<storage::col_id_t, catalog::col_oid_t> inverse_map;

  // Notice the change in the inverse map argument different from sql_table get projection map function
  for (auto col_oid : col_oids_) inverse_map[col_oid_to_id[col_oid].col_id_] = col_oid;

  // Populate the projection map using the in-order iterator on std::map
  uint16_t i = 0;
  for (auto &iter : inverse_map) projection_map_[iter.second] = i++;

  pipeline->LinkSourcePipeline(&build_pipeline_);
  compilation_context->Prepare(*(op_->GetChild(0)), &build_pipeline_);
}

void CteScanLeaderTranslator::TearDownQueryState(FunctionBuilder *function) const {
ast::Expr *cte_free_call =
    GetCodeGen()->CallBuiltin(ast::Builtin::CteScanFree, {cte_scan_ptr_entry_.Get(GetCodeGen())});
function->Append(GetCodeGen()->MakeStmt(cte_free_call));
}

void CteScanLeaderTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  if(&context->GetPipeline() != &build_pipeline_){
    context->Push(function);
    return;
  }

  // Declare & Get table PR
  DeclareInsertPR(function);
  GetInsertPR(function);

  // Set the values to insert
  FillPRFromChild(context, function);

  // Insert into table
  GenTableInsert(function);
}

void CteScanLeaderTranslator::DeclareCteScanIterator(FunctionBuilder *builder) const {
  // Generate col types
  auto codegen = GetCodeGen();
  SetColumnTypes(builder);
  // Call @cteScanIteratorInit
  ast::Expr *cte_scan_iterator_setup = codegen->CteScanIteratorInit(cte_scan_val_entry_.GetPtr(codegen), col_types_,
                                                                    GetCompilationContext()->
                                                                    GetExecutionContextPtrFromQueryState());
  builder->Append(codegen->MakeStmt(cte_scan_iterator_setup));

  ast::Stmt *pointer_setup = codegen->Assign(cte_scan_ptr_entry_.Get(codegen),
                                              cte_scan_val_entry_.GetPtr(codegen));
  builder->Append(pointer_setup);
}

void CteScanLeaderTranslator::SetColumnTypes(FunctionBuilder *builder) const {
  // Declare: var col_types: [num_cols]uint32
  auto codegen = GetCodeGen();
  ast::Expr *arr_type = codegen->ArrayType(all_types_.size(), ast::BuiltinType::Kind::Uint32);
  builder->Append(codegen->DeclareVar(col_types_, arr_type, nullptr));

  // For each oid, set col_oids[i] = col_oid
  for (uint16_t i = 0; i < all_types_.size(); i++) {
    ast::Expr *lhs = codegen->ArrayAccess(col_types_, i);
    ast::Expr *rhs = codegen->Const32(all_types_[i]);
    builder->Append(codegen->Assign(lhs, rhs));
  }
}

void CteScanLeaderTranslator::InitializeQueryState(FunctionBuilder *function) const {
  DeclareCteScanIterator(function);
}

ast::Expr *CteScanLeaderTranslator::GetCteScanPtr(CodeGen *codegen) const {
  return cte_scan_ptr_entry_.Get(codegen);
}

void CteScanLeaderTranslator::DeclareInsertPR(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var insert_pr : *ProjectedRow
  auto codegen = GetCodeGen();
  auto pr_type = codegen->BuiltinType(ast::BuiltinType::Kind::ProjectedRow);
  builder->Append(codegen->DeclareVar(insert_pr_, codegen->PointerType(pr_type), nullptr));
}

void CteScanLeaderTranslator::GetInsertPR(terrier::execution::compiler::FunctionBuilder *builder) const {
  // var insert_pr = cteScanGetInsertTempTablePR(...)
  auto codegen = GetCodeGen();
  auto get_pr_call = codegen->CallBuiltin(ast::Builtin::CteScanGetInsertTempTablePR,
                                          {GetCteScanPtr(codegen)});
  builder->Append(codegen->Assign(codegen->MakeExpr(insert_pr_), get_pr_call));
}

void CteScanLeaderTranslator::GenTableInsert(FunctionBuilder *builder) const {
  // var insert_slot = @cteScanTableInsert(&inserter_)
  auto codegen = GetCodeGen();
  auto insert_slot = codegen->MakeFreshIdentifier("insert_slot");
  auto insert_call = codegen->CallBuiltin(ast::Builtin::CteScanTableInsert,
                                          {GetCteScanPtr(codegen)});
  builder->Append(codegen->DeclareVar(insert_slot, nullptr, insert_call));
}

void CteScanLeaderTranslator::FillPRFromChild(WorkContext *context, FunctionBuilder *builder) const {
  const auto &cols = op_->GetTableOutputSchema()->GetColumns();
  auto codegen = GetCodeGen();

  for (uint32_t i = 0; i < col_oids_.size(); i++) {
    const auto &table_col = cols[i];
    const auto &table_col_oid = col_oids_[i];
    auto val = GetChildOutput(context, 0, i);
    // TODO(Rohan): Figure how to get the general schema of a child node in case the field is Nullable
    // Right now it is only Non Null
    auto pr_set_call = codegen->PRSet(codegen->MakeExpr(insert_pr_), table_col.GetType(), false,
                                       projection_map_.find(table_col_oid)->second, val, true);
    builder->Append(codegen->MakeStmt(pr_set_call));
  }
}
}  // namespace terrier::execution::compiler
