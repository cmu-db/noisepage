#include "execution/compiler/operator/iter_cte_scan_leader_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier::execution::compiler {
void IterCteScanLeaderTranslator::Produce(FunctionBuilder *builder) {

  if(base_translator_ == nullptr) {
    DeclareIterCteScanIterator(builder);
    PopulateReadCteScanIterator(builder);

    child_translator_->Produce(builder);
    return;
  }

  GenInductiveLoop(builder);

  FinalizeReadCteScanIterator(builder);
}

parser::ConstantValueExpression IterCteScanLeaderTranslator::DummyLeaderCVE() {
  return terrier::parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(0));
}

IterCteScanLeaderTranslator::IterCteScanLeaderTranslator(const terrier::planner::CteScanPlanNode *op, CodeGen *codegen,
                                                         OperatorTranslator *base_case, int index)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::CTE_SCAN),
      op_(op),
      iter_cte_scan_(""),
      col_types_(""),
      insert_pr_("")
{
  (void)index;
//  inductive_case_translator_ = child_translator_;
  if(base_case == nullptr){
    iter_cte_scan_ = codegen->NewIdentifier("iter_cte_scan");
    col_types_ = codegen->NewIdentifier("col_types");
    insert_pr_ = codegen->NewIdentifier("insert_pr");
  }else{
    base_translator_ = dynamic_cast<IterCteScanLeaderTranslator*>(base_case);
    iter_cte_scan_ = base_translator_->iter_cte_scan_;
//    col_types_ = base_translator_->col_types_;
    insert_pr_ = codegen->NewIdentifier("insert_pr");
  }
  // ToDo(Gautam,Preetansh): Send the complete schema in the plan node.
  auto &all_columns = op_->GetTableOutputSchema()->GetColumns();
  codegen_->MakeIdentifier(op_->GetCTETableName());
  for (auto &col : all_columns) {
    all_types_.emplace_back(static_cast<int>(col.GetType()));
  }

  std::vector<catalog::Schema::Column> all_schema_columns;
  for (uint32_t i = 0; i < all_types_.size(); i++) {
    catalog::Schema::Column col("col" + std::to_string(i + 1), static_cast<type::TypeId>(all_types_[i]), false,
                                DummyLeaderCVE(), static_cast<catalog::col_oid_t>(i + 1));
    all_schema_columns.push_back(col);
    col_oids_.push_back(static_cast<catalog::col_oid_t>(i + 1));
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
        col_oid_to_id[column.Oid()] = storage::col_id_t(offsets[0]++);
        break;
      case 8:
        col_oid_to_id[column.Oid()] = storage::col_id_t(offsets[1]++);
        break;
      case 4:
        col_oid_to_id[column.Oid()] = storage::col_id_t(offsets[2]++);
        break;
      case 2:
        col_oid_to_id[column.Oid()] = storage::col_id_t(offsets[3]++);
        break;
      case 1:
        col_oid_to_id[column.Oid()] = storage::col_id_t(offsets[4]++);
        break;
      default:
        throw std::runtime_error("unexpected switch case value");
    }
  }

  // Use std::map to effectively sort OIDs by their corresponding ID
  std::map<storage::col_id_t, catalog::col_oid_t> inverse_map;

  // Notice the change in the inverse map argument different from sql_table get projection map function
  for (auto col_oid : col_oids_) inverse_map[col_oid_to_id[col_oid]] = col_oid;

  // Populate the projection map using the in-order iterator on std::map
  uint16_t i = 0;
  for (auto &iter : inverse_map) projection_map_[iter.second] = i++;
}

void IterCteScanLeaderTranslator::Consume(FunctionBuilder *builder) {
  // Declare & Get table PR
  DeclareInsertPR(builder);
  GetInsertPR(builder);

  // Set the values to insert
  FillPRFromChild(builder);

  // Insert into table
  GenTableInsert(builder);
}

// gets state.cte
ast::Expr *IterCteScanLeaderTranslator::GetReadCteScanIterator() {
  return codegen_->GetStateMember(codegen_->GetIdentifier(op_->GetCTETableName()));
}

ast::Expr *IterCteScanLeaderTranslator::GetIterCteScanIterator() {
  return codegen_->GetStateMemberPtr(iter_cte_scan_);
}

void IterCteScanLeaderTranslator::PopulateReadCteScanIterator(FunctionBuilder *builder) {

  ast::Expr *cte_scan_iterator_setup =
      codegen_->OneArgCall(ast::Builtin::IterCteScanGetReadCte, GetIterCteScanIterator());
  ast::Stmt *assign = codegen_->Assign(GetReadCteScanIterator(), cte_scan_iterator_setup);
  builder->Append(assign);
}

void IterCteScanLeaderTranslator::FinalizeReadCteScanIterator(FunctionBuilder *builder) {
  ast::Expr *cte_scan_iterator_setup =
      codegen_->OneArgCall(ast::Builtin::IterCteScanGetResult, GetIterCteScanIterator());
  ast::Stmt *assign = codegen_->Assign(GetReadCteScanIterator(), cte_scan_iterator_setup);
  builder->Append(assign);
}

void IterCteScanLeaderTranslator::DeclareIterCteScanIterator(FunctionBuilder *builder) {
  // Generate col types
  SetColumnTypes(builder);
  // Call @cteScanIteratorInit
  auto is_recursive = op_->GetIsRecursive();
  ast::Expr *cte_scan_iterator_setup = codegen_->IterCteScanIteratorInit(iter_cte_scan_, col_types_, is_recursive);
  builder->Append(codegen_->MakeStmt(cte_scan_iterator_setup));
}
void IterCteScanLeaderTranslator::SetColumnTypes(FunctionBuilder *builder) {
  // Declare: var col_types: [num_cols]uint32
  ast::Expr *arr_type = codegen_->ArrayType(all_types_.size(), ast::BuiltinType::Kind::Uint32);
  builder->Append(codegen_->DeclareVariable(col_types_, arr_type, nullptr));

  // For each oid, set col_oids[i] = col_oid
  for (uint16_t i = 0; i < all_types_.size(); i++) {
    ast::Expr *lhs = codegen_->ArrayAccess(col_types_, i);
    ast::Expr *rhs = codegen_->IntLiteral(all_types_[i]);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}

void IterCteScanLeaderTranslator::DeclareInsertPR(FunctionBuilder *builder) {
  // var insert_pr : *ProjectedRow
  auto pr_type = codegen_->BuiltinType(ast::BuiltinType::Kind::ProjectedRow);
  builder->Append(codegen_->DeclareVariable(insert_pr_, codegen_->PointerType(pr_type), nullptr));
}

void IterCteScanLeaderTranslator::GetInsertPR(terrier::execution::compiler::FunctionBuilder *builder) {
  // var insert_pr = cteScanGetInsertTempTablePR(...)
  auto get_pr_call = codegen_->OneArgCall(ast::Builtin::IterCteScanGetInsertTempTablePR,
                                            GetIterCteScanIterator());
  builder->Append(codegen_->Assign(codegen_->MakeExpr(insert_pr_), get_pr_call));
}

void IterCteScanLeaderTranslator::GenTableInsert(FunctionBuilder *builder) {
  // var insert_slot = @cteScanTableInsert(&inserter_)
  auto insert_slot = codegen_->NewIdentifier("insert_slot");
  auto insert_call = codegen_->OneArgCall(ast::Builtin::IterCteScanTableInsert,
                                          GetIterCteScanIterator());
  builder->Append(codegen_->DeclareVariable(insert_slot, nullptr, insert_call));
}

void IterCteScanLeaderTranslator::FillPRFromChild(terrier::execution::compiler::FunctionBuilder *builder) {
  const auto &cols = op_->GetTableOutputSchema()->GetColumns();

  for (uint32_t i = 0; i < col_oids_.size(); i++) {
    const auto &table_col = cols[i];
    const auto &table_col_oid = col_oids_[i];
    auto val = GetChildOutput(0, i, table_col.GetType());
    // TODO(Rohan): Figure how to get the general schema of a child node in case the field is Nullable
    // Right now it is only Non Null
    auto pr_set_call = codegen_->PRSet(codegen_->MakeExpr(insert_pr_), table_col.GetType(), false,
                                       projection_map_[table_col_oid], val, true);
    builder->Append(codegen_->MakeStmt(pr_set_call));
  }
}



void IterCteScanLeaderTranslator::GenInductiveLoop(FunctionBuilder *builder) {
  builder->StartForStmt(nullptr, codegen_->OneArgCall(ast::Builtin::IterCteScanAccumulate,
                                                     GetIterCteScanIterator()),
                        nullptr);
  PopulateReadCteScanIterator(builder);
  child_translator_->Produce(builder);
  builder->FinishBlockStmt();
}

}  // namespace terrier::execution::compiler
