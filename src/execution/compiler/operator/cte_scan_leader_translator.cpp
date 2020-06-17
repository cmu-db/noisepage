#include "execution/compiler/operator/cte_scan_leader_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier::execution::compiler {
void CteScanLeaderTranslator::Produce(FunctionBuilder *builder) {
  DeclareCteScanIterator(builder);
  child_translator_->Produce(builder);
}

parser::ConstantValueExpression DummyLeaderCVE() {
  return terrier::parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(0));
}

CteScanLeaderTranslator::CteScanLeaderTranslator(const terrier::planner::CteScanPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::CTE_SCAN),
      op_(op),
      col_types_(codegen->NewIdentifier("col_types")),
      insert_pr_(codegen->NewIdentifier("insert_pr")),
      read_col_oids_(codegen_->NewIdentifier("read_col_oids")),
      read_tvi_(codegen_->NewIdentifier("temp_table_iterator")),
      read_pci_(codegen_->NewIdentifier("read_pci")) {
  // ToDo(Gautam,Preetansh): Send the complete schema in the plan node.
  auto &all_columns = op_->GetTableOutputSchema()->GetColumns();
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

void CteScanLeaderTranslator::Consume(FunctionBuilder *builder) {
  // Declare & Get table PR
  DeclareInsertPR(builder);
  GetInsertPR(builder);

  // Set the values to insert
  FillPRFromChild(builder);

  // Insert into table
  GenTableInsert(builder);
}
void CteScanLeaderTranslator::DeclareCteScanIterator(FunctionBuilder *builder) {
  // Generate col types
  SetColumnTypes(builder);
  // Call @cteScanIteratorInit
  ast::Expr *cte_scan_iterator_setup = codegen_->CteScanIteratorInit(codegen_->GetCteScanIdentifier(), col_types_);
  builder->Append(codegen_->MakeStmt(cte_scan_iterator_setup));
}
void CteScanLeaderTranslator::SetColumnTypes(FunctionBuilder *builder) {
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

void CteScanLeaderTranslator::DeclareInsertPR(terrier::execution::compiler::FunctionBuilder *builder) {
  // var insert_pr : *ProjectedRow
  auto pr_type = codegen_->BuiltinType(ast::BuiltinType::Kind::ProjectedRow);
  builder->Append(codegen_->DeclareVariable(insert_pr_, codegen_->PointerType(pr_type), nullptr));
}

void CteScanLeaderTranslator::GetInsertPR(terrier::execution::compiler::FunctionBuilder *builder) {
  // var insert_pr = cteScanGetInsertTempTablePR(...)
  auto get_pr_call = codegen_->OneArgCall(ast::Builtin::CteScanGetInsertTempTablePR,
                                          codegen_->GetStateMemberPtr(codegen_->GetCteScanIdentifier()));
  builder->Append(codegen_->Assign(codegen_->MakeExpr(insert_pr_), get_pr_call));
}

void CteScanLeaderTranslator::GenTableInsert(FunctionBuilder *builder) {
  // var insert_slot = @cteScanTableInsert(&inserter_)
  auto insert_slot = codegen_->NewIdentifier("insert_slot");
  auto insert_call = codegen_->OneArgCall(ast::Builtin::CteScanTableInsert,
                                          codegen_->GetStateMemberPtr(codegen_->GetCteScanIdentifier()));
  builder->Append(codegen_->DeclareVariable(insert_slot, nullptr, insert_call));
}

void CteScanLeaderTranslator::FillPRFromChild(terrier::execution::compiler::FunctionBuilder *builder) {
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
void CteScanLeaderTranslator::SetReadOids(FunctionBuilder *builder) {
  // Declare: var col_oids: [num_cols]uint32
  ast::Expr *arr_type = codegen_->ArrayType(col_oids_.size(), ast::BuiltinType::Kind::Uint32);
  builder->Append(codegen_->DeclareVariable(read_col_oids_, arr_type, nullptr));

  // For each oid, set col_oids[i] = col_oid
  for (uint16_t i = 0; i < col_oids_.size(); i++) {
    ast::Expr *lhs = codegen_->ArrayAccess(read_col_oids_, i);
    ast::Expr *rhs = codegen_->IntLiteral(!col_oids_[i]);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}
void CteScanLeaderTranslator::DeclareReadTVI(FunctionBuilder *builder) {
  // var tvi: TableVectorIterator
  ast::Expr *iter_type = codegen_->BuiltinType(ast::BuiltinType::Kind::TableVectorIterator);
  builder->Append(codegen_->DeclareVariable(read_tvi_, iter_type, nullptr));

  // Call @tableIterInit(&tvi, execCtx, table_oid, col_oids)
  ast::Expr *init_call = codegen_->TempTableIterInit(read_tvi_, codegen_->GetCteScanIdentifier(), read_col_oids_);
  builder->Append(codegen_->MakeStmt(init_call));
}
void CteScanLeaderTranslator::GenReadTVIClose(FunctionBuilder *builder) {
  // Close iterator
  ast::Expr *close_call = codegen_->OneArgCall(ast::Builtin::TableIterClose, read_tvi_, true);
  builder->Append(codegen_->MakeStmt(close_call));
}
void CteScanLeaderTranslator::DoTableScan(FunctionBuilder *builder) {
  // Start looping over the table
  GenTVILoop(builder);
  DeclarePCI(builder);
  GenPCILoop(builder);
  // Declare Slot.
  DeclareSlot(builder);
  // Let parent consume.
  parent_translator_->Consume(builder);

  // Close PCI loop
  builder->FinishBlockStmt();
  // Close TVI loop
  builder->FinishBlockStmt();
}

// Generate for(@tableIterAdvance(&tvi)) {...}
void CteScanLeaderTranslator::GenTVILoop(FunctionBuilder *builder) {
  // The advance call
  ast::Expr *advance_call = codegen_->OneArgCall(ast::Builtin::TableIterAdvance, read_tvi_, true);
  builder->StartForStmt(nullptr, advance_call, nullptr);
}

void CteScanLeaderTranslator::DeclarePCI(FunctionBuilder *builder) {
  // Assign var pci = @tableIterGetPCI(&tvi)
  ast::Expr *get_pci_call = codegen_->OneArgCall(ast::Builtin::TableIterGetPCI, read_tvi_, true);
  builder->Append(codegen_->DeclareVariable(read_pci_, nullptr, get_pci_call));
}

void CteScanLeaderTranslator::DeclareSlot(FunctionBuilder *builder) {
  // Get var slot = @pciGetSlot(pci)
  auto read_slot = codegen_->NewIdentifier("read_slot");
  ast::Expr *get_slot_call = codegen_->OneArgCall(ast::Builtin::PCIGetSlot, read_pci_, false);
  builder->Append(codegen_->DeclareVariable(read_slot, nullptr, get_slot_call));
}

void CteScanLeaderTranslator::GenPCILoop(FunctionBuilder *builder) {
  // Generate for(; @pciHasNext(pci); @pciAdvance(pci)) {...} or the Filtered version
  // The HasNext call
  ast::Builtin has_next_fn = ast::Builtin::PCIHasNext;
  ast::Expr *has_next_call = codegen_->OneArgCall(has_next_fn, read_pci_, false);
  // The Advance call
  ast::Builtin advance_fn = ast::Builtin::PCIAdvance;
  ast::Expr *advance_call = codegen_->OneArgCall(advance_fn, read_pci_, false);
  ast::Stmt *loop_advance = codegen_->MakeStmt(advance_call);
  // Make the for loop.
  builder->StartForStmt(nullptr, has_next_call, loop_advance);
}

}  // namespace terrier::execution::compiler
