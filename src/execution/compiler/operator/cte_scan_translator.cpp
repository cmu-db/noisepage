#include "execution/compiler/operator/cte_scan_translator.h"

#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "execution/compiler/operator/cte_scan_leader_translator.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier::execution::compiler {

parser::ConstantValueExpression DummyCVE() {
  return terrier::parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(0));
}

CteScanTranslator::CteScanTranslator(const planner::CteScanPlanNode &plan, CompilationContext *compilation_context,
                                     Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::CTE_SCAN),
      op_(&plan),
      col_types_(GetCodeGen()->MakeFreshIdentifier("col_types")),
      insert_pr_(GetCodeGen()->MakeFreshIdentifier("insert_pr")),
      read_col_oids_(GetCodeGen()->MakeFreshIdentifier("read_col_oids")),
      read_tvi_(GetCodeGen()->MakeFreshIdentifier("temp_table_iterator")),
      read_vpi_(GetCodeGen()->MakeFreshIdentifier("read_pci")),
      read_slot_(GetCodeGen()->MakeFreshIdentifier("read_slot")) {
  // ToDo(Gautam,Preetansh): Send the complete schema in the plan node.
  auto &all_columns = op_->GetTableOutputSchema()->GetColumns();

  for (auto &col : all_columns) {
    all_types_.emplace_back(static_cast<int>(col.GetType()));
  }

  std::vector<catalog::Schema::Column> all_schema_columns;
  for (uint32_t i = 0; i < all_types_.size(); i++) {
    catalog::Schema::Column col("col" + std::to_string(i + 1), static_cast<type::TypeId>(all_types_[i]), false,
                                DummyCVE(), static_cast<catalog::col_oid_t>(i + 1));
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

    if(plan.GetChildrenSize() > 0){
      compilation_context->Prepare(*(plan.GetChild(0)), pipeline);
    }
  }

  // Use std::map to effectively sort OIDs by their corresponding ID
  std::map<storage::col_id_t, catalog::col_oid_t> inverse_map;

  // Notice the change in the inverse map argument different from sql_table get projection map function
  for (auto col_oid : col_oids_) inverse_map[col_oid_to_id[col_oid].col_id_] = col_oid;

  // Populate the projection map using the in-order iterator on std::map
  uint16_t i = 0;
  for (auto &iter : inverse_map) projection_map_[iter.second-1] = i++;

  schema_ = schema;
}

ast::Expr *CteScanTranslator::GetCteScanIterator() const {
  auto leader_translator = reinterpret_cast<CteScanLeaderTranslator*>(
      GetCompilationContext()->LookupTranslator(*op_->GetLeader()));
  return leader_translator->GetCteScanPtr(GetCodeGen());
}


void CteScanTranslator::SetReadOids(FunctionBuilder *builder) const {
  // Declare: var col_oids: [num_cols]uint32
  ast::Expr *arr_type = GetCodeGen()->ArrayType(col_oids_.size(), ast::BuiltinType::Kind::Uint32);
  builder->Append(GetCodeGen()->DeclareVar(read_col_oids_, arr_type, nullptr));

  // For each oid, set col_oids[i] = col_oid
  for (uint16_t i = 0; i < col_oids_.size(); i++) {
    ast::Expr *lhs = GetCodeGen()->ArrayAccess(read_col_oids_, i);
    ast::Expr *rhs = GetCodeGen()->Const32(!col_oids_[i]);
    builder->Append(GetCodeGen()->Assign(lhs, rhs));
  }
}

void CteScanTranslator::DeclareReadTVI(FunctionBuilder *builder) const {
  // var tvi: TableVectorIterator
  ast::Expr *iter_type = GetCodeGen()->BuiltinType(ast::BuiltinType::Kind::TableVectorIterator);
  builder->Append(GetCodeGen()->DeclareVar(read_tvi_, iter_type, nullptr));

  // Call @tableIterInit(&tvi, execCtx, table_oid, col_oids)
  ast::Expr *init_call = GetCodeGen()->TempTableIterInit(read_tvi_,
                                                    GetCteScanIterator(),
                                                     read_col_oids_,
                                                         GetCompilationContext()->GetExecutionContextPtrFromQueryState());
  builder->Append(GetCodeGen()->MakeStmt(init_call));

  builder->Append(GetCodeGen()->DeclareVarNoInit(read_slot_, GetCodeGen()->BuiltinType(ast::BuiltinType::TupleSlot)));
}
void CteScanTranslator::GenReadTVIClose(FunctionBuilder *builder) const {
  // Close iterator
  ast::Expr *close_call = GetCodeGen()->CallBuiltin(ast::Builtin::TableIterClose, {GetCodeGen()->AddressOf(read_tvi_)});
  builder->Append(GetCodeGen()->MakeStmt(close_call));
}

void CteScanTranslator::DoTableScan(WorkContext *context, FunctionBuilder *builder) const {
  // Start looping over the table
  auto codegen = GetCodeGen();
  ast::Expr *advance_call = codegen->CallBuiltin(ast::Builtin::TableIterAdvance,
                                                      {codegen->AddressOf(read_tvi_)});
  Loop tvi_loop(builder, nullptr, advance_call, nullptr);
  {
    auto vpi = codegen->MakeExpr(read_vpi_);
    builder->Append(codegen->DeclareVarWithInit(read_vpi_,
                                                codegen->TableIterGetVPI(codegen->AddressOf(read_tvi_))));
    Loop vpi_loop(builder, nullptr, codegen->VPIHasNext(vpi, false),
                  codegen->MakeStmt(codegen->VPIAdvance(vpi, false)));
    {
      if (op_->GetScanPredicate() != nullptr) {
        ast::Expr *cond = context->DeriveValue(*op_->GetScanPredicate(), this);

        If predicate(builder, cond);
        {
          // Declare Slot.
          DeclareSlot(builder);
          // Let parent consume.
          context->Push(builder);
        }
        predicate.EndIf();
      } else {
        DeclareSlot(builder);
        context->Push(builder);
      }
    }
    vpi_loop.EndLoop();
  }
  tvi_loop.EndLoop();
  GenReadTVIClose(builder);
}

void CteScanTranslator::DeclareSlot(FunctionBuilder *builder) const {
  // var slot = @tableIterGetSlot(vpi)
  auto codegen = GetCodeGen();
  auto make_slot = codegen->CallBuiltin(ast::Builtin::VPIGetSlot, {codegen->MakeExpr(read_vpi_)});
  auto assign = codegen->Assign(codegen->MakeExpr(read_slot_), make_slot);
  builder->Append(assign);
}

//void CteScanTranslator::GenReadTVIReset(FunctionBuilder *builder) {
//  ast::Expr *reset_call = GetCodeGen()->CallBuiltin(ast::Builtin::TableIterReset, read_tvi_, true);
//  builder->Append(GetCodeGen()->MakeStmt(reset_call));
//}
void CteScanTranslator::TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  OperatorTranslator::TearDownPipelineState(pipeline, function);
}
void CteScanTranslator::InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  OperatorTranslator::TearDownPipelineState(pipeline, function);
}
void CteScanTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  SetReadOids(function);
  DeclareReadTVI(function);
  DoTableScan(context, function);
}
ast::Expr *CteScanTranslator::GetTableColumn(catalog::col_oid_t col_oid) const {
  auto type = static_cast<type::TypeId>(all_types_[!col_oid]);
  auto nullable = false;
  uint16_t attr_idx = projection_map_.find(col_oid)->second;
  return GetCodeGen()->VPIGet(GetCodeGen()->MakeExpr(read_vpi_), sql::GetTypeId(type), nullable, attr_idx);
}

ast::Expr *CteScanTranslator::GetSlotAddress() const { return GetCodeGen()->AddressOf(read_slot_); }

}  // namespace terrier::execution::compiler
