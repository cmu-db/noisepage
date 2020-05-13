#include "execution/compiler/operator/analyze_translator.h"

#include <utility>
#include "execution/ast/type.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/translator_factory.h"
#include "parser/expression/constant_value_expression.h"
#include "planner/plannodes/analyze_plan_node.h"

namespace terrier::execution::compiler {

AnalyzeBottomTranslator::AnalyzeBottomTranslator(const terrier::planner::AnalyzePlanNode *op, CodeGen *codegen,
                                                 std::unique_ptr<planner::AggregatePlanNode> &&agg_plan_node,
                                                 std::vector<std::unique_ptr<parser::AbstractExpression>> &&owned_exprs)
    : StaticAggregateBottomTranslator(agg_plan_node.get(), codegen),
      analyze_plan_node_(op),
      agg_plan_node_(std::move(agg_plan_node)),
      owned_exprs_(std::move(owned_exprs)) {}

std::unique_ptr<AnalyzeBottomTranslator> AnalyzeBottomTranslator::Create(const planner::AnalyzePlanNode *op,
                                                                         CodeGen *codegen) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> owned_exprs;
  auto agg_plan_node = MakeAggregatePlanNode(op, &owned_exprs);
  return std::make_unique<AnalyzeBottomTranslator>(op, codegen, std::move(agg_plan_node), std::move(owned_exprs));
}

std::unique_ptr<planner::AggregatePlanNode> AnalyzeBottomTranslator::MakeAggregatePlanNode(
    const planner::AnalyzePlanNode *op, std::vector<std::unique_ptr<parser::AbstractExpression>> *owned_exprs) {
  planner::AggregatePlanNode::Builder builder;
  std::vector<std::unique_ptr<parser::AbstractExpression>> child_exprs;

  // COUNT(1), i.e. total number of rows
  child_exprs.emplace_back(
      std::make_unique<parser::ConstantValueExpression>(type::TransientValueFactory::GetInteger(1)));
  const auto &count_const_expr = owned_exprs->emplace_back(std::make_unique<parser::AggregateExpression>(
      parser::ExpressionType::AGGREGATE_COUNT, std::move(child_exprs), true));
  builder.AddAggregateTerm(common::ManagedPointer(static_cast<parser::AggregateExpression *>(count_const_expr.get())));

  // Consider all column IDs
  for (const auto &col_oid : op->GetColumnOids()) {
    const auto col_expr = parser::ColumnValueExpression(op->GetDatabaseOid(), op->GetTableOid(), col_oid);

    // COUNT(col), i.e. non-null entries
    child_exprs = std::vector<std::unique_ptr<parser::AbstractExpression>>();
    child_exprs.emplace_back(col_expr.Copy());
    const auto &count_expr = owned_exprs->emplace_back(std::make_unique<parser::AggregateExpression>(
        parser::ExpressionType::AGGREGATE_COUNT, std::move(child_exprs), false));
    builder.AddAggregateTerm(common::ManagedPointer(static_cast<parser::AggregateExpression *>(count_expr.get())));

    // COUNT(DISTINCT col)
    child_exprs = std::vector<std::unique_ptr<parser::AbstractExpression>>();
    child_exprs.emplace_back(col_expr.Copy());
    const auto &distinct_count_expr = owned_exprs->emplace_back(std::make_unique<parser::AggregateExpression>(
        parser::ExpressionType::AGGREGATE_COUNT, std::move(child_exprs), true));
    builder.AddAggregateTerm(
        common::ManagedPointer(static_cast<parser::AggregateExpression *>(distinct_count_expr.get())));

    // TOOD(khg): add aggregate term for TopKAggregate
  }

  return builder.Build();
}

AnalyzeTopTranslator::AnalyzeTopTranslator(const terrier::planner::AnalyzePlanNode *op, CodeGen *codegen,
                                           OperatorTranslator *bottom)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::ANALYZE),
      op_(op),
      bottom_(static_cast<AnalyzeBottomTranslator *>(bottom)),
      table_schema_(codegen->Accessor()->GetSchema(UPDATE_TABLE_OID)),
      table_pm_(codegen->Accessor()
                    ->GetTable(UPDATE_TABLE_OID)
                    ->ProjectionMapForOids(
                        std::vector<catalog::col_oid_t>(std::begin(UPDATE_COL_OIDS), std::end(UPDATE_COL_OIDS)))),
      updater_(codegen->NewIdentifier("updater")),
      update_pr_(codegen->NewIdentifier("update_pr")),
      col_oids_(codegen->NewIdentifier("col_oids")) {}

void AnalyzeTopTranslator::Produce(FunctionBuilder *builder) {
  DeclareUpdater(builder);
  child_translator_->Produce(builder);
  GenUpdaterFree(builder);
}

void AnalyzeTopTranslator::Abort(FunctionBuilder *builder) {
  GenUpdaterFree(builder);
  child_translator_->Abort(builder);
  builder->Append(codegen_->ReturnStmt(nullptr));
}

void AnalyzeTopTranslator::Consume(FunctionBuilder *builder) {
  DeclareUpdatePR(builder);
  GetUpdatePR(builder);
  FillPRFromChild(builder);
  GenTableUpdate(builder);
}

void AnalyzeTopTranslator::DeclareUpdater(FunctionBuilder *builder) {
  // Generate col oids
  SetOids(builder);
  // var updater : StorageInterface
  auto storage_interface_type = codegen_->BuiltinType(ast::BuiltinType::Kind::StorageInterface);
  builder->Append(codegen_->DeclareVariable(updater_, storage_interface_type, nullptr));
  // Call @storageInterfaceInit
  ast::Expr *updater_setup = codegen_->StorageInterfaceInit(updater_, !UPDATE_TABLE_OID, col_oids_, true);
  builder->Append(codegen_->MakeStmt(updater_setup));
}

void AnalyzeTopTranslator::GenUpdaterFree(FunctionBuilder *builder) {
  // Call @storageInterfaceFree
  ast::Expr *updater_free = codegen_->OneArgCall(ast::Builtin::StorageInterfaceFree, updater_, true);
  builder->Append(codegen_->MakeStmt(updater_free));
}

void AnalyzeTopTranslator::SetOids(FunctionBuilder *builder) {
  // Declare: var col_oids: [num_cols]uint32
  ast::Expr *arr_type = codegen_->ArrayType(UPDATE_COL_OIDS.size(), ast::BuiltinType::Kind::Uint32);
  builder->Append(codegen_->DeclareVariable(col_oids_, arr_type, nullptr));

  // For each oid, set col_oids[i] = col_oid
  for (uint16_t i = 0; i < UPDATE_COL_OIDS.size(); i++) {
    ast::Expr *lhs = codegen_->ArrayAccess(col_oids_, i);
    ast::Expr *rhs = codegen_->IntLiteral(!UPDATE_COL_OIDS[i]);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}

void AnalyzeTopTranslator::DeclareUpdatePR(FunctionBuilder *builder) {
  // var update_pr : *ProjectedRow
  auto pr_type = codegen_->BuiltinType(ast::BuiltinType::Kind::ProjectedRow);
  builder->Append(codegen_->DeclareVariable(update_pr_, codegen_->PointerType(pr_type), nullptr));
}

void AnalyzeTopTranslator::GetUpdatePR(terrier::execution::compiler::FunctionBuilder *builder) {
  // var update_pr = ProjectedRow
  auto get_pr_call = codegen_->OneArgCall(ast::Builtin::GetTablePR, updater_, true);
  builder->Append(codegen_->Assign(codegen_->MakeExpr(update_pr_), get_pr_call));
}

void AnalyzeTopTranslator::FillPRFromChild(terrier::execution::compiler::FunctionBuilder *builder) {
  const std::vector<catalog::col_oid_t> &col_oids = op_->GetColumnOids();

  // Get aggregate results
  // TODO(khg): This only gets results for the first column. Figure out how to handle the others.
  ast::Expr *count_rows = bottom_->GetOutput(0);
  ast::Expr *count_non_null = bottom_->GetOutput(1);
  ast::Expr *count_distinct = bottom_->GetOutput(2);

  for (const auto &table_col_oid : UPDATE_COL_OIDS) {
    const auto &table_col = table_schema_.GetColumn(table_col_oid);
    ast::Expr *clause_expr;
    switch (!table_col_oid) {
      case !catalog::postgres::STANULLFRAC_COL_OID:
        clause_expr =
            codegen_->BinaryOp(parsing::Token::Type::SLASH,
                               codegen_->BinaryOp(parsing::Token::Type::MINUS, count_rows, count_non_null), count_rows);
        break;
      case !catalog::postgres::STADISTINCT_COL_OID:
        clause_expr = count_distinct;
        break;
      case !catalog::postgres::STA_NUMROWS_COL_OID:
        clause_expr = count_rows;
        break;
      default:
        UNREACHABLE("Unknown table_col_oid");
    }
    auto pr_set_call = codegen_->PRSet(codegen_->MakeExpr(update_pr_), table_col.Type(), table_col.Nullable(),
                                       table_pm_[table_col_oid], clause_expr, true);
    builder->Append(codegen_->MakeStmt(pr_set_call));
  }
}

void AnalyzeTopTranslator::GenTableUpdate(FunctionBuilder *builder) {
  //   if (update fails) { Abort(); }
  auto update_slot = child_translator_->GetSlot();
  std::vector<ast::Expr *> update_args{codegen_->PointerTo(updater_), update_slot};
  auto update_call = codegen_->BuiltinCall(ast::Builtin::TableUpdate, std::move(update_args));

  auto cond = codegen_->UnaryOp(parsing::Token::Type::BANG, update_call);
  builder->StartIfStmt(cond);
  Abort(builder);
  builder->FinishBlockStmt();
}

}  // namespace terrier::execution::compiler
