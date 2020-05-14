#include "execution/compiler/operator/analyze_translator.h"

#include <utility>
#include "execution/ast/type.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/translator_factory.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/derived_value_expression.h"
#include "planner/plannodes/analyze_plan_node.h"

namespace terrier::execution::compiler {

AnalyzeBottomTranslator::AnalyzeBottomTranslator(const planner::AnalyzePlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::AGGREGATE_BUILD),
      op_(op),
      owned_exprs_(),
      agg_plan_node_(MakeAggregatePlanNode(op_, &owned_exprs_)),
      helper_(codegen_, agg_plan_node_.get()) {}

// TODO(khg): It seems like this kind of code belongs in plan_generator.cpp, not here
std::unique_ptr<planner::AggregatePlanNode> AnalyzeBottomTranslator::MakeAggregatePlanNode(
    const planner::AnalyzePlanNode *op, std::vector<std::unique_ptr<parser::AbstractExpression>> *owned_exprs) {
  planner::AggregatePlanNode::Builder builder;
  std::vector<std::unique_ptr<parser::AbstractExpression>> child_exprs;

  // Get column expressions from the table scan's output schema (constructed in InputColumnDeriver)
  const auto &columns = op->GetChild(0)->GetOutputSchema()->GetColumns();
  TERRIER_ASSERT(!columns.empty(), "Need at least one column to aggregate");

  // COUNT(*), i.e. total number of rows
  // We need a DVE as the child of the aggregate expression, so pick the first output column
  child_exprs.emplace_back(std::make_unique<parser::DerivedValueExpression>(columns[0].GetType(), 0, 0));
  const auto &count_const_expr = owned_exprs->emplace_back(std::make_unique<parser::AggregateExpression>(
      parser::ExpressionType::AGGREGATE_COUNT_STAR, std::move(child_exprs), false));
  builder.AddAggregateTerm(common::ManagedPointer(static_cast<parser::AggregateExpression *>(count_const_expr.get())));

  // Construct aggregates for all column values
  for (size_t i = 0; i < columns.size(); i++) {
    const auto &output_col = columns[i];
    const auto dve = parser::DerivedValueExpression(output_col.GetType(), 0, static_cast<int>(i));

    // COUNT(col), i.e. non-null entries
    child_exprs = std::vector<std::unique_ptr<parser::AbstractExpression>>();
    child_exprs.emplace_back(dve.Copy());
    const auto &count_expr = owned_exprs->emplace_back(std::make_unique<parser::AggregateExpression>(
        parser::ExpressionType::AGGREGATE_COUNT, std::move(child_exprs), false));
    builder.AddAggregateTerm(common::ManagedPointer(static_cast<parser::AggregateExpression *>(count_expr.get())));

    // COUNT(DISTINCT col)
    child_exprs = std::vector<std::unique_ptr<parser::AbstractExpression>>();
    child_exprs.emplace_back(dve.Copy());
    const auto &distinct_count_expr = owned_exprs->emplace_back(std::make_unique<parser::AggregateExpression>(
        parser::ExpressionType::AGGREGATE_COUNT, std::move(child_exprs), true));
    builder.AddAggregateTerm(
        common::ManagedPointer(static_cast<parser::AggregateExpression *>(distinct_count_expr.get())));

    // TOOD(khg): add aggregate term for TopKAggregate
  }

  return builder.Build();
}

void AnalyzeBottomTranslator::InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) {
  // Static aggregations add their aggregates directly in the state.
  for (uint32_t term_idx = 0; term_idx < agg_plan_node_->GetAggregateTerms().size(); term_idx++) {
    auto term = agg_plan_node_->GetAggregateTerms()[term_idx];
    ast::Expr *agg_type = codegen_->AggregateType(term->GetExpressionType(), term->GetChild(0)->GetReturnValueType());
    state_fields->emplace_back(codegen_->MakeField(helper_.GetAggregate(term_idx), agg_type));
  }
  // Each distinct aggregate needs its own hash table.
  helper_.DeclareAHTs(state_fields);
}

void AnalyzeBottomTranslator::InitializeStructs(util::RegionVector<ast::Decl *> *decls) {
  // The values struct contains values that will be aggregated and groubed by.
  helper_.GenValuesStruct(decls);
  // Distinct aggregate each need a hash table.
  helper_.GenAHTStructs(decls);
}

void AnalyzeBottomTranslator::InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) {
  // Each distinct aggregate needs its own key check function.
  helper_.GenKeyChecks(decls);
}

void AnalyzeBottomTranslator::InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) {
  // Static aggregations initialize their aggregates in the setup function.
  for (uint32_t term_idx = 0; term_idx < agg_plan_node_->GetAggregateTerms().size(); term_idx++) {
    ast::Expr *agg = codegen_->GetStateMemberPtr(helper_.GetAggregate(term_idx));
    ast::Expr *agg_init_call = codegen_->OneArgCall(ast::Builtin::AggInit, agg);
    setup_stmts->emplace_back(codegen_->MakeStmt(agg_init_call));
  }
  // Distinct aggregates each initialize their hash tables.
  helper_.InitAHTs(setup_stmts);
}

void AnalyzeBottomTranslator::InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) {
  // Distinct aggregates each free their hash tables.
  helper_.FreeAHTs(teardown_stmts);
}

void AnalyzeBottomTranslator::Consume(FunctionBuilder *builder) {
  // Generate Values
  helper_.FillValues(builder, this);
  // Generate hashes
  helper_.GenHashCalls(builder);
  // Construct new hash table entries
  helper_.GenConstruct(builder);
  // Advance non distinct aggregates
  helper_.GenAdvanceNonDistinct(builder);
}

AnalyzeTopTranslator::AnalyzeTopTranslator(const terrier::planner::AnalyzePlanNode *op, CodeGen *codegen,
                                           OperatorTranslator *bottom)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::ANALYZE_UPDATE),
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

  // To make things floating-point, just add a REAL value.
  // Sema::CheckArithmeticOperands will propagate the REAL-ness of the computation
  auto *zero_real = codegen_->OneArgCall(ast::Builtin::FloatToSql, codegen_->FloatLiteral(0.0));

  // Get aggregate results
  // TODO(khg): This only gets results for the first column. Figure out how to handle the others.
  ast::Expr *count_rows = bottom_->GetOutput(0);
  ast::Expr *count_non_null = bottom_->GetOutput(1);
  ast::Expr *count_distinct = bottom_->GetOutput(2);

  for (const auto &table_col_oid : UPDATE_COL_OIDS) {
    const auto &table_col = table_schema_.GetColumn(table_col_oid);
    ast::Expr *clause_expr;
    switch (!table_col_oid) {
      case !catalog::postgres::STANULLFRAC_COL_OID: {
        // Calculate the fraction of rows that is null
        auto *numerator = codegen_->BinaryOp(parsing::Token::Type::MINUS, count_rows, count_non_null);
        auto *denominator = codegen_->BinaryOp(parsing::Token::Type::PLUS, count_rows, zero_real);
        clause_expr = codegen_->BinaryOp(parsing::Token::Type::SLASH, numerator, denominator);
        break;
      }
      case !catalog::postgres::STADISTINCT_COL_OID:
        clause_expr = codegen_->BinaryOp(parsing::Token::Type::PLUS, count_distinct, zero_real);
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
