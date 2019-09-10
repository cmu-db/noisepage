
#include <execution/compiler/operator/insert_translator.h>

#include "execution/compiler/operator/insert_translator.h"

namespace terrier::execution::compiler {
InsertTranslator::InsertTranslator(const terrier::planner::AbstractPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(op, codegen), inserter_struct_(codegen->NewIdentifier(inserter_name_)) {}


void InsertTranslator::Produce(FunctionBuilder *builder) {
  // generate the code for the insertion
  const auto &node = GetOperatorAs<terrier::planner::InsertPlanNode>();

  // var pr : projectedRow
  auto projected_row = codegen_->InserterGetTablePR(codegen_->PointerTo(inserter_struct_));

  // populate v
  const auto &node_vals = node.GetValues();
  auto param_info = node.GetParameterInfo();
  for (size_t i = 0; i < node_vals.size(); i++) {
    auto col_id = !param_info[i];
    auto *src = codegen_->PeekValue(node_vals[i]);
    auto set_stmt = codegen_->ProjectedRowSet(projected_row, codegen_->IntLiteral(col_id), src)
    builder->Append(set_stmt);
  }

//  // @insert(db_oid, table_oid, &v)
//  util::RegionVector<ast::Expr *> args(pipeline_->GetRegion());
//  args.emplace_back(codegen_->NewIntLiteral(DUMMY_POS, !node.GetDatabaseOid()));
//  args.emplace_back(codegen_->NewIntLiteral(DUMMY_POS, !node.GetTableOid()));
//  args.emplace_back(codegen_->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::AMPERSAND, var_ex));
//  auto call_stmt = codegen_->NewCallExpr(codegen.Binsert(), std::move(args));
//  codegen_->GetCurrentFunction()->Append(codegen->NewExpressionStmt(call_stmt));
}

void InsertTranslator::Consume(FunctionBuilder *builder) {
  // generate the code for insert select
  const auto &node = GetOperatorAs<terrier::planner::InsertPlanNode>();

  auto var_ex = batch->GetIdentifierExpr();

  // @insert(db_oid, table_oid, &v)
  util::RegionVector<ast::Expr *> args(codegen_->GetRegion());
  args.emplace_back(codegen_->NewIntLiteral(DUMMY_POS, !node.GetDatabaseOid()));
  args.emplace_back(codegen_->NewIntLiteral(DUMMY_POS, !node.GetTableOid()));
  args.emplace_back(codegen_->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::AMPERSAND, var_ex));
  auto call_stmt = codegen_->NewCallExpr(codegen_->Binsert(), std::move(args));
  codegen_->GetCurrentFunction()->Append(codegen_->NewExpressionStmt(call_stmt));
}

void InsertTranslator::InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) {
  ast::Expr *inserter_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Inserter);
  state_fields->emplace_back(codegen_->MakeField(inserter_struct_, inserter_type));
}

void InsertTranslator::InitializeStructs(util::RegionVector<ast::Decl *> *decls) {

}
void InsertTranslator::InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) {
  auto &node = GetOperatorAs<terrier::planner::InsertPlanNode>();
  ast::Expr *inserter_setup = codegen_->InserterInit(codegen_->PointerTo(inserter_struct_),
      codegen_->GetExecCtxVar(), codegen_->IntLiteral(!node.GetTableOid()));
  setup_stmts->emplace_back(codegen_->MakeStmt(inserter_setup));
}

void InsertTranslator::Produce(FunctionBuilder *builder) {

}
void InsertTranslator::Consume(FunctionBuilder *builder) {

}
}  // namespace terrier::execution::compiler
