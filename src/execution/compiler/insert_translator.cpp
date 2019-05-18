#include "execution/compiler/operator/insert_translator.h"

#include "execution/compiler/consumer_context.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/row_batch.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/code_context.h"
#include "execution/sql/execution_structures.h"

#include "planner/plannodes/insert_plan_node.h"

namespace tpl::compiler {

InsertTranslator::InsertTranslator(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline)
    : OperatorTranslator(op, pipeline), struct_ty_(nullptr) {
  if(op.GetChildrenSize() == 1) {
    pipeline->GetCompilationContext()->Prepare(*op.GetChild(0), pipeline);
  }
}

void InsertTranslator::InitializeQueryState() {
  const auto &node = GetOperatorAs<terrier::planner::InsertPlanNode>();
  auto &codegen = *pipeline_->GetCodeGen();

  // define the tuple_struct
  util::RegionVector<ast::FieldDecl *> fields(pipeline_->GetRegion());
  for (const auto &val : node.GetValues()) {
    auto field_id = codegen.NewIdentifier();
    auto field_ty = codegen.TyConvert(val.Type());
    fields.emplace_back(codegen->NewFieldDecl(DUMMY_POS, field_id, field_ty));
  }
  struct_ty_ = codegen->NewStructType(DUMMY_POS, std::move(fields));
  auto struct_decl = codegen->NewStructDecl(DUMMY_POS, codegen.NewIdentifier(), struct_ty_);
  codegen.GetCodeContext()->AddTopDecl(struct_decl);
}

void InsertTranslator::Produce() {
  // generate the code for the insertion
  const auto &node = GetOperatorAs<terrier::planner::InsertPlanNode>();
  auto &codegen = *pipeline_->GetCodeGen();

  // var v : tuple_struct
  auto var_name = codegen.NewIdentifier();
  auto var_ex = codegen->NewIdentifierExpr(DUMMY_POS, var_name);
  auto var_decl = codegen->NewVariableDecl(DUMMY_POS, var_name, struct_ty_, nullptr);
  codegen.GetCurrentFunction()->Append(codegen->NewDeclStmt(var_decl));

  // populate v
  const auto &node_vals = node.GetValues();
  const auto &fields = struct_ty_->fields();
  for (u16 i = 0; i < node_vals.size(); i++) {
    const auto &member_name = fields[i]->name();
    auto member_ex = codegen->NewIdentifierExpr(DUMMY_POS, member_name);
    auto *dst = codegen->NewMemberExpr(DUMMY_POS, var_ex, member_ex);
    auto *src = codegen.PeekValue(node_vals[i]);
    auto asgn_stmt = codegen->NewAssignmentStmt(DUMMY_POS, dst, src);
    codegen.GetCurrentFunction()->Append(asgn_stmt);
  }

  // @insert(db_oid, table_oid, &v)
  util::RegionVector<ast::Expr *> args(pipeline_->GetRegion());
  args.emplace_back(codegen->NewIntLiteral(DUMMY_POS, !node.GetDatabaseOid()));
  args.emplace_back(codegen->NewIntLiteral(DUMMY_POS, !node.GetTableOid()));
  args.emplace_back(codegen->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::AMPERSAND, var_ex));
  auto call_stmt = codegen->NewCallExpr(codegen.Binsert(), std::move(args));
  codegen.GetCurrentFunction()->Append(codegen->NewExpressionStmt(call_stmt));
}

void InsertTranslator::Consume(const tpl::compiler::ConsumerContext *context, tpl::compiler::RowBatch *batch) const {
  // generate the code for insert select
  const auto &node = GetOperatorAs<terrier::planner::InsertPlanNode>();
  auto &codegen = *pipeline_->GetCodeGen();

  auto var_ex = batch->GetIdentifierExpr();

  // @insert(db_oid, table_oid, &v)
  util::RegionVector<ast::Expr *> args(pipeline_->GetRegion());
  args.emplace_back(codegen->NewIntLiteral(DUMMY_POS, !node.GetDatabaseOid()));
  args.emplace_back(codegen->NewIntLiteral(DUMMY_POS, !node.GetTableOid()));
  args.emplace_back(codegen->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::AMPERSAND, var_ex));
  auto call_stmt = codegen->NewCallExpr(codegen.Binsert(), std::move(args));
  codegen.GetCurrentFunction()->Append(codegen->NewExpressionStmt(call_stmt));
}


} // namespace tpl::compiler