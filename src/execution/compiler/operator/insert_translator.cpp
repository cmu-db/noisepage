
#include <execution/compiler/operator/insert_translator.h>
#include <execution/compiler/translator_factory.h>
#include <execution/compiler/storage/pr_filler.h>

#include "execution/compiler/operator/insert_translator.h"
#include "execution/compiler/function_builder.h"

namespace terrier::execution::compiler {
InsertTranslator::InsertTranslator(const terrier::planner::InsertPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen),
      op_(op),
      inserter_struct_(codegen->NewIdentifier(inserter_name_)),
      table_pr_(codegen->NewIdentifier(table_pr_name_)),
      table_schema_(codegen->Accessor()->GetSchema(op_->GetTableOid())),
      table_pm_(codegen->Accessor()->GetTable(op_->GetTableOid())->ProjectionMapForOids(op_->GetParameterInfo())) {}


void InsertTranslator::Produce(FunctionBuilder *builder) {
  // generate the code for the insertion
  // var pr : *ProjectedRow

  builder->Append(codegen_->DeclareVariable(table_pr_,
      codegen_->PointerType(codegen_->BuiltinType(ast::BuiltinType::ProjectedRow)),
                                            nullptr));

  //pr = @inserterGetTablePR(&inserter)
  builder->Append(codegen_->Assign(codegen_->MakeExpr(table_pr_), codegen_->InserterGetTablePR(inserter_struct_)));
  // populate v[

  // code size proportional to number of tuples???
  for(size_t tuple = 0;tuple < op_->GetBulkInsertCount();tuple++) {
    const auto &node_vals = op_->GetValues(tuple);
    auto param_info = op_->GetParameterInfo();
    for (size_t i = 0; i < node_vals.size(); i++) {
      auto &val = node_vals[i];
      auto *src = codegen_->PeekValue(node_vals[i]);
      auto *set_stmt = codegen_->MakeStmt(codegen_->PRSet(codegen_->MakeExpr(table_pr_),
                                                          val.Type(),
                                                          table_schema_.GetColumn(param_info[i]).Nullable(),
                                                          table_pm_[param_info[i]],
                                                          src));
      builder->Append(set_stmt);
    }


    // @inserterTableInsert(&inserter)
    auto slot_ident = codegen_->NewIdentifier("slot");
    auto tuple_slot_decl = codegen_->DeclareVariable(slot_ident, codegen_->BuiltinType(ast::BuiltinType::TupleSlot),
                                                     nullptr);
    builder->Append(tuple_slot_decl);
    builder->Append(codegen_->Assign(codegen_->MakeExpr(slot_ident),
        codegen_->InserterTableInsert(inserter_struct_)));

    const auto &indexes = op_->GetIndexOids();
    PRFiller pr_filler(codegen_, table_schema_, table_pm_, table_pr_);

    auto index_pr_name = codegen_->NewIdentifier("index_pr");
    builder->Append(codegen_->DeclareVariable(index_pr_name,
        codegen_->PointerType(codegen_->BuiltinType(ast::BuiltinType::ProjectedRow)), nullptr));

    for (auto &index_oid : indexes) {
      // pr = @inserterGetIndexPR(&inserter, oid)
      builder->Append(codegen_->Assign(codegen_->MakeExpr(index_pr_name),
                                       codegen_->InserterGetIndexPR(inserter_struct_, !index_oid)));
      auto index = codegen_->Accessor()->GetIndex(index_oid);
      auto index_pm = index->GetKeyOidToOffsetMap();
      auto index_schema = codegen_->Accessor()->GetIndexSchema(index_oid);
      pr_filler.GenFiller(index_pm, index_schema, index_pr_name, builder);

      //@inserterInsertIndex(&inserter, index_oid)
      builder->Append(codegen_->MakeStmt(
          codegen_->InserterIndexInsert(inserter_struct_, !index_oid)));
    }
  }
}

void InsertTranslator::Consume(FunctionBuilder *builder) {
  // generate the code for insert into select
//  auto var_ex = batch->GetIdentifierExpr();
//
//  // @insert(db_oid, table_oid, &v)
//  util::RegionVector<ast::Expr *> args(codegen_->GetRegion());
//  args.emplace_back(codegen_->NewIntLiteral(DUMMY_POS, !node.GetDatabaseOid()));
//  args.emplace_back(codegen_->NewIntLiteral(DUMMY_POS, !node.GetTableOid()));
//  args.emplace_back(codegen_->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::AMPERSAND, var_ex));
//  auto call_stmt = codegen_->NewCallExpr(codegen_->Binsert(), std::move(args));
//  codegen_->GetCurrentFunction()->Append(codegen_->NewExpressionStmt(call_stmt));

//  codegen_->DeclareVariable(ast::Identifier("projected_row"), )
//  codegen_->Assign()
//  builder->Append(codegen_->MakeStmt(codegen_->InserterGetTablePR()))


}

void InsertTranslator::InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) {
  ast::Expr *inserter_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Inserter);
  state_fields->emplace_back(codegen_->MakeField(inserter_struct_, inserter_type));
}

void InsertTranslator::InitializeStructs(util::RegionVector<ast::Decl *> *decls) {

}
void InsertTranslator::InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) {
  ast::Expr *inserter_setup = codegen_->InserterInit(inserter_struct_, !op_->GetTableOid());
  setup_stmts->emplace_back(codegen_->MakeStmt(inserter_setup));
}

}  // namespace terrier::execution::compiler

