#include "execution/compiler/expression/pr_filler.h"

#include <string>
#include <unordered_map>
#include <utility>

#include "catalog/index_schema.h"
#include "catalog/schema.h"

namespace terrier::execution::compiler {
void PRFiller::GenFiller(const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &index_pm,
                         const catalog::IndexSchema &index_schema, ast::Expr *index_pr, FunctionBuilder *builder) {
  // Fill index_pr using table_pr
  for (const auto &index_col : index_schema.GetColumns()) {
    auto translator = TranslatorFactory::CreateExpressionTranslator(index_col.StoredExpression().Get(), codegen_);
    uint16_t attr_offset = index_pm.at(index_col.Oid());
    type::TypeId attr_type = index_col.Type();
    bool nullable = index_col.Nullable();
    auto set_key_call = codegen_->PRSet(index_pr, attr_type, nullable, attr_offset, translator->DeriveExpr(this));
    builder->Append(codegen_->MakeStmt(set_key_call));
  }
}

std::pair<ast::File *, std::string> PRFiller::GenFiller(
    const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &index_pm,
    const catalog::IndexSchema &index_schema) {
  // function name
  ast::Identifier fn_name = codegen_->NewIdentifier("indexFiller");
  // First param
  ast::Expr *pr_type = codegen_->PointerType(codegen_->BuiltinType(ast::BuiltinType::ProjectedRow));
  ast::FieldDecl *param1 = codegen_->MakeField(table_pr_, pr_type);
  // Second param
  ast::Identifier index_pr = codegen_->NewIdentifier("index_pr");
  pr_type = codegen_->PointerType(codegen_->BuiltinType(ast::BuiltinType::ProjectedRow));
  ast::FieldDecl *param2 = codegen_->MakeField(index_pr, pr_type);
  // Begin function
  util::RegionVector<ast::FieldDecl *> params{{param1, param2}, codegen_->Region()};
  ast::Expr *ret_type = codegen_->BuiltinType(ast::BuiltinType::Nil);
  FunctionBuilder builder{codegen_, fn_name, std::move(params), ret_type};
  GenFiller(index_pm, index_schema, codegen_->MakeExpr(index_pr), &builder);
  // Return the file.
  util::RegionVector<ast::Decl *> top_level({builder.Finish()}, codegen_->Region());
  return {codegen_->Compile(std::move(top_level)), fn_name.Data()};
}

ast::Expr *PRFiller::GetTableColumn(const catalog::col_oid_t &col_oid) {
  auto type = table_schema_.GetColumn(col_oid).Type();
  auto nullable = table_schema_.GetColumn(col_oid).Nullable();
  uint16_t attr_idx = table_pm_.at(col_oid);
  return codegen_->PRGet(codegen_->MakeExpr(table_pr_), type, nullable, attr_idx);
}

}  // namespace terrier::execution::compiler
