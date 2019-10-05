#pragma once
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
class PRFiller : public ExpressionEvaluator {
 public:
  PRFiller(CodeGen *codegen, const catalog::Schema &table_schema, const storage::ProjectionMap &table_pm)
  : codegen_(codegen)
  , table_schema_(table_schema)
  , table_pm_(table_pm)
  , table_pr_(codegen_->NewIdentifier("table_pr")){}

  std::pair<ast::File*, std::string> GenFiller(const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &index_pm, const catalog::IndexSchema& index_schema) {
    // Declare the function
    // Function name
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

    // Now fill index_pr using table_pr
    for (const auto &index_col : index_schema.GetColumns()) {
      auto translator = TranslatorFactory::CreateExpressionTranslator(index_col.StoredExpression().Get(), codegen_);
      uint16_t attr_offset = index_pm.at(index_col.Oid());
      type::TypeId attr_type = index_col.Type();
      bool nullable = index_col.Nullable();
      auto set_key_call = codegen_->PRSet(codegen_->MakeExpr(index_pr), attr_type, nullable, attr_offset, translator->DeriveExpr(this));
      builder.Append(codegen_->MakeStmt(set_key_call));
    }

    util::RegionVector<ast::Decl *> top_level({builder.Finish()}, codegen_->Region());
    return {codegen_->Compile(std::move(top_level)), fn_name.Data()};
  }

  ast::Expr* GetTableColumn(const catalog::col_oid_t &col_oid) override {
    auto type = table_schema_.GetColumn(col_oid).Type();
    auto nullable = table_schema_.GetColumn(col_oid).Nullable();
    uint16_t attr_idx = table_pm_.at(col_oid);
    return codegen_->PRGet(codegen_->MakeExpr(table_pr_), type, nullable, attr_idx);
  }

  ast::Expr* GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    UNREACHABLE("Index schemas should not have derived value expressions!");
  }

 private:
  CodeGen *codegen_;
  const catalog::Schema &table_schema_;
  const storage::ProjectionMap &table_pm_;
  ast::Identifier table_pr_;
};
}