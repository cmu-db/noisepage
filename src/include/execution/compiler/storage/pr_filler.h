#pragma once
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
/**
 * PRFiller is used to generate functions that fill index PRs from table PRs
 */
class PRFiller : public ExpressionEvaluator {
 public:
  /**
   * Constructor
   * @param codegen code generator
   * @param table_schema schema of the table
   * @param table_pm projection map of the table
   */
  PRFiller(CodeGen *codegen, const catalog::Schema &table_schema, const storage::ProjectionMap &table_pm,
      ast::Identifier table_pr)
  : codegen_(codegen)
  , table_schema_(table_schema)
  , table_pm_(table_pm)
  , table_pr_(table_pr){}

  /**
   * Generate ast to fill the index PR.
   * @param index_pm projection map of the index
   * @param index_schema schema of the index
   * @param builder the function builder to append to
   */
  void GenFiller(const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &index_pm,
      const catalog::IndexSchema& index_schema, ast::Identifier index_pr, FunctionBuilder *builder) {
    // Declare the function
    // Function name

    // Now fill index_pr using table_pr
    for (const auto &index_col : index_schema.GetColumns()) {
      auto translator = TranslatorFactory::CreateExpressionTranslator(index_col.StoredExpression().Get(), codegen_);
      uint16_t attr_offset = index_pm.at(index_col.Oid());
      type::TypeId attr_type = index_col.Type();
      bool nullable = index_col.Nullable();
      auto set_key_call = codegen_->PRSet(codegen_->MakeExpr(index_pr), attr_type, nullable, attr_offset,
          translator->DeriveExpr(this));
      builder->Append(codegen_->MakeStmt(set_key_call));
    }
  }

  /**
   * Generate an expression to read from the table PR
   * @param col_oid oid of the column
   * @return the expression that accesses the table PR.
   */
  ast::Expr* GetTableColumn(const catalog::col_oid_t &col_oid) override {
    auto type = table_schema_.GetColumn(col_oid).Type();
    auto nullable = table_schema_.GetColumn(col_oid).Nullable();
    uint16_t attr_idx = table_pm_.at(col_oid);
    return codegen_->PRGet(codegen_->MakeExpr(table_pr_), type, nullable, attr_idx);
  }

  /**
   * Unreachable: this cannot handle DerivedValueExpression.
   */
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