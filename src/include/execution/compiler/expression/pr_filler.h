#pragma once
#include <cstdint>
#include <iosfwd>
#include <string>
#include <unordered_map>
#include <utility>

#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "execution/ast/identifier.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/translator_factory.h"
#include "execution/util/execution_common.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"

namespace terrier {
namespace catalog {
class IndexSchema;
}  // namespace catalog
namespace execution {
namespace ast {
class Expr;
class File;
}  // namespace ast
namespace compiler {
class FunctionBuilder;
}  // namespace compiler
}  // namespace execution
}  // namespace terrier

namespace terrier::execution::compiler {
/**
 * PRFiller is used to generate code that fills index PRs from table PRs
 */
class PRFiller : public ExpressionEvaluator {
 public:
  /**
   * Constructor
   * @param codegen code generator
   * @param table_schema schema of the table
   * @param table_pm projection map of the table
   * @param table_pr identifier of the table's projected row
   */
  PRFiller(CodeGen *codegen, const catalog::Schema &table_schema, const storage::ProjectionMap &table_pm,
           ast::Identifier table_pr)
      : codegen_(codegen), table_schema_(table_schema), table_pm_(table_pm), table_pr_(table_pr) {}

  /**
   * Same as above, but generate the table projected row.
   * This constructor should be used when a standalone function is needed.
   * @param codegen code generate
   * @param table_schema schema of the table
   * @param table_pm projection map of the table
   */
  PRFiller(CodeGen *codegen, const catalog::Schema &table_schema, const storage::ProjectionMap &table_pm)
      : PRFiller(codegen, table_schema, table_pm, codegen->NewIdentifier("table_pr")) {}

  /**
   * Generate statements to fill the index PR.
   * @param index_pm projection map of the index
   * @param index_schema schema of the index
   * @param index_pr projected row of the index
   * @param builder the function builder to append to
   */
  void GenFiller(const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &index_pm,
                 const catalog::IndexSchema &index_schema, ast::Expr *index_pr, FunctionBuilder *builder);

  /**
   * Generate a standalone function containing the projected row filler.
   * @param index_pm projection map of the index
   * @param index_schema schema of the index
   * @return The generate file and the function name within.
   */
  std::pair<ast::File *, std::string> GenFiller(
      const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &index_pm,
      const catalog::IndexSchema &index_schema);

  /**
   * Generate an expression to read from the table PR
   * @param col_oid oid of the column
   * @return the expression that accesses the table PR.
   */
  ast::Expr *GetTableColumn(const catalog::col_oid_t &col_oid) override {
    auto type = table_schema_.GetColumn(col_oid).Type();
    auto nullable = table_schema_.GetColumn(col_oid).Nullable();
    uint16_t attr_idx = table_pm_.at(col_oid);
    return codegen_->PRGet(codegen_->MakeExpr(table_pr_), type, nullable, attr_idx);
  }

  /**
   * Unreachable: this cannot handle DerivedValueExpression.
   */
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    UNREACHABLE("Index schemas should not have derived value expressions!");
  }

 private:
  CodeGen *codegen_;
  const catalog::Schema &table_schema_;
  const storage::ProjectionMap &table_pm_;
  ast::Identifier table_pr_;
};
}  // namespace terrier::execution::compiler
