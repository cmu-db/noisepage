#include "execution/compiler/row_batch.h"
#include "execution/compiler/compilation_context.h"

namespace tpl::compiler {
  ast::Expr *RowBatch::DeriveValue(const terrier::parser::AbstractExpression &ex) {
    auto translator = context_.GetTranslator(ex);
    auto ret = translator->DeriveExpr(&ex, this);

    //cache ret??
    return ret;
  }
}