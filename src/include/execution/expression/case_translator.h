//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// case_translator.h
//
// Identification: src/include/execution/expression/case_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/expression/expression_translator.h"

namespace peloton {

namespace expression {
class CaseExpression;
}  // namespace expression

namespace codegen {

/// A translator for CASE expressions.
class CaseTranslator : public ExpressionTranslator {
 public:
  CaseTranslator(const expression::CaseExpression &expression,
                 CompilationContext &context);

  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override;
};

}  // namespace codegen
}  // namespace peloton
