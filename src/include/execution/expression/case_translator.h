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

namespace terrier::execution {

namespace expression {
class CaseExpression;
}  // namespace expression

/// A translator for CASE expressions.
class CaseTranslator : public ExpressionTranslator {
 public:
  CaseTranslator(const expression::CaseExpression &expression, CompilationContext &context);

  Value DeriveValue(CodeGen &codegen, RowBatch::Row &row) const override;
};

}  // namespace terrier::execution
