//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// conjunction_translator.h
//
// Identification: src/include/execution/expression/conjunction_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/compilation_context.h"
#include "execution/expression/expression_translator.h"

namespace terrier::execution {

namespace expression {
class ConjunctionExpression;
}  // namespace expression

//===----------------------------------------------------------------------===//
// A translator for conjunction expressions
//===----------------------------------------------------------------------===//
class ConjunctionTranslator : public ExpressionTranslator {
 public:
  // Constructor
  ConjunctionTranslator(const expression::ConjunctionExpression &conjunction, CompilationContext &context);

  // Produce the value that is the result of codegening the expression
  Value DeriveValue(CodeGen &codegen, RowBatch::Row &row) const override;
};

}  // namespace terrier::execution