//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_translator.h
//
// Identification: src/include/execution/expression/constant_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/expression/expression_translator.h"

namespace terrier::execution {

namespace expression {
class ConstantValueExpression;
}  // namespace planner



//===----------------------------------------------------------------------===//
// A const expression translator just produces the LLVM value version of the
// constant value within.
//===----------------------------------------------------------------------===//
class ConstantTranslator : public ExpressionTranslator {
 public:
  ConstantTranslator(const expression::ConstantValueExpression &exp,
                     CompilationContext &ctx);

  // Produce the value that is the result of codegen-ing the expression
  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override;
};


}  // namespace terrier::execution
