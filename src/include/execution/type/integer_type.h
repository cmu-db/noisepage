//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// integer_type.h
//
// Identification: src/include/execution/type/integer_type.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/type/sql_type.h"
#include "execution/type/type_system.h"

namespace terrier::execution {

namespace type {

class Integer : public SqlType {
 public:
  bool IsVariableLength() const override { return false; }

  Value GetMinValue(CodeGen &codegen) const override;

  Value GetMaxValue(CodeGen &codegen) const override;

  Value GetNullValue(CodeGen &codegen) const override;

  void GetTypeForMaterialization(CodeGen &codegen, llvm::Type *&val_type, llvm::Type *&len_type) const override;

  llvm::Function *GetInputFunction(CodeGen &codegen, const Type &type) const override;

  llvm::Function *GetOutputFunction(CodeGen &codegen, const Type &type) const override;

  const TypeSystem &GetTypeSystem() const override { return type_system_; }

 private:
  Integer();

 private:
  // The boolean type's type-system
  TypeSystem type_system_;
};

}  // namespace type

}  // namespace terrier::execution