//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// runtime_functions_proxy.h
//
// Identification: src/include/execution/proxy/runtime_functions_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>
#include "execution/proxy/proxy.h"
#include "execution/runtime_functions.h"
#include "expression/abstract_expression.h"
#include "common/worker_pool.h"

namespace terrier::execution {

PROXY(ColumnLayoutInfo) {
  DECLARE_MEMBER(0, char *, col_start_ptr);
  DECLARE_MEMBER(1, uint32_t, stride);
  DECLARE_MEMBER(2, bool, columnar);
  DECLARE_TYPE;
};

PROXY(AbstractExpression) {
  DECLARE_MEMBER(0, char[sizeof(expression::AbstractExpression)], opaque);
  DECLARE_TYPE;
};

PROXY(Type) {
  DECLARE_MEMBER(0, char[sizeof(type::Type)], opaque);
  DECLARE_TYPE;
};

PROXY(RuntimeFunctions) {
  DECLARE_MEMBER(0, char[sizeof(common::WorkerPool)], opaqueu);
  DECLARE_TYPE;

  DECLARE_METHOD(HashMurmur3);
  DECLARE_METHOD(HashCrc64);
  DECLARE_METHOD(GetTileGroup);
  DECLARE_METHOD(GetTileGroupLayout);
  DECLARE_METHOD(FillPredicateArray);
  DECLARE_METHOD(ExecuteTableScan);
  DECLARE_METHOD(ExecutePerState);
  DECLARE_METHOD(ThrowDivideByZeroException);
  DECLARE_METHOD(ThrowOverflowException);
};

TYPE_BUILDER(ColumnLayoutInfo, RuntimeFunctions::ColumnLayoutInfo);
TYPE_BUILDER(AbstractExpression, expression::AbstractExpression);
TYPE_BUILDER(Type, type::Type);

}  // namespace terrier::execution