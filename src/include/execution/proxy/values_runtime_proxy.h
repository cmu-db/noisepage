//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// values_runtime_proxy.h
//
// Identification: src/include/execution/proxy/values_runtime_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/proxy/proxy.h"
#include "execution/values_runtime.h"

namespace terrier::execution {


PROXY(ValuesRuntime) {
  DECLARE_METHOD(OutputBoolean);
  DECLARE_METHOD(OutputTinyInt);
  DECLARE_METHOD(OutputSmallInt);
  DECLARE_METHOD(OutputInteger);
  DECLARE_METHOD(OutputBigInt);
  DECLARE_METHOD(OutputDate);
  DECLARE_METHOD(OutputTimestamp);
  DECLARE_METHOD(OutputDecimal);
  DECLARE_METHOD(OutputVarchar);
  DECLARE_METHOD(OutputVarbinary);
};


}  // namespace terrier::execution