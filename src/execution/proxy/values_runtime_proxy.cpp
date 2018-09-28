//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// values_runtime_proxy.cpp
//
// Identification: src/execution/proxy/values_runtime_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/values_runtime_proxy.h"

#include "execution/proxy/pool_proxy.h"
#include "execution/proxy/runtime_functions_proxy.h"
#include "execution/proxy/value_proxy.h"

namespace terrier::execution {

DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputBoolean);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputTinyInt);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputSmallInt);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputInteger);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputBigInt);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputDate);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputTimestamp);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputDecimal);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputVarchar);
DEFINE_METHOD(peloton::codegen, ValuesRuntime, OutputVarbinary);

}  // namespace terrier::execution
