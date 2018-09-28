//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_functions_proxy.cpp
//
// Identification: src/execution/proxy/timestamp_functions_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/timestamp_functions_proxy.h"

#include "execution/codegen.h"
#include "function/timestamp_functions.h"

namespace terrier::execution {

DEFINE_METHOD(peloton::function, TimestampFunctions, DateTrunc);
DEFINE_METHOD(peloton::function, TimestampFunctions, DatePart);

}  // namespace terrier::execution
