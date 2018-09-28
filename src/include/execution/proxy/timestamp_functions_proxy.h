//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_functions_proxy.h
//
// Identification: src/include/execution/proxy/timestamp_functions_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/proxy/proxy.h"

namespace terrier::execution {

PROXY(TimestampFunctions) {
  // Proxy everything in function::DateFunctions
  DECLARE_METHOD(DateTrunc);
  DECLARE_METHOD(DatePart);
};

}  // namespace terrier::execution
