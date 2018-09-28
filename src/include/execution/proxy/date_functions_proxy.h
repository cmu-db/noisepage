//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_functions_proxy.h
//
// Identification: src/include/execution/proxy/date_functions_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/proxy/proxy.h"

namespace terrier::execution {


PROXY(DateFunctions) {
  // Utility functions
  DECLARE_METHOD(Now);

  // Input functions
  DECLARE_METHOD(InputDate);
};


}  // namespace terrier::execution
