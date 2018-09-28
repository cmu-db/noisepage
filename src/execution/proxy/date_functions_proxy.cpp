//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_functions_proxy.cpp
//
// Identification: src/execution/proxy/date_functions_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/date_functions_proxy.h"

#include "execution/proxy/runtime_functions_proxy.h"
#include "execution/proxy/type_builder.h"
#include "function/date_functions.h"

namespace terrier::execution {

// Utility functions
DEFINE_METHOD(peloton::function, DateFunctions, Now);

// Input functions
DEFINE_METHOD(peloton::function, DateFunctions, InputDate);

}  // namespace terrier::execution
