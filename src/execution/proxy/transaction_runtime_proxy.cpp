//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_runtime_proxy.cpp
//
// Identification: src/execution/proxy/transaction_runtime_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/transaction_runtime_proxy.h"

#include "execution/proxy/data_table_proxy.h"
#include "execution/proxy/tile_group_proxy.h"
#include "execution/proxy/transaction_context_proxy.h"
#include "execution/proxy/value_proxy.h"
#include "execution/transaction_runtime.h"

namespace terrier::execution {

DEFINE_METHOD(peloton::codegen, TransactionRuntime, PerformVectorizedRead);
DEFINE_METHOD(peloton::codegen, TransactionRuntime, PerformVisibilityCheck);

}  // namespace terrier::execution
