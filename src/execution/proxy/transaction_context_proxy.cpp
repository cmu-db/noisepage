//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_context_proxy.cpp
//
// Identification: src/execution/proxy/transaction_context_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/transaction_context_proxy.h"

namespace terrier::execution {


DEFINE_TYPE(TransactionContext, "concurrency::TransactionContext", opaque);


}  // namespace terrier::execution