//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_context_proxy.h
//
// Identification: src/include/execution/proxy/transaction_context_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrency/transaction_context.h"
#include "execution/proxy/proxy.h"

namespace terrier::execution {

PROXY(TransactionContext) {
  DECLARE_MEMBER(0, char[sizeof(concurrency::TransactionContext)], opaque);
  DECLARE_TYPE;
};

TYPE_BUILDER(TransactionContext, concurrency::TransactionContext);

}  // namespace terrier::execution