//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// execution_context_proxy.h
//
// Identification: src/include/execution/proxy/execution_context_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/proxy/pool_proxy.h"
#include "execution/proxy/proxy.h"
#include "execution/proxy/query_parameters_proxy.h"
#include "execution/proxy/storage_manager_proxy.h"
#include "execution/proxy/transaction_context_proxy.h"
#include "execution/execution_context.h"

namespace terrier::execution {

PROXY(ThreadStates) {
  DECLARE_MEMBER(0, terrier::type::EphemeralPool *, pool);
  DECLARE_MEMBER(1, uint32_t, num_threads);
  DECLARE_MEMBER(2, uint32_t, state_size);
  DECLARE_MEMBER(3, char *, states);
  DECLARE_TYPE;

  DECLARE_METHOD(Reset);
  DECLARE_METHOD(Allocate);
};

PROXY(ExecutionContext) {
  /// We don't need access to internal fields, so use an opaque byte array
  DECLARE_MEMBER(0, uint32_t, num_processed);
  DECLARE_MEMBER(1, concurrency::TransactionContext *, txn);
  DECLARE_MEMBER(2, QueryParameters, params);
  DECLARE_MEMBER(3, storage::StorageManager *, storage_manager);
  DECLARE_MEMBER(4, terrier::type::EphemeralPool, pool);
  DECLARE_MEMBER(5, execution::ExecutionContext::ThreadStates, thread_states);
  DECLARE_TYPE;
};

TYPE_BUILDER(ThreadStates, execution::ExecutionContext::ThreadStates);
TYPE_BUILDER(ExecutionContext, execution::ExecutionContext);

}  // namespace terrier::execution
