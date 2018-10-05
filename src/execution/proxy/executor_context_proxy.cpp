//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context_proxy.h
//
// Identification: src/execution/proxy/executor_context_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/executor_context_proxy.h"
#include "execution/proxy/transaction_context_proxy.h"

namespace terrier::execution {

// ThreadStates
DEFINE_TYPE(ThreadStates, "executor::ThreadStates", pool, num_threads, state_size, states);

DEFINE_METHOD(terrier::executor::ExecutionContext, ThreadStates, Reset);
DEFINE_METHOD(terrier::executor::ExecutionContext, ThreadStates, Allocate);

// ExecutionContext
DEFINE_TYPE(ExecutionContext, "executor::ExecutionContext", num_processed, txn, params, storage_manager, pool,
            thread_states);

}  // namespace terrier::execution