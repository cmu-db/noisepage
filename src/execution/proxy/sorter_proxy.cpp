//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter_proxy.cpp
//
// Identification: src/execution/proxy/sorter_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/sorter_proxy.h"

#include "execution/proxy/executor_context_proxy.h"

namespace terrier::execution {

DEFINE_TYPE(Sorter, "peloton::runtime::Sorter", opaque1, tuples_start, tuples_end, opaque2);

DEFINE_METHOD(peloton::util, Sorter, Init);
DEFINE_METHOD(peloton::util, Sorter, StoreInputTuple);
DEFINE_METHOD(peloton::util, Sorter, Sort);
DEFINE_METHOD(peloton::util, Sorter, SortParallel);
DEFINE_METHOD(peloton::util, Sorter, Destroy);

}  // namespace terrier::execution