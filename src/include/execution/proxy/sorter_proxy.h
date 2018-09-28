//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter_proxy.h
//
// Identification: src/include/execution/proxy/sorter_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/proxy/proxy.h"
#include "execution/runtime/sorter.h"

namespace terrier::execution {

PROXY(Sorter) {
  // clang-format off
  DECLARE_MEMBER(0,
                 char[sizeof(void *) +              // abstract pool reference
                      sizeof(void *) +              // comparison function
                      sizeof(uint32_t) +            // tuple size
                      sizeof(char *) +              // buffer start
                      sizeof(char *) +              // buffer end
                      sizeof(uint64_t) +            // next allocation size
                      sizeof(std::vector<char *>)], // tuple reference vector
                 opaque1);
  DECLARE_MEMBER(1, char **, tuples_start);
  DECLARE_MEMBER(2, char **, tuples_end);
  DECLARE_MEMBER(3, char[sizeof(std::vector<std::pair<void *, uint64_t>>)],
                 opaque2);
  DECLARE_TYPE;
  // clang-format on

  // Proxy methods in runtime::Sorter
  DECLARE_METHOD(Init);
  DECLARE_METHOD(StoreInputTuple);
  DECLARE_METHOD(Sort);
  DECLARE_METHOD(SortParallel);
  DECLARE_METHOD(Destroy);
};

TYPE_BUILDER(Sorter, util::Sorter);

}  // namespace terrier::execution