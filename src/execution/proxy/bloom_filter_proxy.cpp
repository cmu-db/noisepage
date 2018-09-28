//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter_proxy.cpp
//
// Identification: src/execution/proxy/bloom_filter_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/bloom_filter_proxy.h"

namespace terrier::execution {

DEFINE_TYPE(BloomFilter, "peloton::BloomFilter", num_hash_funcs, bytes, num_bits, num_misses, num_probes);

DEFINE_METHOD(peloton::util, BloomFilter, Init);
DEFINE_METHOD(peloton::util, BloomFilter, Destroy);

}  // namespace terrier::execution