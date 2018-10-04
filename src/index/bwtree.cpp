
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree.cpp
//
// Identification: src/index/bwtree.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "bwtree/bwtree.h"

namespace wangziqi2013 {
namespace bwtree {

bool print_flag = true;

// This will be initialized when thread is initialized and in a per-thread
// basis, i.e. each thread will get the same initialization image and then
// is free to change them
thread_local int BwTreeBase::gc_id = -1;

std::atomic<size_t> BwTreeBase::total_thread_num{0UL};

}  // namespace bwtree
}  // namespace wangziqi2013
