//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// varlen_pool.h
//
// Identification: src/include/storage/varlen_pool.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <cstdlib>
#include <unordered_set>

#include "common/spin_latch.h"

namespace terrier {

/**
 * @brief A varlen pool that can quickly allocate chunks of memory.
 * */
class VarlenPool {
 public:
  VarlenPool() = default;

  ~VarlenPool();

  void *Allocate(size_t size);

  void Free(void *ptr);

 public:
  /** Location list */
  std::unordered_set<char *> locations_;

  /** Spin lock protecting location list */
  SpinLatch pool_lock_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

VarlenPool::~VarlenPool() {
  pool_lock_.Lock();
  for (auto location : locations_) {
    delete[] location;
  }
  pool_lock_.Unlock();
}

void *VarlenPool::Allocate(size_t size) {
  auto location = new char[size];

  pool_lock_.Lock();
  locations_.insert(location);
  pool_lock_.Unlock();

  return location;
}

void VarlenPool::Free(void *ptr) {
  auto *cptr = (char *)ptr;
  pool_lock_.Lock();
  locations_.erase(cptr);
  pool_lock_.Unlock();
  delete[] cptr;
}

}  // namespace terrier
