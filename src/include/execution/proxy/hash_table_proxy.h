//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_table_proxy.h
//
// Identification: src/include/execution/proxy/hash_table_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/proxy/proxy.h"
#include "execution/runtime/hash_table.h"

namespace terrier::execution {


/// The proxy for HashTable::Entry
PROXY(Entry) {
  DECLARE_MEMBER(0, uint64_t, hash);
  DECLARE_MEMBER(1, util::HashTable::Entry *, next);
  DECLARE_TYPE;
};

/// The proxy for CCHashTable
PROXY(HashTable) {
  DECLARE_MEMBER(0, char *, memory);
  DECLARE_MEMBER(1, util::HashTable::Entry **, directory);
  DECLARE_MEMBER(2, uint64_t, size);
  DECLARE_MEMBER(3, uint64_t, mask);
  DECLARE_MEMBER(4, char[sizeof(util::HashTable::EntryBuffer)], entry_buffer);
  DECLARE_MEMBER(5, uint64_t, num_elems);
  DECLARE_MEMBER(6, uint64_t, capacity);
  DECLARE_MEMBER(7, char[sizeof(std::unique_ptr<char>)], stats);
  DECLARE_TYPE;

  // Proxy all methods that will be called from codegen
  DECLARE_METHOD(Init);
  DECLARE_METHOD(Insert);
  DECLARE_METHOD(InsertLazy);
  DECLARE_METHOD(BuildLazy);
  DECLARE_METHOD(ReserveLazy);
  DECLARE_METHOD(MergeLazyUnfinished);
  DECLARE_METHOD(Destroy);
};

/// The type builders for Entry and HashTable
TYPE_BUILDER(Entry, util::HashTable::Entry);
TYPE_BUILDER(HashTable, util::HashTable);


}  // namespace terrier::execution