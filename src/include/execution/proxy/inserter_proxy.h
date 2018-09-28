//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// inserter_proxy.h
//
// Identification: src/include/execution/proxy/inserter_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/inserter.h"
#include "execution/proxy/proxy.h"

namespace peloton {
namespace codegen {

PROXY(Inserter) {

  DECLARE_MEMBER(0, char[sizeof(Inserter)], opaque);
  DECLARE_TYPE;

  DECLARE_METHOD(Init);
  DECLARE_METHOD(AllocateTupleStorage);
  DECLARE_METHOD(GetPool);
  DECLARE_METHOD(Insert);
  DECLARE_METHOD(TearDown);
};

TYPE_BUILDER(Inserter, codegen::Inserter);

}  // namespace codegen
}  // namespace peloton
