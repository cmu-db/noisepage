//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen_proxy.cpp
//
// Identification: src/execution/proxy/varlen_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/varlen_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(Varlen, "peloton::Varlen", length, ptr);

}  // namespace codegen
}  // namespace peloton