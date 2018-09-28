//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// buffer_proxy.cpp
//
// Identification: src/execution/proxy/buffer_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/buffer_proxy.h"

#include "execution/proxy/proxy.h"
#include "execution/proxy/type_builder.h"

namespace terrier::execution {


DEFINE_TYPE(Buffer, "peloton::Buffer", buffer_start, buffer_pos, buffer_end);

DEFINE_METHOD(peloton::codegen::util, Buffer, Init);
DEFINE_METHOD(peloton::codegen::util, Buffer, Destroy);
DEFINE_METHOD(peloton::codegen::util, Buffer, Append);
DEFINE_METHOD(peloton::codegen::util, Buffer, Reset);


}  // namespace terrier::execution