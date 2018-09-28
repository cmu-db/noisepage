//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updater_proxy.h
//
// Identification: src/include/execution/proxy/updater_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/proxy/proxy.h"
#include "execution/updater.h"

namespace terrier::execution {

PROXY(Updater) {
  /// We don't need access to internal fields, so use an opaque byte array
  DECLARE_MEMBER(0, char[sizeof(Updater)], opaque);
  DECLARE_TYPE;

  /// Proxy Init() and Update() in Updater
  DECLARE_METHOD(Init);
  DECLARE_METHOD(Prepare);
  DECLARE_METHOD(PreparePK);
  DECLARE_METHOD(GetPool);
  DECLARE_METHOD(Update);
  DECLARE_METHOD(UpdatePK);
  DECLARE_METHOD(TearDown);
};

TYPE_BUILDER(Updater, Updater);

}  // namespace terrier::execution
