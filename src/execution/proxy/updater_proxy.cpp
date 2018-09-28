//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updater_proxy.cpp
//
// Identification: src/execution/proxy/updater_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/updater_proxy.h"

#include "execution/proxy/data_table_proxy.h"
#include "execution/proxy/executor_context_proxy.h"
#include "execution/proxy/pool_proxy.h"
#include "execution/proxy/target_proxy.h"
#include "execution/proxy/tile_group_proxy.h"
#include "execution/proxy/transaction_context_proxy.h"
#include "execution/proxy/value_proxy.h"

namespace terrier::execution {

DEFINE_TYPE(Updater, "Updater", opaque);

DEFINE_METHOD(peloton::codegen, Updater, Init);
DEFINE_METHOD(peloton::codegen, Updater, Prepare);
DEFINE_METHOD(peloton::codegen, Updater, PreparePK);
DEFINE_METHOD(peloton::codegen, Updater, GetPool);
DEFINE_METHOD(peloton::codegen, Updater, Update);
DEFINE_METHOD(peloton::codegen, Updater, UpdatePK);
DEFINE_METHOD(peloton::codegen, Updater, TearDown);

}  // namespace terrier::execution
