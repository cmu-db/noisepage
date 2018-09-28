//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table_proxy.cpp
//
// Identification: src/execution/proxy/data_table_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/data_table_proxy.h"

namespace terrier::execution {


DEFINE_TYPE(DataTable, "storage::DataTable", opaque);

DEFINE_METHOD(peloton::storage, DataTable, GetTileGroupCount);


}  // namespace terrier::execution
