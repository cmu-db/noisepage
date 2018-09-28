//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table_proxy.h
//
// Identification: src/include/execution/proxy/data_table_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/proxy/proxy.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

PROXY(DataTable) {
  /// We don't need access to internal fields, so use an opaque byte array
  DECLARE_MEMBER(0, char[sizeof(storage::DataTable)], opaque);
  DECLARE_TYPE;

  /// Proxy DataTable::GetTileGroupCount()
  DECLARE_METHOD(GetTileGroupCount);
};

TYPE_BUILDER(DataTable, storage::DataTable);

}  // namespace codegen
}  // namespace peloton