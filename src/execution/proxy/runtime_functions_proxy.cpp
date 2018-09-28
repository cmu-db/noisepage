//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// runtime_functions_proxy.cpp
//
// Identification: src/execution/proxy/runtime_functions_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/runtime_functions_proxy.h"

#include "execution/proxy/data_table_proxy.h"
#include "execution/proxy/executor_context_proxy.h"
#include "execution/proxy/tile_group_proxy.h"
#include "execution/proxy/zone_map_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(ColumnLayoutInfo, "peloton::ColumnLayoutInfo", col_start_ptr,
            stride, columnar);

DEFINE_TYPE(AbstractExpression, "peloton::expression::AbstractExpression",
            opaque);

DEFINE_TYPE(Type, "peloton::Type", opaque);

DEFINE_METHOD(peloton::codegen, RuntimeFunctions, HashMurmur3);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, HashCrc64);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, GetTileGroup);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, GetTileGroupLayout);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, FillPredicateArray);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, ExecuteTableScan);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, ExecutePerState);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, ThrowDivideByZeroException);
DEFINE_METHOD(peloton::codegen, RuntimeFunctions, ThrowOverflowException);

}  // namespace codegen
}  // namespace peloton