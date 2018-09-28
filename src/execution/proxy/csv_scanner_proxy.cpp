//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scanner_proxy.cpp
//
// Identification: src/execution/proxy/csv_scanner_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/csv_scanner_proxy.h"

#include "execution/proxy/executor_context_proxy.h"
#include "execution/proxy/runtime_functions_proxy.h"

namespace terrier::execution {


DEFINE_TYPE(CSVScanner, "runtime::CSVScanner", opaque1, cols, opaque2);

DEFINE_TYPE(CSVScannerColumn, "runtime::CSVScanner::Column", type, ptr, len,
            is_null);

DEFINE_METHOD(peloton::codegen::util, CSVScanner, Init);
DEFINE_METHOD(peloton::codegen::util, CSVScanner, Destroy);
DEFINE_METHOD(peloton::codegen::util, CSVScanner, Produce);


}  // namespace terrier::execution