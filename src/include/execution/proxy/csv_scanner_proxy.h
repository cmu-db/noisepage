//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scanner_proxy.h
//
// Identification: src/include/execution/proxy/csv_scanner_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/proxy/proxy.h"
#include "execution/proxy/type_builder.h"
#include "execution/runtime/csv_scanner.h"
#include "execution/proxy/runtime_functions_proxy.h"

namespace terrier::execution {


PROXY(CSVScannerColumn) {
  DECLARE_MEMBER(0, type::Type, type);
  DECLARE_MEMBER(1, char *, ptr);
  DECLARE_MEMBER(2, uint32_t, len);
  DECLARE_MEMBER(3, bool, is_null);
  DECLARE_TYPE;
};

PROXY(CSVScanner) {
  DECLARE_MEMBER(0, char[sizeof(codegen::util::CSVScanner) -
                         sizeof(util::CSVScanner::Column *) -
                         sizeof(util::CSVScanner::Stats) - sizeof(uint32_t)],
                 opaque1);
  DECLARE_MEMBER(1, util::CSVScanner::Column *, cols);
  DECLARE_MEMBER(2, char[sizeof(util::CSVScanner::Stats) + sizeof(uint32_t)],
                 opaque2);
  DECLARE_TYPE;

  DECLARE_METHOD(Init);
  DECLARE_METHOD(Destroy);
  DECLARE_METHOD(Produce);
};

TYPE_BUILDER(CSVScanner, codegen::util::CSVScanner);
TYPE_BUILDER(CSVScannerColumn, codegen::util::CSVScanner::Column);


}  // namespace terrier::execution