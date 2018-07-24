//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// printable.cpp
//
// Identification: src/common/printable.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include <iostream>
#include "common/printable.h"

namespace terrier {

Json::Value Printable::GetPrintable (){
  return json_value_;
}

std::string Printable::GetInfo () {
  return json_value_.toStyledString();
}

}  // namespace terrier
