//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// utility.h
//
// Identification: src/include/common/utility.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <string>

namespace terrier{

  int peloton_close(int fd);

  std::string peloton_error_message();
}
