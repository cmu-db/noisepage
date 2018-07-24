//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// json_serializable.h
//
// Identification: src/include/common/json_serializable.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#ifdef __APPLE__
#include <json/json.h>
#else
#include <jsoncpp/json/json.h>
#endif

namespace terrier {

//===--------------------------------------------------------------------===//
// Json Serializable Object
//===--------------------------------------------------------------------===//

/**
 * @brief Interface to allow printing of debug information in Json foramt
 *
 * Most stateful classes should implement this interface and return useful
 * information about its state for debugging purposes.
 */
class JsonSerializable {
 public:
  virtual ~JsonSerializable(){};

  /** @brief Get the Json value about the state information. */
  virtual Json::Value GetJson () = 0;
};

}  // namespace peloton
