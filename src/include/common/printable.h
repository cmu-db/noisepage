//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// printable.h
//
// Identification: src/include/common/printable.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iosfwd>
#include <string>

#ifdef __APPLE__
#include <json/json.h>
#else
#include <jsoncpp/json/json.h>
#endif

namespace terrier {

//===--------------------------------------------------------------------===//
// Printable Object
//===--------------------------------------------------------------------===//

/**
 * @brief Interface to allow printing of debug information
 *
 * Most stateful classes should implement this interface and return useful
 * information about its state for debugging purposes.
 */
class Printable {
 public:
  virtual ~Printable(){};

  /** @brief Set the Json value about the printable object. */
  virtual void SetPrintable() = 0;

  /** @brief Get the Json value about the printable object. */
  Json::Value GetPrintable ();

  /** @brief Get the styled string from the Json value. */
  std::string GetInfo ();

 protected:
  /** The Json value about the printable object */
  Json::Value json_value_;
};

}  // namespace peloton
