//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// attribute_info.h
//
// Identification: src/include/planner/attribute_info.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "common/typedefs.h"

namespace terrier::sql::plannode {

/**
 * Describes an attribute that is passed around in the query plan
 */
struct AttributeInfo {
  /**
   * The actual type of this attribute (smallint, integer, varchar etc.)
   */
  codegen::type::Type type;

  /**
   * The ID of the attribute
   */
  col_oid_t attribute_id;

  /**
   * The name of this attribute. This isn't always available, so no one should
   * rely on its existence.
   */
  std::string name;
};

}  // namespace terrier::sql::plannode
