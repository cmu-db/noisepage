#pragma once

#include <algorithm>
#include <cstdlib>
#include <set>
#include <string>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/column_value_expression.h"

namespace terrier::parser::expression {

/**
 * An utility class for Expression objects
 */
class ExpressionUtil {
 public:
  // Static utility class
  ExpressionUtil() = delete;


};

}  // namespace terrier::parser::expression
