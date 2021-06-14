#pragma once

#include <memory>
#include <string>
#include <utility>

#include "common/managed_pointer.h"
#include "parser/parse_result.h"
#include "parser/select_statement.h"

namespace noisepage {

/**
 * Common utility functions for binder tests.
 */
class BinderTestUtil {
 public:
  /**
   * Static class cannot be instantitated.
   */
  BinderTestUtil() = delete;

  /**
   * Get the SELECT statement from a raw SQL query.
   * @param sql The query string
   * @return A pair of the parse tree and the SELECT statement;
   * we must return both in order to extend the parse tree's lifetime
   */
  static std::pair<std::unique_ptr<parser::ParseResult>, common::ManagedPointer<parser::SelectStatement>>
  ParseToSelectStatement(const std::string &sql);
};

}  // namespace noisepage
