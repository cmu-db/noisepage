#pragma once

#include <vector>

namespace noisepage::execution::sql {
struct Val;
}  // namespace noisepage::execution::sql

namespace noisepage::util {

/**
 * Signature of a function that is capable of processing rows retrieved
 * from ExecuteDML or ExecuteQuery. This function is invoked once per
 * row, with the argument being a row's attributes.
 */
using TupleFunction = std::function<void(const std::vector<execution::sql::Val *> &)>;

}  // namespace noisepage::util
