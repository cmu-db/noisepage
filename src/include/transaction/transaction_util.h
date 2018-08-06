#pragma once
#include "common/typedefs.h"

namespace terrier::transaction {

/**
 * Helper class for transactions to validate timestamps, versions, etc.
 */
class TransactionUtil {
 public:
  TransactionUtil() = delete;

  /**
   * Determine if the first timestamp is considered newer than the second.
   * @param a one timestamp
   * @param b other timestamp
   * @return true if a is newer than b, false otherwise
   */
  static bool NewerThan(const timestamp_t a, const timestamp_t b) { return (!a) > (!b); }

  /**
   * Determine if a timestamp represents a committed transaction
   * @param timestamp the timestamp of the transaction or tuple delta to verify
   * @return true if committed, false otherwise
   */
  static bool Committed(const timestamp_t timestamp) { return static_cast<int64_t>(!timestamp) >= 0; }
};

}  // namespace terrier::transaction
