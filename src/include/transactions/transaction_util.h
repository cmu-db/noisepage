#pragma once
#include "common/typedefs.h"
#include "transactions/transaction_util.h"

namespace terrier::transactions {

/**
 * Helper class for transactions to validate timestamps, versions, etc.
 */
class TransactionUtil {
 public:
  /**
   * Determine if a timestamp represents a committed transaction
   * @param timestamp the timestamp of the transaction or tuple delta to verify
   * @return true if committed, false otherwise
   */
  static bool Committed(const timestamp_t timestamp) { return static_cast<int64_t>(timestamp) >= 0; }
};

}  // namespace terrier::transactions
