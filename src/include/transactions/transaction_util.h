#pragma once
#include "transactions/transaction_util.h"

namespace terrier::transactions {

class TransactionUtil {
 public:
  static bool Committed(const timestamp_t timestamp) { return static_cast<int64_t>(timestamp) >= 0; }
};

}  // namespace terrier::transactions
