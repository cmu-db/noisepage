#pragma once

#include <algorithm>
#include <map>
#include <string_view>
#include "catalog/catalog_defs.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "tpcc/database.h"
#include "tpcc/util.h"
#include "tpcc/worker.h"
#include "tpcc/workload.h"
#include "transaction/transaction_manager.h"
#include "util/transaction_benchmark_util.h"

namespace terrier::tpcc {

class StockLevel {
 private:
 public:
  explicit StockLevel(const Database *const db) {}

  // 2.4.2
  template <class Random>
  bool Execute(transaction::TransactionManager *const txn_manager, Random *const generator, Database *const db,
               Worker *const worker, const TransactionArgs &args) const {
    TERRIER_ASSERT(args.type == TransactionType::StockLevel, "Wrong transaction type.");
    // TODO(WAN): missing range scans for limit
    return true;
  }
};

}  // namespace terrier::tpcc
