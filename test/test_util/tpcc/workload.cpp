#include "test_util/tpcc/workload.h"

#include <vector>

namespace terrier::tpcc {

void Workload(const int8_t worker_id, Database *const tpcc_db, transaction::TransactionManager *const txn_manager,
              const std::vector<std::vector<TransactionArgs>> &precomputed_args, std::vector<Worker> *const workers) {
  auto new_order = NewOrder(tpcc_db);
  auto payment = Payment(tpcc_db);
  auto order_status = OrderStatus(tpcc_db);
  auto delivery = Delivery(tpcc_db);
  auto stock_level = StockLevel(tpcc_db);

  for (const auto &txn_args : precomputed_args[worker_id]) {
    switch (txn_args.type_) {
      case TransactionType::NewOrder: {
        new_order.Execute(txn_manager, tpcc_db, &((*workers)[worker_id]), txn_args);
        break;
      }
      case TransactionType::Payment: {
        payment.Execute(txn_manager, tpcc_db, &((*workers)[worker_id]), txn_args);
        break;
      }
      case TransactionType::OrderStatus: {
        order_status.Execute(txn_manager, tpcc_db, &((*workers)[worker_id]), txn_args);
        break;
      }
      case TransactionType::Delivery: {
        delivery.Execute(txn_manager, tpcc_db, &((*workers)[worker_id]), txn_args);
        break;
      }
      case TransactionType::StockLevel: {
        stock_level.Execute(txn_manager, tpcc_db, &((*workers)[worker_id]), txn_args);
        break;
      }
      default:
        throw std::runtime_error("Unexpected transaction type.");
    }
  }
}

void CleanUpVarlensInPrecomputedArgs(const std::vector<std::vector<TransactionArgs>> *const precomputed_args) {
  for (const auto &worker_id : *precomputed_args) {
    for (const auto &args : worker_id) {
      if ((args.type_ == TransactionType::Payment || args.type_ == TransactionType::OrderStatus) && args.use_c_last_ &&
          !args.c_last_.IsInlined()) {
        delete[] args.c_last_.Content();
      }
    }
  }
}

}  // namespace terrier::tpcc
