#pragma once

#include <vector>

#include "test_util/tpcc/delivery.h"
#include "test_util/tpcc/new_order.h"
#include "test_util/tpcc/order_status.h"
#include "test_util/tpcc/payment.h"
#include "test_util/tpcc/stock_level.h"
#include "test_util/tpcc/tpcc_defs.h"
#include "test_util/tpcc/util.h"

namespace terrier::tpcc {

// Txn distribution. New Order is not provided because it's the implicit difference of the txns below from 100. Default
// values come from TPC-C spec.
struct TransactionWeights {
  uint32_t w_payment_ = 43;
  uint32_t w_delivery_ = 4;
  uint32_t w_order_status_ = 4;
  uint32_t w_stock_level_ = 4;
};

/*
 * An infinite deck of randomly shuffled TPCC cards_.
 */
class Deck {
 public:
  /*
   * Default deck suggested by section 5.2.4.2 of the specification.
   */
  Deck() {
    cards_.reserve(23);
    for (uint32_t i = 0; i < 10; i++) cards_.emplace_back(tpcc::TransactionType::NewOrder);
    for (uint32_t i = 0; i < 10; i++) cards_.emplace_back(tpcc::TransactionType::Payment);
    cards_.emplace_back(tpcc::TransactionType::OrderStatus);
    cards_.emplace_back(tpcc::TransactionType::Delivery);
    cards_.emplace_back(tpcc::TransactionType::StockLevel);
    std::shuffle(cards_.begin(), cards_.end(), generator_);
  }

  /*
   * User-specified transaction mix. The sum of the weights must be divisible by 100,
   * and the overall transaction mix must satisfy TPCC minimums, which are:
   * 43% payment, 4% order_status, 4% delivery, 4% stock_level
   *
   * The weight of new_order will be approximately (100% - sum of the others), because the TPCC spec
   * calls for deck sizes to be multiples of 23, an exact lower bound cannot always be achieved.
   */
  explicit Deck(const TransactionWeights &txn_weights) {
    TERRIER_ASSERT(txn_weights.w_payment_ >= 43, "At least 43% payment.");
    TERRIER_ASSERT(txn_weights.w_order_status_ >= 4, "At least 4% order status.");
    TERRIER_ASSERT(txn_weights.w_delivery_ >= 4, "At least 4% delivery.");
    TERRIER_ASSERT(txn_weights.w_payment_ >= 4, "At least 4% stock level.");
    TERRIER_ASSERT(
        txn_weights.w_payment_ + txn_weights.w_order_status_ + txn_weights.w_delivery_ + txn_weights.w_stock_level_ <=
            100,
        "Weights cannot be more than 100.");

    auto min_payment = static_cast<uint32_t>(std::ceil(static_cast<double>(txn_weights.w_payment_) / 100.0 * 23));
    auto min_order_status =
        static_cast<uint32_t>(std::ceil(static_cast<double>(txn_weights.w_order_status_) / 100.0 * 23));
    auto min_delivery = static_cast<uint32_t>(std::ceil(static_cast<double>(txn_weights.w_delivery_) / 100.0 * 23));
    auto min_stock_level =
        static_cast<uint32_t>(std::ceil(static_cast<double>(txn_weights.w_stock_level_) / 100.0 * 23));

    uint32_t min_num_cards = min_payment + min_order_status + min_delivery + min_stock_level;
    TERRIER_ASSERT(min_num_cards <= 23, "Can't have more than 23 cards!");
    auto min_new_order = 23 - min_num_cards;

    cards_.reserve(23);
    for (uint32_t i = 0; i < min_new_order; i++) cards_.emplace_back(tpcc::TransactionType::NewOrder);
    for (uint32_t i = 0; i < min_payment; i++) cards_.emplace_back(tpcc::TransactionType::Payment);
    for (uint32_t i = 0; i < min_order_status; i++) cards_.emplace_back(tpcc::TransactionType::OrderStatus);
    for (uint32_t i = 0; i < min_delivery; i++) cards_.emplace_back(tpcc::TransactionType::Delivery);
    for (uint32_t i = 0; i < min_stock_level; i++) cards_.emplace_back(tpcc::TransactionType::StockLevel);

    auto UNUSED_ATTRIBUTE c_new_order = std::count_if(
        cards_.begin(), cards_.end(), [](tpcc::TransactionType txn) { return txn == tpcc::TransactionType::NewOrder; });
    auto UNUSED_ATTRIBUTE c_payment = std::count_if(
        cards_.begin(), cards_.end(), [](tpcc::TransactionType txn) { return txn == tpcc::TransactionType::Payment; });
    auto UNUSED_ATTRIBUTE c_order_status = std::count_if(cards_.begin(), cards_.end(), [](tpcc::TransactionType txn) {
      return txn == tpcc::TransactionType::OrderStatus;
    });
    auto UNUSED_ATTRIBUTE c_delivery = std::count_if(
        cards_.begin(), cards_.end(), [](tpcc::TransactionType txn) { return txn == tpcc::TransactionType::Delivery; });
    auto UNUSED_ATTRIBUTE c_stock_level = std::count_if(cards_.begin(), cards_.end(), [](tpcc::TransactionType txn) {
      return txn == tpcc::TransactionType::StockLevel;
    });
    TERRIER_ASSERT(static_cast<double>(c_payment) / 23.0 * 100 >= txn_weights.w_payment_,
                   "Payment weight unsatisfied.");
    TERRIER_ASSERT(static_cast<double>(c_order_status) / 23.0 * 100 >= txn_weights.w_order_status_,
                   "Order status weight unsatisfied.");
    TERRIER_ASSERT(static_cast<double>(c_delivery) / 23.0 * 100 >= txn_weights.w_delivery_,
                   "Delivery weight unsatisfied.");
    TERRIER_ASSERT(static_cast<double>(c_stock_level) / 23.0 * 100 >= txn_weights.w_stock_level_,
                   "Stock level weight unsatisfied.");

    std::shuffle(cards_.begin(), cards_.end(), generator_);
  }

  tpcc::TransactionType NextCard() {
    auto next_txn = cards_[card_idx_++];
    if (card_idx_ == cards_.size()) {
      std::shuffle(cards_.begin(), cards_.end(), generator_);
      card_idx_ = 0;
    }
    return next_txn;
  }

 private:
  std::default_random_engine generator_;
  std::vector<tpcc::TransactionType> cards_;
  uint32_t card_idx_ = 0;
};

// 2.4.1
template <class Random>
TransactionArgs BuildNewOrderArgs(Random *const generator, const int8_t w_id, const int8_t num_warehouses) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses, "Invalid w_id.");
  TransactionArgs args;
  args.type_ = TransactionType::NewOrder;
  args.w_id_ = w_id;
  args.d_id_ = Util::RandomWithin<int8_t>(1, 10, 0, generator);
  args.c_id_ = Util::NURand(1023, 1, 3000, generator);
  args.ol_cnt_ = Util::RandomWithin<int8_t>(5, 15, 0, generator);
  args.rbk_ = Util::RandomWithin<uint8_t>(1, 100, 0, generator);
  args.o_all_local_ = true;

  args.items_.reserve(args.ol_cnt_);

  for (int32_t i = 0; i < args.ol_cnt_; i++) {
    int32_t ol_i_id = (i == args.ol_cnt_ - 1 && args.rbk_ == 1) ? 8491138 : Util::NURand(8191, 1, 100000, generator);
    int8_t ol_supply_w_id;
    bool remote;
    if (num_warehouses == 1 || Util::RandomWithin<uint8_t>(1, 100, 0, generator) > 1) {
      ol_supply_w_id = w_id;
      remote = false;
    } else {
      int8_t remote_w_id;
      do {
        remote_w_id = Util::RandomWithin<uint8_t>(1, num_warehouses, 0, generator);
      } while (remote_w_id == w_id);
      ol_supply_w_id = remote_w_id;
      remote = true;
      args.o_all_local_ = false;
    }
    int8_t ol_quantity = Util::RandomWithin<uint8_t>(1, 10, 0, generator);
    args.items_.push_back({ol_i_id, ol_supply_w_id, ol_quantity, remote});
  }
  args.o_entry_d_ = Util::Timestamp();
  return args;
}

// 2.5.1
template <class Random>
TransactionArgs BuildPaymentArgs(Random *const generator, const int8_t w_id, const int8_t num_warehouses) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses, "Invalid w_id.");
  TransactionArgs args;
  args.type_ = TransactionType::Payment;
  args.w_id_ = w_id;
  args.d_id_ = Util::RandomWithin<int8_t>(1, 10, 0, generator);
  if (Util::RandomWithin<int8_t>(1, 100, 0, generator) <= 85) {
    args.c_d_id_ = args.d_id_;
    args.c_w_id_ = args.w_id_;
    args.remote_ = false;
  } else {
    args.c_d_id_ = Util::RandomWithin<int8_t>(1, 10, 0, generator);
    int8_t remote_w_id;
    do {
      remote_w_id = Util::RandomWithin<int8_t>(1, num_warehouses, 0, generator);
    } while (num_warehouses > 1 && remote_w_id == w_id);
    args.c_w_id_ = remote_w_id;
    args.remote_ = true;
  }
  if (Util::RandomWithin<int8_t>(1, 100, 0, generator) <= 60) {
    args.c_last_ = Util::LastNameVarlenEntry(static_cast<uint16_t>(Util::NURand(255, 0, 999, generator)));
    args.use_c_last_ = true;
  } else {
    args.c_id_ = Util::NURand(1023, 1, 3000, generator);
    args.use_c_last_ = false;
  }
  args.h_amount_ = Util::RandomWithin<double>(100, 500000, 2, generator);
  args.h_date_ = Util::Timestamp();
  return args;
}

// 2.6.1
template <class Random>
TransactionArgs BuildOrderStatusArgs(Random *const generator, const int8_t w_id, const int8_t num_warehouses) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses, "Invalid w_id.");
  TransactionArgs args;
  args.type_ = TransactionType::OrderStatus;
  args.w_id_ = w_id;
  args.d_id_ = Util::RandomWithin<int8_t>(1, 10, 0, generator);
  if (Util::RandomWithin<int8_t>(1, 100, 0, generator) <= 60) {
    args.c_last_ = Util::LastNameVarlenEntry(static_cast<uint16_t>(Util::NURand(255, 0, 999, generator)));
    args.use_c_last_ = true;
  } else {
    args.c_id_ = Util::NURand(1023, 1, 3000, generator);
    args.use_c_last_ = false;
  }
  return args;
}

// 2.7.1
template <class Random>
TransactionArgs BuildDeliveryArgs(Random *const generator, const int8_t w_id, const int8_t num_warehouses) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses, "Invalid w_id.");
  TransactionArgs args;
  args.type_ = TransactionType::Delivery;
  args.w_id_ = w_id;
  args.o_carrier_id_ = Util::RandomWithin<int8_t>(1, 10, 0, generator);
  args.ol_delivery_d_ = Util::Timestamp();
  return args;
}

// 2.8.1
template <class Random>
TransactionArgs BuildStockLevelArgs(Random *const generator, const int8_t w_id, const int8_t num_warehouses) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses, "Invalid w_id.");
  TransactionArgs args;
  args.type_ = TransactionType::StockLevel;
  args.w_id_ = w_id;
  args.d_id_ = Util::RandomWithin<int8_t>(1, 10, 0, generator);  // specification doesn't specify computing this
  args.s_quantity_threshold_ = Util::RandomWithin<int8_t>(10, 20, 0, generator);
  return args;
}

template <class Random>
std::vector<std::vector<TransactionArgs>> PrecomputeArgs(Random *const generator, const TransactionWeights &txn_weights,
                                                         const int8_t num_threads,
                                                         const uint32_t num_precomputed_txns_per_worker) {
  Deck deck(txn_weights);
  std::vector<std::vector<TransactionArgs>> precomputed_args;
  precomputed_args.reserve(num_threads);

  for (int8_t warehouse_id = 1; warehouse_id <= num_threads; warehouse_id++) {
    std::vector<TransactionArgs> txns;
    txns.reserve(num_precomputed_txns_per_worker);
    for (uint32_t i = 0; i < num_precomputed_txns_per_worker; i++) {
      switch (deck.NextCard()) {
        case TransactionType::NewOrder:
          txns.emplace_back(BuildNewOrderArgs(generator, warehouse_id, num_threads));
          break;
        case TransactionType::Payment:
          txns.emplace_back(BuildPaymentArgs(generator, warehouse_id, num_threads));
          break;
        case TransactionType::OrderStatus:
          txns.emplace_back(BuildOrderStatusArgs(generator, warehouse_id, num_threads));
          break;
        case TransactionType::Delivery:
          txns.emplace_back(BuildDeliveryArgs(generator, warehouse_id, num_threads));
          break;
        case TransactionType::StockLevel:
          txns.emplace_back(BuildStockLevelArgs(generator, warehouse_id, num_threads));
          break;
        default:
          throw std::runtime_error("Unexpected transaction type.");
      }
    }
    precomputed_args.emplace_back(txns);
  }

  return precomputed_args;
}

/**
 * Function to invoke for a single worker thread to invoke its TPC-C workload to completion
 * @param worker_id 1-indexed thread id
 * @param tpcc_db pointer to the database
 * @param txn_manager pointer to the txn_manager
 * @param precomputed_args all of the precomputed args for this TPC-C run
 * @param workers preallocated workers with buffers to use for execution
 */
void Workload(int8_t worker_id, Database *tpcc_db, transaction::TransactionManager *txn_manager,
              const std::vector<std::vector<TransactionArgs>> &precomputed_args, std::vector<Worker> *workers);

/**
 * Clean up the buffers from any non-inlined VarlenEntrys in the precomputed args
 * @param precomputed_args args to look for non-inlined VarlenEntrys to free
 */
void CleanUpVarlensInPrecomputedArgs(const std::vector<std::vector<TransactionArgs>> *precomputed_args);
}  // namespace terrier::tpcc
