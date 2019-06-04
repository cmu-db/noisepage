#pragma once

#include <vector>
#include "util/tpcc/util.h"

namespace terrier::tpcc {

enum class TransactionType : uint8_t { NewOrder, Payment, OrderStatus, Delivery, StockLevel };

// Txn distribution. New Order is not provided because it's the implicit difference of the txns below from 100. Default
// values come from TPC-C spec.
struct TransactionWeights {
  uint32_t w_payment = 43;
  uint32_t w_delivery = 4;
  uint32_t w_order_status = 4;
  uint32_t w_stock_level = 4;
};

/*
 * An infinite deck of randomly shuffled TPCC cards.
 */
class Deck {
 public:
  /*
   * Default deck suggested by section 5.2.4.2 of the specification.
   */
  Deck() {
    cards.reserve(23);
    for (uint32_t i = 0; i < 10; i++) cards.emplace_back(tpcc::TransactionType::NewOrder);
    for (uint32_t i = 0; i < 10; i++) cards.emplace_back(tpcc::TransactionType::Payment);
    cards.emplace_back(tpcc::TransactionType::OrderStatus);
    cards.emplace_back(tpcc::TransactionType::Delivery);
    cards.emplace_back(tpcc::TransactionType::StockLevel);
    std::shuffle(cards.begin(), cards.end(), generator_);
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
    TERRIER_ASSERT(txn_weights.w_payment >= 43, "At least 43% payment.");
    TERRIER_ASSERT(txn_weights.w_order_status >= 4, "At least 4% order status.");
    TERRIER_ASSERT(txn_weights.w_delivery >= 4, "At least 4% delivery.");
    TERRIER_ASSERT(txn_weights.w_payment >= 4, "At least 4% stock level.");
    TERRIER_ASSERT(
        txn_weights.w_payment + txn_weights.w_order_status + txn_weights.w_delivery + txn_weights.w_stock_level <= 100,
        "Weights cannot be more than 100.");

    auto min_payment = static_cast<uint32_t>(std::ceil(static_cast<double>(txn_weights.w_payment) / 100.0 * 23));
    auto min_order_status =
        static_cast<uint32_t>(std::ceil(static_cast<double>(txn_weights.w_order_status) / 100.0 * 23));
    auto min_delivery = static_cast<uint32_t>(std::ceil(static_cast<double>(txn_weights.w_delivery) / 100.0 * 23));
    auto min_stock_level =
        static_cast<uint32_t>(std::ceil(static_cast<double>(txn_weights.w_stock_level) / 100.0 * 23));

    uint32_t min_num_cards = min_payment + min_order_status + min_delivery + min_stock_level;
    TERRIER_ASSERT(min_num_cards <= 23, "Can't have more than 23 cards!");
    auto min_new_order = 23 - min_num_cards;

    cards.reserve(23);
    for (uint32_t i = 0; i < min_new_order; i++) cards.emplace_back(tpcc::TransactionType::NewOrder);
    for (uint32_t i = 0; i < min_payment; i++) cards.emplace_back(tpcc::TransactionType::Payment);
    for (uint32_t i = 0; i < min_order_status; i++) cards.emplace_back(tpcc::TransactionType::OrderStatus);
    for (uint32_t i = 0; i < min_delivery; i++) cards.emplace_back(tpcc::TransactionType::Delivery);
    for (uint32_t i = 0; i < min_stock_level; i++) cards.emplace_back(tpcc::TransactionType::StockLevel);

    auto UNUSED_ATTRIBUTE c_new_order = std::count_if(
        cards.begin(), cards.end(), [](tpcc::TransactionType txn) { return txn == tpcc::TransactionType::NewOrder; });
    auto UNUSED_ATTRIBUTE c_payment = std::count_if(
        cards.begin(), cards.end(), [](tpcc::TransactionType txn) { return txn == tpcc::TransactionType::Payment; });
    auto UNUSED_ATTRIBUTE c_order_status = std::count_if(cards.begin(), cards.end(), [](tpcc::TransactionType txn) {
      return txn == tpcc::TransactionType::OrderStatus;
    });
    auto UNUSED_ATTRIBUTE c_delivery = std::count_if(
        cards.begin(), cards.end(), [](tpcc::TransactionType txn) { return txn == tpcc::TransactionType::Delivery; });
    auto UNUSED_ATTRIBUTE c_stock_level = std::count_if(
        cards.begin(), cards.end(), [](tpcc::TransactionType txn) { return txn == tpcc::TransactionType::StockLevel; });
    TERRIER_ASSERT(static_cast<double>(c_payment) / 23.0 * 100 >= txn_weights.w_payment, "Payment weight unsatisfied.");
    TERRIER_ASSERT(static_cast<double>(c_order_status) / 23.0 * 100 >= txn_weights.w_order_status,
                   "Order status weight unsatisfied.");
    TERRIER_ASSERT(static_cast<double>(c_delivery) / 23.0 * 100 >= txn_weights.w_delivery,
                   "Delivery weight unsatisfied.");
    TERRIER_ASSERT(static_cast<double>(c_stock_level) / 23.0 * 100 >= txn_weights.w_stock_level,
                   "Stock level weight unsatisfied.");

    std::shuffle(cards.begin(), cards.end(), generator_);
  }

  tpcc::TransactionType NextCard() {
    auto next_txn = cards[card_idx++];
    if (card_idx == cards.size()) {
      std::shuffle(cards.begin(), cards.end(), generator_);
      card_idx = 0;
    }
    return next_txn;
  }

 private:
  std::default_random_engine generator_;
  std::vector<tpcc::TransactionType> cards;
  uint32_t card_idx = 0;
};

/**
 * Contains the input arguments for all transaction types, but only args for the matching type are populated.
 */
struct TransactionArgs {
  TransactionType type;

  struct NewOrderItem {
    int32_t ol_i_id;
    int8_t ol_supply_w_id;
    int8_t ol_quantity;
    bool remote;
  };

  int8_t w_id;                      // NewOrder, Payment, Order Status, Delivery, StockLevel
  int8_t d_id;                      // NewOrder, Payment, Order Status, StockLevel
  int32_t c_id;                     // NewOrder, Payment, Order Status
  int8_t ol_cnt;                    // NewOrder
  uint8_t rbk;                      // NewOrder
  std::vector<NewOrderItem> items;  // NewOrder
  uint64_t o_entry_d;               // NewOrder
  bool o_all_local;                 // NewOrder
  int8_t c_d_id;                    // Payment
  int8_t c_w_id;                    // Payment
  bool remote;                      // Payment
  bool use_c_last;                  // Payment, Order Status
  storage::VarlenEntry c_last;      // Payment, Order Status
  double h_amount;                  // Payment
  uint64_t h_date;                  // Payment
  int8_t o_carrier_id;              // Delivery
  uint64_t ol_delivery_d;           // Delivery
  int8_t s_quantity_threshold;      // StockLevel
};

// 2.4.1
template <class Random>
TransactionArgs BuildNewOrderArgs(Random *const generator, const int8_t w_id, const int8_t num_warehouses) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses, "Invalid w_id.");
  TransactionArgs args;
  args.type = TransactionType::NewOrder;
  args.w_id = w_id;
  args.d_id = Util::RandomWithin<int8_t>(1, 10, 0, generator);
  args.c_id = Util::NURand(1023, 1, 3000, generator);
  args.ol_cnt = Util::RandomWithin<int8_t>(5, 15, 0, generator);
  args.rbk = Util::RandomWithin<uint8_t>(1, 100, 0, generator);
  args.o_all_local = true;

  args.items.reserve(args.ol_cnt);

  for (int32_t i = 0; i < args.ol_cnt; i++) {
    int32_t ol_i_id = (i == args.ol_cnt - 1 && args.rbk == 1) ? 8491138 : Util::NURand(8191, 1, 100000, generator);
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
      args.o_all_local = false;
    }
    int8_t ol_quantity = Util::RandomWithin<uint8_t>(1, 10, 0, generator);
    args.items.push_back({ol_i_id, ol_supply_w_id, ol_quantity, remote});
  }
  args.o_entry_d = Util::Timestamp();
  return args;
}

// 2.5.1
template <class Random>
TransactionArgs BuildPaymentArgs(Random *const generator, const int8_t w_id, const int8_t num_warehouses) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses, "Invalid w_id.");
  TransactionArgs args;
  args.type = TransactionType::Payment;
  args.w_id = w_id;
  args.d_id = Util::RandomWithin<int8_t>(1, 10, 0, generator);
  if (Util::RandomWithin<int8_t>(1, 100, 0, generator) <= 85) {
    args.c_d_id = args.d_id;
    args.c_w_id = args.w_id;
    args.remote = false;
  } else {
    args.c_d_id = Util::RandomWithin<int8_t>(1, 10, 0, generator);
    int8_t remote_w_id;
    do {
      remote_w_id = Util::RandomWithin<int8_t>(1, num_warehouses, 0, generator);
    } while (num_warehouses > 1 && remote_w_id == w_id);
    args.c_w_id = remote_w_id;
    args.remote = true;
  }
  if (Util::RandomWithin<int8_t>(1, 100, 0, generator) <= 60) {
    args.c_last = Util::LastNameVarlenEntry(static_cast<uint16_t>(Util::NURand(255, 0, 999, generator)));
    args.use_c_last = true;
  } else {
    args.c_id = Util::NURand(1023, 1, 3000, generator);
    args.use_c_last = false;
  }
  args.h_amount = Util::RandomWithin<double>(100, 500000, 2, generator);
  args.h_date = Util::Timestamp();
  return args;
}

// 2.6.1
template <class Random>
TransactionArgs BuildOrderStatusArgs(Random *const generator, const int8_t w_id, const int8_t num_warehouses) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses, "Invalid w_id.");
  TransactionArgs args;
  args.type = TransactionType::OrderStatus;
  args.w_id = w_id;
  args.d_id = Util::RandomWithin<int8_t>(1, 10, 0, generator);
  if (Util::RandomWithin<int8_t>(1, 100, 0, generator) <= 60) {
    args.c_last = Util::LastNameVarlenEntry(static_cast<uint16_t>(Util::NURand(255, 0, 999, generator)));
    args.use_c_last = true;
  } else {
    args.c_id = Util::NURand(1023, 1, 3000, generator);
    args.use_c_last = false;
  }
  return args;
}

// 2.7.1
template <class Random>
TransactionArgs BuildDeliveryArgs(Random *const generator, const int8_t w_id, const int8_t num_warehouses) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses, "Invalid w_id.");
  TransactionArgs args;
  args.type = TransactionType::Delivery;
  args.w_id = w_id;
  args.o_carrier_id = Util::RandomWithin<int8_t>(1, 10, 0, generator);
  args.ol_delivery_d = Util::Timestamp();
  return args;
}

// 2.8.1
template <class Random>
TransactionArgs BuildStockLevelArgs(Random *const generator, const int8_t w_id, const int8_t num_warehouses) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses, "Invalid w_id.");
  TransactionArgs args;
  args.type = TransactionType::StockLevel;
  args.w_id = w_id;
  args.d_id = Util::RandomWithin<int8_t>(1, 10, 0, generator);  // specification doesn't specify computing this
  args.s_quantity_threshold = Util::RandomWithin<int8_t>(10, 20, 0, generator);
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
 * Clean up the buffers from any non-inlined VarlenEntrys in the precomputed args
 * @param precomputed_args
 */
void CleanUpVarlensInPrecomputedArgs(const std::vector<std::vector<TransactionArgs>> *const precomputed_args) {
  for (const auto &worker_id : *precomputed_args) {
    for (const auto &args : worker_id) {
      if ((args.type == TransactionType::Payment || args.type == TransactionType::OrderStatus) && args.use_c_last &&
          !args.c_last.IsInlined()) {
        delete[] args.c_last.Content();
      }
    }
  }
}
}  // namespace terrier::tpcc
