#pragma once

#include "tpcc/util.h"

namespace terrier::tpcc {

enum class TransactionType : uint8_t { NewOrder, Payment, OrderStatus, Delivery, StockLevel };

struct TransactionArgs {
  TransactionType type;

  struct NewOrderItem {
    int32_t ol_i_id;
    int32_t ol_supply_w_id;
    int32_t ol_quantity;
    bool remote;
  };

  int32_t w_id;                     // NewOrder, Payment
  int32_t d_id;                     // NewOrder, Payment
  int32_t c_id;                     // NewOrder, Payment
  int32_t ol_cnt;                   // NewOrder
  uint8_t rbk;                      // NewOrder
  std::vector<NewOrderItem> items;  // NewOrder
  uint64_t o_entry_d;               // NewOrder
  bool o_all_local;                 // NewOrder
  int32_t c_d_id;                   // Payment
  int32_t c_w_id;                   // Payment
  bool remote;                      // Payment
  bool use_c_last;                  // Payment
  storage::VarlenEntry c_last;      // Payment
  double h_amount;                  // Payment
  uint64_t h_date;                  // Payment
};

// 2.4.1
template <class Random>
TransactionArgs BuildNewOrderArgs(Random *const generator, const int32_t w_id, const uint32_t num_warehouses) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses, "Invalid w_id.");
  TransactionArgs args;
  args.type = TransactionType::NewOrder;
  args.w_id = w_id;
  args.d_id = Util::RandomWithin<int32_t>(1, 10, 0, generator);
  args.c_id = Util::NURand(1023, 1, 3000, generator);
  args.ol_cnt = Util::RandomWithin<int32_t>(5, 15, 0, generator);
  args.rbk = Util::RandomWithin<uint8_t>(1, 100, 0, generator);
  args.o_all_local = true;

  args.items.reserve(args.ol_cnt);

  for (int32_t i = 0; i < args.ol_cnt; i++) {
    int32_t ol_i_id = (i == args.ol_cnt - 1 && args.rbk == 1) ? 8491138 : Util::NURand(8191, 1, 100000, generator);
    int32_t ol_supply_w_id;
    bool remote;
    if (num_warehouses == 1 || Util::RandomWithin<uint8_t>(1, 100, 0, generator) > 1) {
      ol_supply_w_id = w_id;
      remote = false;
    } else {
      int32_t remote_w_id;
      do {
        remote_w_id = Util::RandomWithin<uint8_t>(1, num_warehouses, 0, generator);
      } while (remote_w_id == w_id);
      ol_supply_w_id = remote_w_id;
      remote = true;
      args.o_all_local = false;
    }
    int32_t ol_quantity = Util::RandomWithin<uint8_t>(1, 10, 0, generator);
    args.items.push_back({ol_i_id, ol_supply_w_id, ol_quantity, remote});
  }
  args.o_entry_d = Util::Timestamp();
  return args;
}

// 2.5.1
template <class Random>
TransactionArgs BuildPaymentArgs(Random *const generator, const int32_t w_id, const uint32_t num_warehouses) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses, "Invalid w_id.");
  TransactionArgs args;
  args.type = TransactionType::Payment;
  args.w_id = w_id;
  args.d_id = Util::RandomWithin<int32_t>(1, 10, 0, generator);
  if (Util::RandomWithin<int32_t>(1, 100, 0, generator) <= 85) {
    args.c_d_id = args.d_id;
    args.c_w_id = args.w_id;
    args.remote = false;
  } else {
    args.c_d_id = Util::RandomWithin<int32_t>(1, 10, 0, generator);
    int32_t remote_w_id;
    do {
      remote_w_id = Util::RandomWithin<uint8_t>(1, num_warehouses, 0, generator);
    } while (num_warehouses > 1 && remote_w_id == w_id);
    args.c_w_id = remote_w_id;
    args.remote = true;
  }
  if (Util::RandomWithin<int32_t>(1, 100, 0, generator) <= 60) {
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

template <class Random>
TransactionArgs BuildOrderStatusArgs(Random *const generator, const int32_t w_id) {
  TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
  TransactionArgs args;
  args.type = TransactionType::OrderStatus;
  return args;
}

template <class Random>
TransactionArgs BuildDeliveryArgs(Random *const generator, const int32_t w_id) {
  TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
  TransactionArgs args;
  args.type = TransactionType::Delivery;
  return args;
}

template <class Random>
TransactionArgs BuildStockLevelArgs(Random *const generator, const int32_t w_id) {
  TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
  TransactionArgs args;
  args.type = TransactionType::StockLevel;
  return args;
}

}  // namespace terrier::tpcc
