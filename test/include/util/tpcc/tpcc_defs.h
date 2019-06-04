#pragma once

namespace terrier::tpcc {

enum class TransactionType : uint8_t { NewOrder, Payment, OrderStatus, Delivery, StockLevel };

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
}  // namespace terrier::tpcc