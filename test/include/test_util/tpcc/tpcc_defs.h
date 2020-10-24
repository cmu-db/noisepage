#pragma once

#include <vector>

namespace noisepage::tpcc {

enum class TransactionType : uint8_t { NewOrder, Payment, OrderStatus, Delivery, StockLevel };

/**
 * Contains the input arguments for all transaction types, but only args for the matching type are populated.
 */
struct TransactionArgs {
  TransactionType type_;

  struct NewOrderItem {
    int32_t ol_i_id_;
    int8_t ol_supply_w_id_;
    int8_t ol_quantity_;
    bool remote_;
  };

  int8_t w_id_;                      // NewOrder, Payment, Order Status, Delivery, StockLevel
  int8_t d_id_;                      // NewOrder, Payment, Order Status, StockLevel
  int32_t c_id_;                     // NewOrder, Payment, Order Status
  int8_t ol_cnt_;                    // NewOrder
  uint8_t rbk_;                      // NewOrder
  std::vector<NewOrderItem> items_;  // NewOrder
  uint64_t o_entry_d_;               // NewOrder
  bool o_all_local_;                 // NewOrder
  int8_t c_d_id_;                    // Payment
  int8_t c_w_id_;                    // Payment
  bool remote_;                      // Payment
  bool use_c_last_;                  // Payment, Order Status
  storage::VarlenEntry c_last_;      // Payment, Order Status
  double h_amount_;                  // Payment
  uint64_t h_date_;                  // Payment
  int8_t o_carrier_id_;              // Delivery
  uint64_t ol_delivery_d_;           // Delivery
  int8_t s_quantity_threshold_;      // StockLevel
};
}  // namespace noisepage::tpcc
