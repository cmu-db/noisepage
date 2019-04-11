#pragma once

namespace terrier::tpcc {

struct TransactionArgs {
  struct NewOrderItem {
    int32_t ol_i_id;
    int32_t ol_supply_w_id;
    int32_t ol_quantity;
    bool remote;
  };

  int32_t w_id;
  int32_t d_id;
  int32_t c_id;
  int32_t ol_cnt;
  uint8_t rbk;
  std::vector<NewOrderItem> items;
  uint64_t o_entry_d;
  bool o_all_local;
};

// 2.4.1
template <class Random>
TransactionArgs BuildNewOrderArgs(Random *const generator, const int32_t w_id) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
  TransactionArgs args;
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
    if (num_warehouses_ == 1 || Util::RandomWithin<uint8_t>(1, 100, 0, generator) > 1) {
      ol_supply_w_id = w_id;
      remote = false;
    } else {
      int32_t remote_w_id;
      do {
        remote_w_id = Util::RandomWithin<uint8_t>(1, num_warehouses_, 0, generator);
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

}  // namespace terrier::tpcc
