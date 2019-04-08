#pragma once

#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "tpcc/database.h"
#include "tpcc/loader.h"
#include "tpcc/util.h"
#include "transaction/transaction_manager.h"
#include "util/transaction_benchmark_util.h"

namespace terrier::tpcc {

struct NewOrderArgs {
  struct NO_Item {
    int32_t ol_i_id;
    int32_t ol_supply_w_id;
    int32_t ol_quantity;
  };

  int32_t w_id;
  int32_t d_id;
  int32_t c_id;
  int32_t ol_cnt;
  uint8_t rbk;
  std::vector<NO_Item> items;
  uint64_t o_entry_d;
};

// 2.4.1
template <class Random>
NewOrderArgs BuildNewOrderArgs(Random *const generator, const int32_t w_id) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
  NewOrderArgs args;
  args.w_id = w_id;
  args.d_id = Util::RandomWithin<int32_t>(1, 10, 0, generator);
  args.c_id = Util::NURand(1023, 1, 3000, generator);
  args.ol_cnt = Util::RandomWithin<int32_t>(5, 15, 0, generator);
  args.rbk = Util::RandomWithin<uint8_t>(1, 100, 0, generator);

  args.items.reserve(args.ol_cnt);

  for (int32_t i = 0; i < args.ol_cnt; i++) {
    int32_t ol_i_id = (i == args.ol_cnt - 1 && args.rbk == 1) ? 8491138 : Util::NURand(8191, 1, 100000, generator);
    int32_t ol_supply_w_id;
    if (Util::RandomWithin<uint8_t>(1, 100, 0, generator) > 1 && num_warehouses_ > 1) {
      ol_supply_w_id = w_id;
    } else {
      int32_t remote_w_id;
      do {
        remote_w_id = Util::RandomWithin<uint8_t>(1, num_warehouses_, 0, generator);
      } while (remote_w_id == w_id);
      ol_supply_w_id = remote_w_id;
    }
    int32_t ol_quantity = Util::RandomWithin<uint8_t>(1, 10, 0, generator);
    args.items.push_back({ol_i_id, ol_supply_w_id, ol_quantity});
  }
  args.o_entry_d = Util::Timestamp();
  return args;
}

struct Transactions {
  Transactions() = delete;

  template <class Random>
  static bool NewOrder(transaction::TransactionManager *const txn_manager, Random *const generator, Database *const db,
                       Worker *const worker, const NewOrderArgs &args) {
    auto *const txn = txn_manager->BeginTransaction();

    // Warehouse key
    const auto warehouse_key_pr_initializer = db->warehouse_index_->GetProjectedRowInitializer();
    const auto warehouse_key_pr_map = db->warehouse_index_->GetKeyOidToOffsetMap();

    // Look up w_id in index
    const auto *const warehouse_key =
        Loader::BuildWarehouseKey(args.w_id, worker->warehouse_key_buffer, warehouse_key_pr_initializer,
                                  warehouse_key_pr_map, db->warehouse_key_schema_);
    std::vector<storage::TupleSlot> index_scan_results;
    db->warehouse_index_->ScanKey(*warehouse_key, &index_scan_results);
    TERRIER_ASSERT(index_scan_results.size() == 1, "Warehouse index lookup failed.");

    // Look up W_TAX in table
    const auto [warehouse_tuple_pr_initializer, warehouse_tuple_pr_map] =
        db->warehouse_table_->InitializerForProjectedRow({db->warehouse_schema_.GetColumn(7).GetOid()});

    auto *const warehouse_tuple = warehouse_tuple_pr_initializer.InitializeRow(worker->warehouse_tuple_buffer);
    db->warehouse_table_->Select(txn, index_scan_results[0], warehouse_tuple);

    const auto UNUSED_ATTRIBUTE w_tax = *reinterpret_cast<double *>(warehouse_tuple->AccessWithNullCheck(0));

    //    std::cout << w_tax << std::endl;

    txn_manager->Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    return true;
  }
};

}  // namespace terrier::tpcc
