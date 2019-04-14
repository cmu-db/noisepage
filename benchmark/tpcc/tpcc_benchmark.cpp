#include <random>
#include <vector>
#include "benchmark/benchmark.h"
#include "common/macros.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "storage/garbage_collector.h"
#include "storage/storage_defs.h"
#include "tpcc/builder.h"
#include "tpcc/database.h"
#include "tpcc/delivery.h"
#include "tpcc/loader.h"
#include "tpcc/new_order.h"
#include "tpcc/order_status.h"
#include "tpcc/payment.h"
#include "tpcc/stock_level.h"
#include "tpcc/worker.h"
#include "tpcc/workload.h"
#include "transaction/transaction_manager.h"

namespace terrier {

#define LOG_FILE_NAME "./tpcc.log"

class TPCCBenchmark : public benchmark::Fixture {
 public:
  void StartLogging() {
    logging_ = true;
    log_thread_ = std::thread([this] { LogThreadLoop(); });
  }

  void EndLogging() {
    logging_ = false;
    log_thread_.join();
    log_manager_->Shutdown();
  }

  void StartGC(transaction::TransactionManager *const txn_manager) {
    gc_ = new storage::GarbageCollector(txn_manager);
    run_gc_ = true;
    gc_thread_ = std::thread([this] { GCThreadLoop(); });
  }

  void EndGC() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
  }

  const uint64_t blockstore_size_limit_ = 1000;
  const uint64_t blockstore_reuse_limit_ = 1000;
  const uint64_t buffersegment_size_limit_ = 1000000;
  const uint64_t buffersegment_reuse_limit_ = 1000000;
  storage::BlockStore block_store_{blockstore_size_limit_, blockstore_reuse_limit_};
  storage::RecordBufferSegmentPool buffer_pool_{buffersegment_size_limit_, buffersegment_reuse_limit_};
  std::default_random_engine generator_;
  storage::LogManager *log_manager_ = nullptr;

  const uint32_t num_threads_ = 4;
  const uint32_t num_precomputed_txns_per_worker_ = 100000;
  const uint32_t w_payment = 43;
  const uint32_t w_delivery = 4;
  const uint32_t w_order_status = 4;
  const uint32_t w_stock_level = 4;

  common::WorkerPool thread_pool_{num_threads_, {}};

 private:
  std::thread log_thread_;
  volatile bool logging_ = false;
  const std::chrono::milliseconds log_period_milli_{10};

  void LogThreadLoop() {
    while (logging_) {
      std::this_thread::sleep_for(log_period_milli_);
      log_manager_->Process();
    }
  }

  std::thread gc_thread_;
  storage::GarbageCollector *gc_ = nullptr;
  volatile bool run_gc_ = false;
  const std::chrono::milliseconds gc_period_{10};

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      gc_->PerformGarbageCollection();
    }
  }
};

/*
 * An infinite deck of randomly shuffled TPCC cards.
 */
class Deck {
 public:
  /*
   * Default deck suggested by TPCC specification.
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
  Deck(uint32_t w_payment, uint32_t w_order_status, uint32_t w_delivery, uint32_t w_stock_level) {
    TERRIER_ASSERT(w_payment >= 43, "At least 43% payment.");
    TERRIER_ASSERT(w_order_status >= 4, "At least 4% order status.");
    TERRIER_ASSERT(w_delivery >= 4, "At least 4% delivery.");
    TERRIER_ASSERT(w_payment >= 4, "At least 4% stock level.");
    TERRIER_ASSERT(w_payment + w_order_status + w_delivery + w_stock_level <= 100, "Weights cannot be more than 100.");

    auto min_payment = static_cast<uint32_t>(std::ceil(static_cast<double>(w_payment) / 100.0 * 23));
    auto min_order_status = static_cast<uint32_t>(std::ceil(static_cast<double>(w_order_status) / 100.0 * 23));
    auto min_delivery = static_cast<uint32_t>(std::ceil(static_cast<double>(w_delivery) / 100.0 * 23));
    auto min_stock_level = static_cast<uint32_t>(std::ceil(static_cast<double>(w_stock_level) / 100.0 * 23));

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
    TERRIER_ASSERT(static_cast<double>(c_payment) / 23.0 * 100 >= w_payment, "Payment weight unsatisfied.");
    TERRIER_ASSERT(static_cast<double>(c_order_status) / 23.0 * 100 >= w_order_status,
                   "Order status weight unsatisfied.");
    TERRIER_ASSERT(static_cast<double>(c_delivery) / 23.0 * 100 >= w_delivery, "Delivery weight unsatisfied.");
    TERRIER_ASSERT(static_cast<double>(c_stock_level) / 23.0 * 100 >= w_stock_level, "Stock level weight unsatisfied.");

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

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCCBenchmark, Basic)(benchmark::State &state) {
  // one TPCC worker = one TPCC terminal = one thread
  std::vector<tpcc::Worker> workers;
  workers.reserve(num_threads_);

  thread_pool_.Shutdown();
  thread_pool_.SetNumWorkers(num_threads_);
  thread_pool_.Startup();

  // we need transactions, TPCC database, and GC
  transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
  auto tpcc_builder = tpcc::Builder(&block_store_);

  // random number generation is slow, so we precompute the args
  std::vector<std::vector<tpcc::TransactionArgs>> precomputed_args;
  precomputed_args.reserve(workers.size());

  Deck deck(w_payment, w_order_status, w_delivery, w_stock_level);

  for (uint32_t warehouse_id = 1; warehouse_id <= num_threads_; warehouse_id++) {
    std::vector<tpcc::TransactionArgs> txns;
    txns.reserve(num_precomputed_txns_per_worker_);
    for (uint32_t i = 0; i < num_precomputed_txns_per_worker_; i++) {
      switch (deck.NextCard()) {
        case tpcc::TransactionType::NewOrder:
          txns.emplace_back(tpcc::BuildNewOrderArgs(&generator_, warehouse_id, num_threads_));
          break;
        case tpcc::TransactionType::Payment:
          txns.emplace_back(tpcc::BuildPaymentArgs(&generator_, warehouse_id, num_threads_));
          break;
        case tpcc::TransactionType::OrderStatus:
          txns.emplace_back(tpcc::BuildOrderStatusArgs(&generator_, warehouse_id, num_threads_));
          break;
        case tpcc::TransactionType::Delivery:
          txns.emplace_back(tpcc::BuildDeliveryArgs(&generator_, warehouse_id, num_threads_));
          break;
        case tpcc::TransactionType::StockLevel:
          txns.emplace_back(tpcc::BuildStockLevelArgs(&generator_, warehouse_id, num_threads_));
          break;
        default:
          throw std::runtime_error("Unexpected transaction type.");
      }
    }
    precomputed_args.emplace_back(txns);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    // build the TPCC database
    //    log_manager_ = new storage::LogManager(LOG_FILE_NAME, &buffer_pool_);
    auto *const tpcc_db = tpcc_builder.Build();

    // prepare the workers
    workers.clear();
    for (uint32_t i = 0; i < num_threads_; i++) {
      workers.emplace_back(tpcc_db);
    }

    tpcc::Loader::PopulateDatabase(&txn_manager, &generator_, tpcc_db, workers);
    //    log_manager_->Process();  // log all of the Inserts from table creation
    StartGC(&txn_manager);
    //    StartLogging();

    // define the TPCC workload
    auto tpcc_workload = [&](uint32_t worker_id) {
      auto new_order = tpcc::NewOrder(tpcc_db);
      auto payment = tpcc::Payment(tpcc_db);
      auto order_status = tpcc::OrderStatus(tpcc_db);
      auto delivery = tpcc::Delivery(tpcc_db);
      auto stock_level = tpcc::StockLevel(tpcc_db);

      for (uint32_t i = 0; i < num_precomputed_txns_per_worker_; i++) {
        const auto &txn_args = precomputed_args[worker_id][i];
        switch (txn_args.type) {
          case tpcc::TransactionType::NewOrder: {
            auto new_order_committed =
                new_order.Execute(&txn_manager, &generator_, tpcc_db, &workers[worker_id], txn_args);
            workers[worker_id].num_committed_txns += new_order_committed ? 1 : 0;
            break;
          }
          case tpcc::TransactionType::Payment: {
            auto payment_committed = payment.Execute(&txn_manager, &generator_, tpcc_db, &workers[worker_id], txn_args);
            workers[worker_id].num_committed_txns += payment_committed ? 1 : 0;
            break;
          }
          case tpcc::TransactionType::OrderStatus: {
            auto order_status_committed =
                order_status.Execute(&txn_manager, &generator_, tpcc_db, &workers[worker_id], txn_args);
            workers[worker_id].num_committed_txns += order_status_committed ? 1 : 0;
            break;
          }
          case tpcc::TransactionType::Delivery: {
            auto delivery_committed =
                delivery.Execute(&txn_manager, &generator_, tpcc_db, &workers[worker_id], txn_args);
            workers[worker_id].num_committed_txns += delivery_committed ? 1 : 0;
            break;
          }
          case tpcc::TransactionType::StockLevel: {
            auto stock_level_committed =
                stock_level.Execute(&txn_manager, &generator_, tpcc_db, &workers[worker_id], txn_args);
            workers[worker_id].num_committed_txns += stock_level_committed ? 1 : 0;
            break;
          }
          default:
            throw std::runtime_error("Unexpected transaction type.");
        }
      }
    };

    // run the TPCC workload to completion
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      for (uint32_t i = 0; i < num_threads_; i++) {
        thread_pool_.SubmitTask([i, &tpcc_workload] { tpcc_workload(i); });
      }
      thread_pool_.WaitUntilAllFinished();
    }

    // figure out how many transactions committed
    uint64_t num_items_processed = state.items_processed();
    for (uint64_t i = 0; i < num_threads_; i++) {
      num_items_processed += workers[i].num_committed_txns;
    }

    // update benchmark state
    state.SetItemsProcessed(num_items_processed);
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    // cleanup
    //    EndLogging();
    EndGC();
    delete tpcc_db;
    //    delete log_manager_;
  }

  // Clean up the buffers from any non-inlined VarlenEntrys in the precomputed args
  for (const auto &worker_id : precomputed_args) {
    for (const auto &args : worker_id) {
      if ((args.type == tpcc::TransactionType::Payment || args.type == tpcc::TransactionType::OrderStatus) &&
          args.use_c_last && !args.c_last.IsInlined()) {
        delete[] args.c_last.Content();
      }
    }
  }
}

BENCHMARK_REGISTER_F(TPCCBenchmark, Basic)->Unit(benchmark::kMillisecond)->UseManualTime();
}  // namespace terrier
