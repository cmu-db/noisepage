#include "util/storage_test_util.h"
#include "storage/data_table.h"
#include "storage/log_manager.h"
#include "transaction/transaction_manager.h"
#include "execution/sql_table.h"

namespace terrier {
// NOLINTNEXTLINE
TEST(LogTest, SimpleLogTest) {
  std::default_random_engine engine;
  storage::RecordBufferSegmentPool pool(100, 100);
  storage::BlockStore block_store(100, 100);
  storage::LogManager log_manager("test.txt", &pool);
  storage::BlockLayout layout = StorageTestUtil::RandomLayout(2, &engine);
  storage::DataTable table(&block_store, layout, layout_version_t(0));
  transaction::TransactionManager txn_manager(&pool, false, &log_manager);
  transaction::TransactionContext *txn = txn_manager.BeginTransaction();
  execution::SqlTable fake_table;

  const storage::ProjectedRowInitializer initializer(layout, StorageTestUtil::ProjectionListAllColumns(layout));

  auto redo = txn->StageWrite(&fake_table, tuple_id_t(0), initializer);
  StorageTestUtil::PopulateRandomRow(redo->Delta(), layout, 0, &engine);
  table.Insert(txn, *redo->Delta());
  txn_manager.Commit(txn);
  log_manager.Process();
  log_manager.Flush();
}
}  // namespace terrier