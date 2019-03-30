#include "common/object_pool.h"
#include "storage/storage_util.h"
#include "storage/data_table.h"
#include "storage/checkpoint_manager.h"
#include "util/storage_test_util.h"
#include "util/transaction_test_util.h"


#define CHECKPOINT_FILE_PREFIX "checkpoint_"

namespace terrier {

struct CheckpointTests : public TerrierTest {
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{100000, 10000};
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
};

TEST_F(CheckpointTests, SimpleCheckpointNoVarlen) {
  // const uint32_t num_inserts = 1000;
  const uint32_t max_columns = 10;
  const uint32_t initial_table_size = 100;
  const uint32_t checkpoint_buffer_size = 10000;

  LargeTransactionTestObject tested = LargeTransactionTestObject::Builder()
                                        .SetMaxColumns(max_columns)
                                        .SetInitialTableSize(initial_table_size)
                                        // .SetTxnLength(txn_length)
                                        // .SetUpdateSelectRatio(update_select_ratio)
                                        .SetBlockStore(&block_store_)
                                        .SetBufferPool(&buffer_pool_)
                                        .SetGenerator(&generator_)
                                        .SetGcOn(false)
                                        .SetBookkeeping(true)
                                        .build();

  storage::CheckpointManager manager(CHECKPOINT_FILE_PREFIX, checkpoint_buffer_size);
  storage::DataTable *table = tested.GetTable();
  transaction::TransactionManager *txn_manager = tested.GetTxnManager();

  transaction::TransactionContext *txn = txn_manager->BeginTransaction();
  manager.StartCheckpoint(txn);
  manager.Checkpoint(*table, tested.Layout());
  manager.EndCheckpoint();


}

}  // namespace terrier