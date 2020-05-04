#include "storage/checkpoints/checkpoint.h"
#include <common/worker_pool.h>
#include <storage/block_compactor.h>
#include <fstream>
namespace terrier::storage {

bool Checkpoint::TakeCheckpoint(const std::string &path, catalog::db_oid_t db, const char *cur_log_file, uint32_t num_threads,
                    common::WorkerPool *thread_pool_) {
  // get db catalog accessor
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(static_cast<common::ManagedPointer<transaction::TransactionContext>>(txn), db);
  std::unordered_set<catalog::table_oid_t> table_oids = accessor->GetAllTableOids();

  // lock the log_serializer buffer to prevent further logging
  common::ManagedPointer<LogSerializerTask> log_serializer_task = log_manager_->log_serializer_task_;
  log_serializer_task->flush_queue_latch_.Lock();

  // copy all data tables to serialize later
  for (const auto &oid : table_oids) {
    // copy data table
    common::ManagedPointer<storage::SqlTable> curr_sql_table = accessor->GetTable(oid);
    storage::DataTable *curr_data_table = curr_sql_table->table_.data_table_;
    std::list<RawBlock *> new_blocks(curr_data_table->blocks_);
    storage::DataTable *new_table = new storage::DataTable(
        curr_data_table->block_store_, curr_data_table->GetBlockLayout(), curr_data_table->layout_version_);
    new_table->blocks_ = new_blocks;
    queue.emplace_back(oid, new_table);
  }
  // delete previous log file (uncomment after separate catalog log from other logs)
  //  if (remove(cur_log_file) != 0)
  //    perror( "Error deleting file" );
  //  else
  //    puts( "File successfully deleted" );

  log_serializer_task->flush_queue_latch_.Unlock();

  thread_pool_->Startup();
  auto workload = [&](uint32_t worker_id) {
    // copy contents of table to disk
    WriteToDisk(path, accessor, db);
  };
  for (auto i = 0u; i < num_threads; i++) {
    thread_pool_->SubmitTask([i, &workload] { workload(i); });
  }
  thread_pool_->WaitUntilAllFinished();
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  if (queue.size() > 0) {
    // the table oid that failed to be backup
    return false;
  }
  return true;
}

void Checkpoint::WriteToDisk(const std::string &path, const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                             catalog::db_oid_t db_oid) {
  while (queue.size() > 0) {
    queue_latch.lock();
    if (queue.size() <= 0) {
      queue_latch.unlock();
      return;
    }
    catalog::table_oid_t curr_table_oid = queue[0].first;
    storage::DataTable *data_table = queue[0].second;
    queue.erase(queue.begin());
    queue_latch.unlock();

    // get daga table
    std::string out_file = path + GenFileName(db_oid, curr_table_oid);

    const BlockLayout &layout = data_table->GetBlockLayout();
    std::vector<type::TypeId> column_types;
    column_types.resize(layout.NumColumns());
    for (RawBlock *block : data_table->blocks_) {
      auto &arrow_metadata = data_table->accessor_.GetArrowBlockMetadata(block);
      for (storage::col_id_t col_id : layout.AllColumns()) {
        if (layout.IsVarlen(col_id)) {
          arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::GATHERED_VARLEN;
          column_types[!col_id] = type::TypeId::VARCHAR;
        } else {
          arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
          column_types[!col_id] = type::TypeId::INTEGER;
        }
      }
    }

    // compact blocks into arrow format
    storage::BlockCompactor compactor;
    for (RawBlock *block : data_table->blocks_) {
      compactor.PutInQueue(block);
      compactor.ProcessCompactionQueue(deferred_action_manager_.Get(), txn_manager_.Get());  // compaction pass
      gc_->PerformGarbageCollection();
      compactor.PutInQueue(block);
      compactor.ProcessCompactionQueue(deferred_action_manager_.Get(), txn_manager_.Get());  // gathering pass
    }

    // write to disk
    storage::ArrowSerializer arrow_serializer(*data_table);
    arrow_serializer.ExportTable(out_file, &column_types, false);
  }
}

}  // namespace terrier::storage
