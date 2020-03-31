

#include "storage/checkpoints/checkpoint.h"
#include <common/worker_pool.h>
#include <fstream>
namespace terrier::storage{

bool Checkpoint::TakeCheckpoint(const std::string &path, catalog::db_oid_t db) {
  // TODO (Xuanxuan): fake code for build purpose, change later
  auto accessor = catalog_->GetAccessor(txn_, db);
  std::unordered_set<catalog::table_oid_t> table_oids = accessor->GetAllTableOids();

  for (const auto &oid : table_oids) {
    queue.push_back(oid);
  }


  // initalize threads
  auto num_threads = 1u;
  common::WorkerPool thread_pool_{num_threads, {}};
  thread_pool_.Startup();
  auto workload = [&](uint32_t worker_id){
    // copy contents of table to disk
    WriteToDisk(path, accessor, db);

  };
  for (auto i = 0u; i < num_threads; i++){
    thread_pool_.SubmitTask([i, &workload]{workload(i);});
  }
  thread_pool_.WaitUntilAllFinished();
  if (queue.size() > 0){
    // the table oid that failed to be backup
    return false;
  }
  return true;
}

void Checkpoint::WriteToDisk(const std::string &path, const std::unique_ptr<catalog::CatalogAccessor> &accessor,
    catalog::db_oid_t db_oid) {
  while (queue.size() > 0){
    queue_latch.lock();
    if (queue.size() <= 0){
      queue_latch.unlock();
      return;
    }
    catalog::table_oid_t curr_table_oid = queue[0];
    queue.erase(queue.begin());
    queue_latch.unlock();
    common::ManagedPointer<storage::SqlTable> curr_table = accessor->GetTable(curr_table_oid);
    std::string out_file = GenFileName(db_oid, curr_table_oid);
    std::ofstream f;
    f.open(path + out_file, std::ios::binary);

    // write block contents (in bytes) to the file
    if (f.is_open()){

      for (auto &block : curr_table->table_.data_table_->blocks_){
        f.write(reinterpret_cast<const char *>(block->content_), sizeof(block->content_));
      }
      f.close();

    } else {
      // failed to copy to disk, add the table_oid back to queue
      queue_latch.lock();
      queue.push_back(curr_table_oid);
      queue_latch.unlock();
      return;
    }

  }
}

} //namespace terrier::storage
