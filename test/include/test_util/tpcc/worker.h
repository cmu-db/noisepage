#pragma once

#include "common/allocator.h"
#include "test_util/tpcc/database.h"

namespace noisepage::tpcc {

/**
 * Worker contains buffers for each worker thread (running txns) to use for PRs for tables and indexes to avoid
 * constantly allocating and freeing them within the transactions themselves. It also contains a random number generator
 * for the parallel loader, since I don't believe that STL class is safe for concurrent access.
 */
struct Worker {
  explicit Worker(tpcc::Database *const db)
      : item_tuple_buffer_(common::AllocationUtil::AllocateAligned(
            db->item_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->item_schema_))
                .ProjectedRowSize())),
        warehouse_tuple_buffer_(common::AllocationUtil::AllocateAligned(
            db->warehouse_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->warehouse_schema_))
                .ProjectedRowSize())),
        stock_tuple_buffer_(common::AllocationUtil::AllocateAligned(
            db->stock_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->stock_schema_))
                .ProjectedRowSize())),
        district_tuple_buffer_(common::AllocationUtil::AllocateAligned(
            db->district_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->district_schema_))
                .ProjectedRowSize())),
        customer_tuple_buffer_(common::AllocationUtil::AllocateAligned(
            db->customer_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->customer_schema_))
                .ProjectedRowSize())),
        history_tuple_buffer_(common::AllocationUtil::AllocateAligned(
            db->history_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->history_schema_))
                .ProjectedRowSize())),
        order_tuple_buffer_(common::AllocationUtil::AllocateAligned(
            db->order_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->order_schema_))
                .ProjectedRowSize())),
        new_order_tuple_buffer_(common::AllocationUtil::AllocateAligned(
            db->new_order_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->new_order_schema_))
                .ProjectedRowSize())),
        order_line_tuple_buffer_(common::AllocationUtil::AllocateAligned(
            db->order_line_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->order_line_schema_))
                .ProjectedRowSize())),
        item_key_buffer_(common::AllocationUtil::AllocateAligned(
            db->item_primary_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        warehouse_key_buffer_(common::AllocationUtil::AllocateAligned(
            db->warehouse_primary_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        stock_key_buffer_(common::AllocationUtil::AllocateAligned(
            db->stock_primary_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        district_key_buffer_(common::AllocationUtil::AllocateAligned(
            db->district_primary_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        customer_key_buffer_(common::AllocationUtil::AllocateAligned(
            db->customer_primary_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        customer_name_key_buffer_(common::AllocationUtil::AllocateAligned(
            db->customer_secondary_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        customer_name_varlen_buffer_(common::AllocationUtil::AllocateAligned(16)),
        order_key_buffer_(common::AllocationUtil::AllocateAligned(
            db->order_primary_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        order_secondary_key_buffer_(common::AllocationUtil::AllocateAligned(
            db->order_secondary_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        new_order_key_buffer_(common::AllocationUtil::AllocateAligned(
            db->new_order_primary_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        order_line_key_buffer_(common::AllocationUtil::AllocateAligned(
            db->order_line_primary_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        generator_(new std::default_random_engine) {}

  ~Worker() {
    delete[] item_tuple_buffer_;
    delete[] warehouse_tuple_buffer_;
    delete[] stock_tuple_buffer_;
    delete[] district_tuple_buffer_;
    delete[] customer_tuple_buffer_;
    delete[] history_tuple_buffer_;
    delete[] order_tuple_buffer_;
    delete[] new_order_tuple_buffer_;
    delete[] order_line_tuple_buffer_;

    delete[] item_key_buffer_;
    delete[] warehouse_key_buffer_;
    delete[] stock_key_buffer_;
    delete[] district_key_buffer_;
    delete[] customer_key_buffer_;
    delete[] customer_name_key_buffer_;
    delete[] customer_name_varlen_buffer_;
    delete[] order_key_buffer_;
    delete[] order_secondary_key_buffer_;
    delete[] new_order_key_buffer_;
    delete[] order_line_key_buffer_;

    delete generator_;
  }

  byte *const item_tuple_buffer_;
  byte *const warehouse_tuple_buffer_;
  byte *const stock_tuple_buffer_;
  byte *const district_tuple_buffer_;
  byte *const customer_tuple_buffer_;
  byte *const history_tuple_buffer_;
  byte *const order_tuple_buffer_;
  byte *const new_order_tuple_buffer_;
  byte *const order_line_tuple_buffer_;

  byte *const item_key_buffer_;
  byte *const warehouse_key_buffer_;
  byte *const stock_key_buffer_;
  byte *const district_key_buffer_;
  byte *const customer_key_buffer_;
  byte *const customer_name_key_buffer_;
  byte *const customer_name_varlen_buffer_;
  byte *const order_key_buffer_;
  byte *const order_secondary_key_buffer_;
  byte *const new_order_key_buffer_;
  byte *const order_line_key_buffer_;

  std::default_random_engine *const generator_;
};
}  // namespace noisepage::tpcc
