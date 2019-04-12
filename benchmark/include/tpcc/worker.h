#pragma once

#include "common/allocator.h"
#include "tpcc/database.h"

namespace terrier::tpcc {

struct Worker {
  explicit Worker(tpcc::Database *const db)
      : item_tuple_buffer(common::AllocationUtil::AllocateAligned(
            db->item_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->item_schema_))
                .first.ProjectedRowSize())),
        warehouse_tuple_buffer(common::AllocationUtil::AllocateAligned(
            db->warehouse_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->warehouse_schema_))
                .first.ProjectedRowSize())),
        stock_tuple_buffer(common::AllocationUtil::AllocateAligned(
            db->stock_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->stock_schema_))
                .first.ProjectedRowSize())),
        district_tuple_buffer(common::AllocationUtil::AllocateAligned(
            db->district_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->district_schema_))
                .first.ProjectedRowSize())),
        customer_tuple_buffer(common::AllocationUtil::AllocateAligned(
            db->customer_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->customer_schema_))
                .first.ProjectedRowSize())),
        history_tuple_buffer(common::AllocationUtil::AllocateAligned(
            db->history_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->history_schema_))
                .first.ProjectedRowSize())),
        order_tuple_buffer(common::AllocationUtil::AllocateAligned(
            db->order_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->order_schema_))
                .first.ProjectedRowSize())),
        new_order_tuple_buffer(common::AllocationUtil::AllocateAligned(
            db->new_order_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->new_order_schema_))
                .first.ProjectedRowSize())),
        order_line_tuple_buffer(common::AllocationUtil::AllocateAligned(
            db->order_line_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->order_line_schema_))
                .first.ProjectedRowSize())),
        item_key_buffer(
            common::AllocationUtil::AllocateAligned(db->item_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        warehouse_key_buffer(common::AllocationUtil::AllocateAligned(
            db->warehouse_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        stock_key_buffer(
            common::AllocationUtil::AllocateAligned(db->stock_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        district_key_buffer(common::AllocationUtil::AllocateAligned(
            db->district_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        customer_key_buffer(common::AllocationUtil::AllocateAligned(
            db->customer_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        customer_name_key_buffer(common::AllocationUtil::AllocateAligned(
            db->customer_name_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        customer_name_varlen_buffer(common::AllocationUtil::AllocateAligned(16)),
        order_key_buffer(
            common::AllocationUtil::AllocateAligned(db->order_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        new_order_key_buffer(common::AllocationUtil::AllocateAligned(
            db->new_order_index_->GetProjectedRowInitializer().ProjectedRowSize())),
        order_line_key_buffer(common::AllocationUtil::AllocateAligned(
            db->order_line_index_->GetProjectedRowInitializer().ProjectedRowSize())) {}

  ~Worker() {
    delete[] item_tuple_buffer;
    delete[] warehouse_tuple_buffer;
    delete[] stock_tuple_buffer;
    delete[] district_tuple_buffer;
    delete[] customer_tuple_buffer;
    delete[] history_tuple_buffer;
    delete[] order_tuple_buffer;
    delete[] new_order_tuple_buffer;
    delete[] order_line_tuple_buffer;

    delete[] item_key_buffer;
    delete[] warehouse_key_buffer;
    delete[] stock_key_buffer;
    delete[] district_key_buffer;
    delete[] customer_key_buffer;
    delete[] customer_name_key_buffer;
    delete[] customer_name_varlen_buffer;
    delete[] order_key_buffer;
    delete[] new_order_key_buffer;
    delete[] order_line_key_buffer;
  }

  byte *const item_tuple_buffer;
  byte *const warehouse_tuple_buffer;
  byte *const stock_tuple_buffer;
  byte *const district_tuple_buffer;
  byte *const customer_tuple_buffer;
  byte *const history_tuple_buffer;
  byte *const order_tuple_buffer;
  byte *const new_order_tuple_buffer;
  byte *const order_line_tuple_buffer;

  byte *const item_key_buffer;
  byte *const warehouse_key_buffer;
  byte *const stock_key_buffer;
  byte *const district_key_buffer;
  byte *const customer_key_buffer;
  byte *const customer_name_key_buffer;
  byte *const customer_name_varlen_buffer;
  byte *const order_key_buffer;
  byte *const new_order_key_buffer;
  byte *const order_line_key_buffer;

  uint32_t num_committed_txns = 0;
};
}  // namespace terrier::tpcc
