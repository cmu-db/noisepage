#pragma once

#include <utility>
#include "common/macros.h"

namespace terrier::storage {
class SqlTable;
namespace index {
class Index;
}
}  // namespace terrier::storage

namespace terrier::catalog {
class Schema;
}

// TODO(Matt): it seems many fields can by smaller than INTEGER

namespace terrier::tpcc {

class Database {
 public:
  ~Database() {
    delete item_table_;
    delete warehouse_table_;
    delete stock_table_;
    delete district_table_;
    delete customer_table_;
    delete history_table_;
    delete new_order_table_;
    delete order_table_;
    delete order_line_table_;
  }

  const catalog::Schema item_schema_;
  const catalog::Schema warehouse_schema_;
  const catalog::Schema stock_schema_;
  const catalog::Schema district_schema_;
  const catalog::Schema customer_schema_;
  const catalog::Schema history_schema_;
  const catalog::Schema new_order_schema_;
  const catalog::Schema order_schema_;
  const catalog::Schema order_line_schema_;

  storage::SqlTable *const item_table_;
  storage::SqlTable *const warehouse_table_;
  storage::SqlTable *const stock_table_;
  storage::SqlTable *const district_table_;
  storage::SqlTable *const customer_table_;
  storage::SqlTable *const history_table_;
  storage::SqlTable *const new_order_table_;
  storage::SqlTable *const order_table_;
  storage::SqlTable *const order_line_table_;

  storage::index::Index *const item_index_;
  storage::index::Index *const warehouse_index_;
  storage::index::Index *const stock_index_;
  storage::index::Index *const district_index_;
  storage::index::Index *const customer_index_;
  storage::index::Index *const new_order_index_;
  storage::index::Index *const order_index_;
  storage::index::Index *const order_line_index_;

 private:
  friend class Builder;

  Database(catalog::Schema item_schema, catalog::Schema warehouse_schema, catalog::Schema stock_schema,
           catalog::Schema district_schema, catalog::Schema customer_schema, catalog::Schema history_schema,
           catalog::Schema new_order_schema, catalog::Schema order_schema, catalog::Schema order_line_schema,
           storage::SqlTable *const item, storage::SqlTable *const warehouse, storage::SqlTable *const stock,
           storage::SqlTable *const district, storage::SqlTable *const customer, storage::SqlTable *const history,
           storage::SqlTable *const new_order, storage::SqlTable *const order, storage::SqlTable *const order_line)
      : item_schema_(std::move(item_schema)),
        warehouse_schema_(std::move(warehouse_schema)),
        stock_schema_(std::move(stock_schema)),
        district_schema_(std::move(district_schema)),
        customer_schema_(std::move(customer_schema)),
        history_schema_(std::move(history_schema)),
        new_order_schema_(std::move(new_order_schema)),
        order_schema_(std::move(order_schema)),
        order_line_schema_(std::move(order_line_schema)),
        item_table_(item),
        warehouse_table_(warehouse),
        stock_table_(stock),
        district_table_(district),
        customer_table_(customer),
        history_table_(history),
        new_order_table_(new_order),
        order_table_(order),
        order_line_table_(order_line),
        item_index_(nullptr),
        warehouse_index_(nullptr),
        stock_index_(nullptr),
        district_index_(nullptr),
        customer_index_(nullptr),
        new_order_index_(nullptr),
        order_index_(nullptr),
        order_line_index_(nullptr) {}
};

}  // namespace terrier::tpcc
