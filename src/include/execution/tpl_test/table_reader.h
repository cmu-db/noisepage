#include <iostream>
#include <memory>
#include <fstream>
#include "type/type_id.h"
#include "catalog/schema.h"
#include "catalog/catalog.h"
#include "transaction/transaction_context.h"


namespace tpl::reader {
using namespace terrier;

class TableReader {
  TableReader(catalog::Catalog * catalog, transaction::TransactionContext txn);

  WriteTableData(storage::ProjectedRow * insert_pr, uint16_t col_idx, );
};
}