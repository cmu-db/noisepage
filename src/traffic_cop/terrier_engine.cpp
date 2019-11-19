#include "traffic_cop/terrier_engine.h"

#include <memory>
#include <string>
#include <utility>

#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "parser/postgresparser.h"
#include "transaction/transaction_manager.h"

namespace terrier::trafficcop {

void TerrierEngine::ParseAndBind(catalog::db_oid_t db_oid, const std::string &query) {
  parser::ParseResult parse_result = parser_->BuildParseTree(query);
  transaction::TransactionContext *txn = txn_manager_->BeginTransaction();
  std::unique_ptr<catalog::CatalogAccessor> accessor = catalog_->GetAccessor(txn, db_oid);
  binder::BindNodeVisitor binder{std::move(accessor), catalog::DEFAULT_DATABASE};

  for (common::ManagedPointer<parser::SQLStatement> stmt : parse_result.GetStatements()) {
    // TODO(WAN): binder should probably be taking managed pointers to parse_result...
    binder.BindNameToNode(stmt, &parse_result);
  }
}

}  // namespace terrier::trafficcop
