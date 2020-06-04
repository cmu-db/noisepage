#include "network/postgres/statement_cache.h"

namespace terrier::network {

common::ManagedPointer<Statement> StatementCache::Lookup(const std::string &query_text) const {
  const auto it = cache_.find(query_text);
  if (it != cache_.end()) return common::ManagedPointer(it->second);
  return nullptr;
}

}  // namespace terrier::network