#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/managed_pointer.h"
#include "network/postgres/statement.h"
#include "xxHash/xxh3.h"

namespace noisepage::network {

/**
 * Simple statement cache. It contains a map from query string to Statement objects, allowing for reuse of bound parser
 * result, physical plan, and codegen'd executable query if appropriate. Can be extended in the future to have a maximum
 * size, replacement policy, etc.
 */
class StatementCache {
 public:
  /**
   * Check if a Statement for a query string exists
   * @param query_text key to look up
   * @return pointer to Statement object if it already exists, nullptr otherwise
   */
  common::ManagedPointer<Statement> Lookup(const std::string &query_text) const {
    const auto it = cache_.find(query_text);
    if (it != cache_.end()) return common::ManagedPointer(it->second);
    return nullptr;
  }

  /**
   * Transfer ownership of a Statement to the cache
   * @param statement object to take ownership of
   */
  void Add(std::unique_ptr<network::Statement> &&statement) {
    cache_[statement->GetQueryText()] = std::move(statement);
  }

 private:
  /**
   * We'll use xxHash for the keys since it's a fast hash algorithm for strings.
   */
  struct FastStringHasher {
    std::size_t operator()(const std::string &key) const { return XXH3_64bits(key.data(), key.length()); }
  };

  std::unordered_map<std::string, std::unique_ptr<Statement>, FastStringHasher> cache_;
};

}  // namespace noisepage::network
