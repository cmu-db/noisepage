#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "common/managed_pointer.h"
#include "network/postgres/statement.h"
#include "xxHash/xxh3.h"

namespace terrier::network {

class Statement;

class StatementCache {
 public:
  common::ManagedPointer<Statement> Lookup(const std::string &query_text) const;

  void Add(std::unique_ptr<network::Statement> &&statement) {
    cache_[statement->GetQueryText()] = std::move(statement);
  }

 private:
  struct FastStringHasher {
    std::size_t operator()(const std::string &key) const { return XXH3_64bits(key.data(), key.length()); }
  };

  std::unordered_map<std::string, std::unique_ptr<Statement>, FastStringHasher> cache_;
};

}  // namespace terrier::network