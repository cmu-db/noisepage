#pragma once

#include <algorithm>
#include <map>
#include <unordered_set>

#include "catalog/index_schema.h"

namespace noisepage::parser {
class AbstractExpression;
}

namespace noisepage::storage::index {

class Index;
class IndexMetadata;

class IndexOptions {
 public:
  enum Value {
    BUILD_THREADS,
    BPLUSTREE_INNER_NODE_UPPER_THRESHOLD,
    BPLUSTREE_INNER_NODE_LOWER_THRESHOLD,

    UNKNOWN
  };

  static IndexOptions::Value ConvertToOptionValue(std::string option) {
    std::transform(option.begin(), option.end(), option.begin(), ::toupper);
    if (option == "BUILD_THREADS") {
      return BUILD_THREADS;
    } else if (option == "BPLUSTREE_INNER_NODE_UPPER_THRESHOLD") {
      return BPLUSTREE_INNER_NODE_UPPER_THRESHOLD;
    } else if (option == "BPLUSTREE_INNER_NODE_LOWER_THRESHOLD") {
      return BPLUSTREE_INNER_NODE_LOWER_THRESHOLD;
    } else {
      return UNKNOWN;
    }
  }

  static type::TypeId ExpectedTypeForOption(Value val) {
    switch (val) {
      case BUILD_THREADS:
        return type::TypeId::INTEGER;
      case BPLUSTREE_INNER_NODE_UPPER_THRESHOLD:
        return type::TypeId::INTEGER;
      case BPLUSTREE_INNER_NODE_LOWER_THRESHOLD:
        return type::TypeId::INTEGER;
      case UNKNOWN:
      default:
        return type::TypeId::INVALID;
    }
  }

  void AddOption(Value option, std::unique_ptr<parser::AbstractExpression> value) {
    NOISEPAGE_ASSERT(options_.find(option) == options_.end(), "Adding duplicate option to INDEX options");
    options_[option] = std::move(value);
  }

  IndexOptions() = default;

  IndexOptions(const IndexOptions &other) {
    for (const auto &pair : other.options_) {
      AddOption(pair.first, pair.second->Copy());
    }
  }

  IndexOptions &operator=(const IndexOptions &other) = delete;
  IndexOptions &operator=(IndexOptions &&other) {
    options_ = std::move(other.options_);
    return *this;
  }

  bool operator==(const IndexOptions &other) const {
    std::unordered_set<Value> currentOptions;
    std::unordered_set<Value> otherOptions;
    for (const auto &pair : options_) currentOptions.insert(pair.first);
    for (const auto &pair : other.options_) otherOptions.insert(pair.first);
    if (currentOptions != otherOptions) return false;

    for (const auto &pair : options_) {
      auto it = other.options_.find(pair.first);
      if ((*pair.second) != (*it->second)) return false;
    }

    return true;
  }

  bool operator!=(const IndexOptions &other) const { return !(*this == other); }

  common::hash_t Hash() const {
    common::hash_t hash = 0;
    for (const auto &pair : options_) {
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(pair.first));
      hash = common::HashUtil::CombineHashes(hash, pair.second->Hash());
    }
    return hash;
  }

  const std::map<Value, std::unique_ptr<parser::AbstractExpression>> &GetOptions() const { return options_; }

 private:
  std::map<Value, std::unique_ptr<parser::AbstractExpression>> options_;
};

/**
 * The IndexBuilder automatically creates the best possible index for the given parameters.
 */
class IndexBuilder {
 private:
  catalog::IndexSchema key_schema_;
  IndexOptions index_options_;

 public:
  IndexBuilder() = default;

  /**
   * @return a new best-possible index for the current parameters, nullptr if it failed to construct a valid index
   */
  Index *Build() const;

  /**
   * @param key_schema the index key schema
   * @return the builder object
   */
  IndexBuilder &SetKeySchema(const catalog::IndexSchema &key_schema);

  IndexBuilder &SetIndexOptions(const IndexOptions &index_options);

 private:
  template <storage::index::IndexType type, class Key>
  void ApplyIndexOptions(Index *index) const;

  Index *BuildBwTreeIntsKey(IndexMetadata metadata) const;

  Index *BuildBwTreeGenericKey(IndexMetadata metadata) const;

  Index *BuildBPlusTreeIntsKey(IndexMetadata &&metadata) const;

  Index *BuildBPlusTreeGenericKey(IndexMetadata metadata) const;

  Index *BuildHashIntsKey(IndexMetadata metadata) const;

  Index *BuildHashGenericKey(IndexMetadata metadata) const;
};

}  // namespace noisepage::storage::index
