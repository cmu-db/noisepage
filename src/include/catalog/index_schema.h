#pragma once

#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/json_header.h"
#include "common/macros.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/column_value_expression.h"
#include "storage/index/index_defs.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace noisepage {
class StorageTestUtil;
class TpccPlanTest;
}  // namespace noisepage

namespace noisepage::storage {
class RecoveryManager;
}

namespace noisepage::tpcc {
class Schemas;
}

namespace noisepage::catalog {
class DatabaseCatalog;

namespace postgres {
class Builder;
}

/**
 * A schema for an index.  It contains the definitions for the columns in the
 * key as well as additional metdata.
 */
class IndexSchema {
 public:
  /**
   * A column of the index key has an identifier, type, and describes whether it can be null.
   */
  class Column {
   public:
    /**
     * Non-varlen constructor for index key columns.
     * @param name column name (column name or "expr")
     * @param type_id the non-varlen type of the column
     * @param nullable whether the column is nullable
     * @param definition definition of this attribute
     */
    Column(std::string name, type::TypeId type_id, bool nullable, const parser::AbstractExpression &definition)
        : name_(std::move(name)), oid_(INVALID_INDEXKEYCOL_OID), packed_type_(0), definition_(definition.Copy()) {
      NOISEPAGE_ASSERT(!(type_id == type::TypeId::VARCHAR || type_id == type::TypeId::VARBINARY),
                       "Non-varlen constructor.");
      SetTypeId(type_id);
      SetNullable(nullable);
    }

    /**
     * Varlen constructor for index key columns.
     * @param name column name (column name or "expr")
     * @param type_id the varlen type of the column
     * @param max_varlen_size the maximum varlen size
     * @param nullable whether the column is nullable
     * @param definition definition of this attribute
     */
    Column(std::string name, type::TypeId type_id, uint16_t max_varlen_size, bool nullable,
           const parser::AbstractExpression &definition)
        : name_(std::move(name)), oid_(INVALID_INDEXKEYCOL_OID), packed_type_(0), definition_(definition.Copy()) {
      NOISEPAGE_ASSERT(type_id == type::TypeId::VARCHAR || type_id == type::TypeId::VARBINARY, "Varlen constructor.");
      SetTypeId(type_id);
      SetNullable(nullable);
      SetMaxVarlenSize(max_varlen_size);
    }

    /**
     * Overrides default copy constructor to ensure we do a deep copy on the abstract expressions
     * @param old_column to be copied
     */
    Column(const Column &old_column)
        : name_(old_column.name_),
          oid_(old_column.oid_),
          packed_type_(old_column.packed_type_),
          definition_(old_column.definition_->Copy()) {}

    /**
     * Allows operator= to call Column's custom copy-constructor.
     * @param col column to be copied
     * @return the current column after update
     */
    Column &operator=(const Column &col) {
      name_ = col.name_;
      oid_ = col.oid_;
      packed_type_ = col.packed_type_;
      definition_ = col.definition_->Copy();
      return *this;
    }

    /**
     * @return column name
     */
    const std::string &Name() const { return name_; }

    /**
     * @return oid of this key column
     */
    indexkeycol_oid_t Oid() const { return oid_; }

    /**
     * @return definition expression
     */
    common::ManagedPointer<const parser::AbstractExpression> StoredExpression() const {
      return common::ManagedPointer(static_cast<const parser::AbstractExpression *>(definition_.get()));
    }

    /**
     * @warning only defined for varlen types
     * @return maximum varlen size of this varlen column
     */
    uint16_t MaxVarlenSize() const { return static_cast<uint16_t>((packed_type_ & MASK_VARLEN) >> OFFSET_VARLEN); }

    /**
     * @return size of the attribute in bytes. Varlen attributes have the sign bit set.
     */
    uint8_t AttrSize() const { return type::TypeUtil::GetTypeSize(Type()); }

    /**
     * @return type of this key column
     */
    type::TypeId Type() const { return static_cast<type::TypeId>(packed_type_ & MASK_TYPE); }

    /**
     * @return true if this column is nullable
     */
    bool Nullable() const { return static_cast<bool>(packed_type_ & MASK_NULLABLE); }

    /**
     * Default constructor for deserialization
     */
    Column() = default;

    /**
     * @return the hashed value of this column
     */
    common::hash_t Hash() const {
      common::hash_t hash = common::HashUtil::Hash(name_);
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(name_));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(Type()));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(MaxVarlenSize()));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(Nullable()));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(oid_));
      if (definition_ != nullptr) hash = common::HashUtil::CombineHashes(hash, definition_->Hash());
      return hash;
    }

    /**
     * Perform a comparison of column
     * @param rhs other column to compare against
     * @return true if the two columns are equal
     */
    bool operator==(const Column &rhs) const {
      if (name_ != rhs.name_) return false;
      if (Type() != rhs.Type()) return false;
      if (MaxVarlenSize() != rhs.MaxVarlenSize()) return false;
      if (Nullable() != rhs.Nullable()) return false;
      if (oid_ != rhs.oid_) return false;
      if (definition_ == nullptr) return rhs.definition_ == nullptr;
      return rhs.definition_ != nullptr && *definition_ == *rhs.definition_;
    }

    /**
     * Perform a comparison of column
     * @param rhs other column to compare against
     * @return true if the two columns are not equal
     */
    bool operator!=(const Column &rhs) const { return !operator==(rhs); }

    /**
     * @return column serialized to json
     */
    nlohmann::json ToJson() const;

    /**
     * Deserializes a column
     * @param j serialized column
     */
    std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j);

   private:
    static constexpr uint32_t MASK_VARLEN = 0x00FFFF00;
    static constexpr uint32_t MASK_NULLABLE = 0x00000080;
    static constexpr uint32_t MASK_TYPE = 0x0000007F;
    static constexpr uint32_t OFFSET_VARLEN = 8;

    std::string name_;
    indexkeycol_oid_t oid_;
    uint32_t packed_type_;

    std::unique_ptr<parser::AbstractExpression> definition_;

    // TODO(John): Should these "OIDS" be implicitly set by the index in the columns?
    void SetOid(indexkeycol_oid_t oid) { oid_ = oid; }

    void SetMaxVarlenSize(uint16_t max_varlen_size) {
      NOISEPAGE_ASSERT((packed_type_ & MASK_VARLEN) == 0, "Should only set max varlen size once.");
      const auto varlen_bits = (max_varlen_size << OFFSET_VARLEN) & MASK_VARLEN;
      packed_type_ = packed_type_ | varlen_bits;
    }

    void SetTypeId(type::TypeId type_id) {
      NOISEPAGE_ASSERT((packed_type_ & MASK_TYPE) == 0, "Should only set type once.");
      packed_type_ = packed_type_ | (static_cast<uint32_t>(type_id) & MASK_TYPE);
    }

    void SetNullable(bool nullable) {
      NOISEPAGE_ASSERT((packed_type_ & MASK_NULLABLE) == 0, "Should only set nullability once.");
      packed_type_ = nullable ? packed_type_ | MASK_NULLABLE : packed_type_;
    }

    friend class DatabaseCatalog;
    friend class postgres::Builder;
    friend class tpcc::Schemas;
    friend class noisepage::StorageTestUtil;
  };

  /**
   * Instantiates a new catalog description of an index
   * @param columns describing the individual parts of the key
   * @param type backing data structure of the index
   * @param is_unique indicating whether the same key can be (logically) visible repeats are allowed in the index
   * @param is_primary indicating whether this will be the index for a primary key
   * @param is_exclusion indicating whether this index is for exclusion constraints
   * @param is_immediate indicating that the uniqueness check fails at insertion time
   */
  IndexSchema(std::vector<Column> columns, const storage::index::IndexType type, const bool is_unique,
              const bool is_primary, const bool is_exclusion, const bool is_immediate)
      : columns_(std::move(columns)),
        type_(type),
        is_unique_(is_unique),
        is_primary_(is_primary),
        is_exclusion_(is_exclusion),
        is_immediate_(is_immediate) {
    NOISEPAGE_ASSERT((is_primary && is_unique) || (!is_primary), "is_primary requires is_unique to be true as well.");
    ExtractIndexedColOids();
  }

  IndexSchema() = default;

  /**
   * @return the columns which define the index's schema
   */
  const std::vector<Column> &GetColumns() const { return columns_; }

  /**
   * @param index in the column vector for the requested column
   * @return requested key column
   */
  const Column &GetColumn(uint32_t index) const { return columns_.at(index); }

  /**
   * @param name name of the Column to access
   * @return description of the schema for a specific column
   * @throw std::out_of_range if the column doesn't exist.
   */
  const Column &GetColumn(const std::string &name) const {
    for (auto &c : columns_) {
      if (c.Name() == name) {
        return c;
      }
    }
    // TODO(John): Should this be a NOISEPAGE_ASSERT to have the same semantics
    // as the other accessor methods above?
    throw std::out_of_range("Column name doesn't exist");
  }

  /**
   * @return true if is a unique index
   */
  bool Unique() const { return is_unique_; }

  /**
   * @return true if this schema is for a unique index
   */
  bool Primary() const { return is_primary_; }

  /**
   * @return true if index supports an exclusion constraint
   */
  bool Exclusion() const { return is_exclusion_; }

  /**
   * @return true if uniqueness check is enforced immediately on insertion
   */
  bool Immediate() const { return is_immediate_; }

  /**
   * @return the backend that should be used to implement this index
   */
  storage::index::IndexType Type() const { return type_; }

  /**
   * @param index_type that should be used to back this index
   */
  void SetType(storage::index::IndexType index_type) { type_ = index_type; }

  /**
   * @return serialized schema
   */
  nlohmann::json ToJson() const;

  /**
   * Should not be used. See NOISEPAGE_ASSERT
   */
  void FromJson(const nlohmann::json &j);

  /**
   * Deserialize a schema
   * @param j json containing serialized schema
   * @return deserialized schema object
   */
  std::unique_ptr<IndexSchema> static DeserializeSchema(const nlohmann::json &j);

  /**
   * @return map of index key oid to col_oid contained in that index key
   */
  const std::vector<col_oid_t> &GetIndexedColOids() const {
    NOISEPAGE_ASSERT(!indexed_oids_.empty(),
                     "The indexed oids map should not be empty. Was ExtractIndexedColOids called before?");
    return indexed_oids_;
  }

  /**
   * @warning Calling this function will traverse the entire expression tree for each column, which may be expensive for
   * large expressions. Thus, it should only be called once during object construction.
   * @return col oids in index keys, ordered by index key
   */
  void ExtractIndexedColOids() {
    // We will traverse every expr tree
    std::deque<common::ManagedPointer<const parser::AbstractExpression>> expr_queue;

    // Traverse expression tree for each index key
    for (auto &col : GetColumns()) {
      NOISEPAGE_ASSERT(col.StoredExpression() != nullptr, "Index column expr should not be missing");
      // Add root of expression of tree for the column
      expr_queue.push_back(col.StoredExpression());

      // Iterate over the tree
      while (!expr_queue.empty()) {
        auto expr = expr_queue.front();
        expr_queue.pop_front();

        // If this expr is a column value, add it to the queue
        if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
          indexed_oids_.push_back(expr.CastManagedPointerTo<const parser::ColumnValueExpression>()->GetColumnOid());
        }

        // Add children to queue
        for (const auto &child : expr->GetChildren()) {
          NOISEPAGE_ASSERT(child != nullptr, "We should not be adding missing expressions to the queue");
          expr_queue.emplace_back(child.CastManagedPointerTo<const parser::AbstractExpression>());
        }
      }
    }
  }

  /**
   * @return the hashed value of this index schema
   */
  common::hash_t Hash() const {
    // TODO(Ling): Does column order matter for hash?
    common::hash_t hash = common::HashUtil::Hash(type_);
    for (const auto &col : columns_) hash = common::HashUtil::CombineHashes(hash, col.Hash());
    hash = common::HashUtil::CombineHashInRange(hash, indexed_oids_.begin(), indexed_oids_.end());
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_unique_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_primary_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_exclusion_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_immediate_));
    return hash;
  }

  /**
   * Perform a comparison of index schema
   * @param rhs other index schema to compare against
   * @return true if the two index schemas are equal
   */
  bool operator==(const IndexSchema &rhs) const {
    if (type_ != rhs.type_) return false;
    if (is_unique_ != rhs.is_unique_) return false;
    if (is_primary_ != rhs.is_primary_) return false;
    if (is_exclusion_ != rhs.is_exclusion_) return false;
    if (is_immediate_ != rhs.is_immediate_) return false;
    // TODO(Ling): Does column order matter for compare equal?
    if (indexed_oids_ != rhs.indexed_oids_) return false;
    return columns_ == rhs.columns_;
  }

  /**
   * Perform a comparison of index schema
   * @param rhs other index schema to compare against
   * @return true if the two index schema are not equal
   */
  bool operator!=(const IndexSchema &rhs) const { return !operator==(rhs); }

 private:
  friend class DatabaseCatalog;
  std::vector<Column> columns_;
  storage::index::IndexType type_;
  std::vector<col_oid_t> indexed_oids_;
  bool is_unique_;
  bool is_primary_;
  bool is_exclusion_;
  bool is_immediate_;

  friend class Catalog;
  friend class postgres::Builder;
  friend class noisepage::TpccPlanTest;
};

DEFINE_JSON_HEADER_DECLARATIONS(IndexSchema::Column);
DEFINE_JSON_HEADER_DECLARATIONS(IndexSchema);

}  // namespace noisepage::catalog
