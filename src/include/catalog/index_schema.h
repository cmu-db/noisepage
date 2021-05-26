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
}  // namespace noisepage::storage

namespace noisepage::catalog {
class DatabaseCatalog;

namespace postgres {
class Builder;
class PgCoreImpl;
}  // namespace postgres

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
    // TODO(Matt): does having two constructors even make sense anymore?

    /**
     * Non-varlen constructor for index key columns.
     * @param name column name (column name or "expr")
     * @param type the non-varlen type of the column
     * @param nullable whether the column is nullable
     * @param definition definition of this attribute
     */
    Column(std::string name, const type::TypeId type, const bool nullable, const parser::AbstractExpression &definition)
        : name_(std::move(name)),
          type_(type),
          attr_length_(type::TypeUtil::GetTypeSize(type_)),
          nullable_(nullable),
          oid_(INVALID_INDEXKEYCOL_OID),
          definition_(definition.Copy()) {
      Validate();
    }

    /**
     * Varlen constructor for index key columns.
     * @param name column name (column name or "expr")
     * @param type the varlen type of the column
     * @param type_modifier the maximum varlen size or precision for decimal
     * @param nullable whether the column is nullable
     * @param definition definition of this attribute
     */
    Column(std::string name, const type::TypeId type, const int32_t type_modifier, const bool nullable,
           const parser::AbstractExpression &definition)
        : name_(std::move(name)),
          type_(type),
          attr_length_(type::TypeUtil::GetTypeSize(type_)),
          type_modifier_(type_modifier == -1 ? 0 : type_modifier),  // TODO(Matt): this is a hack around unlimited
                                                                    // length varlens and is likely busted elsewhere
          nullable_(nullable),
          oid_(INVALID_INDEXKEYCOL_OID),
          definition_(definition.Copy()) {
      Validate();
    }

    /**
     * Overrides default copy constructor to ensure we do a deep copy on the abstract expressions
     * @param old_column to be copied
     */
    Column(const Column &old_column)
        : name_(old_column.name_),
          type_(old_column.type_),
          attr_length_(old_column.attr_length_),
          type_modifier_(old_column.type_modifier_),
          nullable_(old_column.nullable_),
          oid_(old_column.oid_),
          definition_(old_column.definition_->Copy()) {
      Validate();
    }

    /**
     * Allows operator= to call Column's custom copy-constructor.
     * @param col column to be copied
     * @return the current column after update
     */
    Column &operator=(const Column &col) {
      name_ = col.name_;
      type_ = col.type_;
      attr_length_ = col.attr_length_;
      type_modifier_ = col.type_modifier_;
      nullable_ = col.nullable_;
      oid_ = col.oid_;
      definition_ = col.definition_->Copy();
      Validate();
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
     * @return true if the column is nullable, false otherwise
     */
    bool Nullable() const { return nullable_; }

    /**
     * @return size of the attribute in bytes. Varlen attributes have the sign bit set.
     */
    uint16_t AttributeLength() const { return attr_length_; }

    /**
     * @return The maximum length of this column (only valid if it's VARLEN)
     */
    int32_t TypeModifier() const { return type_modifier_; }

    /**
     * @return SQL type for this column
     */
    type::TypeId Type() const { return type_; }

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
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(type_));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(type_modifier_));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(nullable_));
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
      if (type_ != rhs.type_) return false;
      if (attr_length_ != rhs.attr_length_) return false;
      if ((ShouldHaveTypeModifier() != rhs.ShouldHaveTypeModifier()) ||
          ((ShouldHaveTypeModifier() && rhs.ShouldHaveTypeModifier()) && type_modifier_ != rhs.type_modifier_))
        return false;
      if (nullable_ != rhs.nullable_) return false;
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
    bool ShouldHaveTypeModifier() const {
      return type_ == type::TypeId::VARCHAR || type_ == type::TypeId::VARBINARY || type_ == type::TypeId::DECIMAL;
    }

    void Validate() const {
      NOISEPAGE_ASSERT(type_ != type::TypeId::INVALID, "Attribute type cannot be INVALID.");
      NOISEPAGE_ASSERT(definition_ != nullptr, "Definition cannot be nullptr.");

      if (type_ == type::TypeId::VARCHAR || type_ == type::TypeId::VARBINARY) {
        NOISEPAGE_ASSERT(attr_length_ == storage::VARLEN_COLUMN, "Invalid attribute length.");
        // TODO(Matt): uncomment this assertion once we decide what a reasonable default is for unlimited varlens

        //        NOISEPAGE_ASSERT(type_modifier_ == -1 || type_modifier_ > 0,
        //                         "Type modifier should be -1 (no limit), or a positive integer.");
      } else if (type_ == type::TypeId::DECIMAL) {
        NOISEPAGE_ASSERT(attr_length_ == 16, "Invalid attribute length.");
        NOISEPAGE_ASSERT(type_modifier_ > 0, "Type modifier should be a  positive integer.");
      } else {
        NOISEPAGE_ASSERT(attr_length_ == 1 || attr_length_ == 2 || attr_length_ == 4 || attr_length_ == 8,
                         "Invalid attribute length.");
        NOISEPAGE_ASSERT(type_modifier_ == -1, "Invalid attribute modifier. Should be -1 for types that don't use it.");
      }
    }

    std::string name_;
    type::TypeId type_;
    uint16_t attr_length_;
    int32_t type_modifier_ = -1;  // corresponds to Postgres' atttypmod int4: atttypmod records type-specific data
    // supplied at table creation time (for example, the maximum length of a varchar
    // column). It is passed to type-specific input functions and length coercion
    // functions. The value will generally be -1 for types that do not need atttypmod.
    bool nullable_;
    indexkeycol_oid_t oid_;

    std::unique_ptr<parser::AbstractExpression> definition_;

    // TODO(John): Should these "OIDS" be implicitly set by the index in the columns?
    void SetOid(indexkeycol_oid_t oid) { oid_ = oid; }

    void SetTypeModifier(const int32_t type_modifier) { type_modifier_ = type_modifier; }

    void SetTypeId(const type::TypeId type) { type_ = type; }

    void SetNullable(const bool nullable) { nullable_ = nullable; }

    friend class DatabaseCatalog;
    friend class postgres::Builder;
    friend class postgres::PgCoreImpl;
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
  friend class Catalog;
  friend class DatabaseCatalog;
  friend class postgres::Builder;
  friend class postgres::PgCoreImpl;
  friend class noisepage::TpccPlanTest;

  std::vector<Column> columns_;
  storage::index::IndexType type_;
  std::vector<col_oid_t> indexed_oids_;
  bool is_unique_;
  bool is_primary_;
  bool is_exclusion_;
  bool is_immediate_;
};

DEFINE_JSON_HEADER_DECLARATIONS(IndexSchema::Column);
DEFINE_JSON_HEADER_DECLARATIONS(IndexSchema);

}  // namespace noisepage::catalog
