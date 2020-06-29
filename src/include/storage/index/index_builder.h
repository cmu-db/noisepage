#pragma once

#include "catalog/index_schema.h"
#include "storage/sql_table.h"

namespace terrier::storage::index {

class Index;
class IndexMetadata;

/**
 * The IndexBuilder automatically creates the best possible index for the given parameters.
 */
class IndexBuilder {
 private:
  catalog::IndexSchema key_schema_;
  common::ManagedPointer<storage::SqlTable> sql_table_{};
  common::ManagedPointer<transaction::TransactionContext> txn_{};

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

  /**
   * Set the SQL table and transaction context for this index builder to allow it to perform bulk inserts
   * @param sql_table the table this index is being built on
   * @param txn the transaction to use when inserting into the table
   * @param key_schema the index schema
   * @return the builder object
   */
  IndexBuilder &SetSqlTableAndTransactionContext(const common::ManagedPointer<transaction::TransactionContext> txn,
                                                 common::ManagedPointer<storage::SqlTable> sql_table,
                                                 const catalog::IndexSchema &key_schema);

  /**
   * Insert everything in the table this index is made on into the index
   * @param index newly created index
   * @param table_tuple_slot the tuple to be inserted
   * @return Whether the insertion succeeds
   */
  bool Insert(common::ManagedPointer<storage::index::Index> index, storage::TupleSlot table_tuple_slot) const;

 private:
  Index *BuildBwTreeIntsKey(IndexMetadata metadata) const;

  Index *BuildBwTreeGenericKey(IndexMetadata metadata) const;

  Index *BuildHashIntsKey(IndexMetadata metadata) const;

  Index *BuildHashGenericKey(IndexMetadata metadata) const;
};

}  // namespace terrier::storage::index
