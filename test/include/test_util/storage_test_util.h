#pragma once

#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "common/strong_typedef.h"
#include "gtest/gtest.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "storage/data_table.h"
#include "storage/garbage_collector.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/index_defs.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "storage/storage_util.h"
#include "storage/tuple_access_strategy.h"
#include "storage/undo_record.h"
#include "test_util/multithread_test_util.h"
#include "test_util/random_test_util.h"
#include "transaction/transaction_manager.h"
#include "type/type_id.h"

namespace terrier {
class StorageTestUtil {
 public:
  StorageTestUtil() = delete;

#define TO_INT(p) reinterpret_cast<uintptr_t>(p)

#define MAX_TEST_VARLEN_SIZE (5 * storage::VarlenEntry::InlineThreshold())

  /**
   * Check if memory address represented by val in [lower, upper)
   * @tparam A type of ptr
   * @tparam B type of ptr
   * @tparam C type of ptr
   * @param val value to check
   * @param lower lower bound
   * @param upper upper bound
   */
  template <typename A, typename B, typename C>
  static void CheckInBounds(A *const val, B *const lower, C *const upper) {
    EXPECT_GE(TO_INT(val), TO_INT(lower));
    EXPECT_LT(TO_INT(val), TO_INT(upper));
  }

  /**
   * Check if memory address represented by val not in [lower, upper)
   * @tparam A type of ptr
   * @tparam B type of ptr
   * @tparam C type of ptr
   * @param val value to check
   * @param lower lower bound
   * @param upper upper bound
   */
  template <typename A, typename B, typename C>
  static void CheckNotInBounds(A *const val, B *const lower, C *const upper) {
    EXPECT_TRUE(TO_INT(val) < TO_INT(lower) || TO_INT(val) >= TO_INT(upper));
  }

  template <typename A>
  static void CheckAlignment(A *const val, const uint32_t word_size) {
    EXPECT_EQ(0, TO_INT(val) % word_size);
  }
#undef TO_INT
  /**
   * @tparam A type of ptr
   * @param ptr ptr to start from
   * @param bytes bytes to advance
   * @return  pointer that is the specified amount of bytes ahead of the given
   */
  template <typename A>
  static A *IncrementByBytes(A *const ptr, const uint64_t bytes) {
    return reinterpret_cast<A *>(reinterpret_cast<byte *>(ptr) + bytes);
  }

  // Returns a random layout that is guaranteed to be valid.
  template <typename Random>
  static storage::BlockLayout RandomLayoutNoVarlen(const uint16_t max_cols, Random *const generator) {
    return RandomLayout(max_cols, generator, false);
  }

  template <typename Random>
  static storage::BlockLayout RandomLayoutWithVarlens(const uint16_t max_cols, Random *const generator) {
    return RandomLayout(max_cols, generator, true);
  }

  // Returns a random schema that is guaranteed to be valid.
  template <typename Random>
  static catalog::Schema *RandomSchemaNoVarlen(const uint16_t max_cols, Random *const generator) {
    return RandomSchema(max_cols, generator, false);
  }

  template <typename Random>
  static catalog::Schema *RandomSchemaWithVarlens(const uint16_t max_cols, Random *const generator) {
    return RandomSchema(max_cols, generator, true);
  }

  // Fill the given location with the specified amount of random bytes, using the
  // given generator as a source of randomness.
  template <typename Random>
  static void FillWithRandomBytes(const uint32_t num_bytes, byte *const out, Random *const generator) {
    std::uniform_int_distribution<uint8_t> dist(0, UINT8_MAX);
    for (uint32_t i = 0; i < num_bytes; i++) out[i] = static_cast<byte>(dist(*generator));
  }

  template <typename Random>
  static void PopulateRandomRow(storage::ProjectedRow *const row, const storage::BlockLayout &layout,
                                const double null_bias, Random *const generator) {
    std::bernoulli_distribution coin(1 - null_bias);
    // TODO(Tianyu): I don't think this matters as a tunable thing?
    // Make sure we have a mix of inlined and non-inlined values
    std::uniform_int_distribution<uint32_t> varlen_size(1, MAX_TEST_VARLEN_SIZE);
    // For every column in the project list, populate its attribute with random bytes or set to null based on coin flip
    for (uint16_t projection_list_idx = 0; projection_list_idx < row->NumColumns(); projection_list_idx++) {
      storage::col_id_t col = row->ColumnIds()[projection_list_idx];

      if (coin(*generator)) {
        if (layout.IsVarlen(col)) {
          uint32_t size = varlen_size(*generator);
          if (size > storage::VarlenEntry::InlineThreshold()) {
            byte *varlen = common::AllocationUtil::AllocateAligned(size);
            FillWithRandomBytes(size, varlen, generator);
            // varlen entries always start off not inlined
            *reinterpret_cast<storage::VarlenEntry *>(row->AccessForceNotNull(projection_list_idx)) =
                storage::VarlenEntry::Create(varlen, size, true);
          } else {
            byte buf[storage::VarlenEntry::InlineThreshold()];
            FillWithRandomBytes(size, buf, generator);
            *reinterpret_cast<storage::VarlenEntry *>(row->AccessForceNotNull(projection_list_idx)) =
                storage::VarlenEntry::CreateInline(buf, size);
          }
        } else {
          FillWithRandomBytes(layout.AttrSize(col), row->AccessForceNotNull(projection_list_idx), generator);
        }
      } else {
        row->SetNull(projection_list_idx);
      }
    }
  }

  static std::vector<storage::col_id_t> ProjectionListAllColumns(const storage::BlockLayout &layout) {
    std::vector<storage::col_id_t> col_ids(layout.NumColumns() - storage::NUM_RESERVED_COLUMNS);
    // Add all of the column ids from the layout to the projection list
    // 0 is version vector so we skip it
    for (uint16_t col = storage::NUM_RESERVED_COLUMNS; col < layout.NumColumns(); col++) {
      col_ids[col - storage::NUM_RESERVED_COLUMNS] = storage::col_id_t(col);
    }
    return col_ids;
  }

  template <typename Random>
  static std::vector<storage::col_id_t> ProjectionListRandomColumns(const storage::BlockLayout &layout,
                                                                    Random *const generator) {
    std::vector<storage::col_id_t> col_ids;
    // Add all of the column ids from the layout to the projection list
    // 0 is version vector so we skip it
    for (uint16_t col = storage::NUM_RESERVED_COLUMNS; col < layout.NumColumns(); col++) col_ids.emplace_back(col);

    return RandomNonEmptySubset(col_ids, generator);
  }

  template <typename Random, typename T>
  static std::vector<T> RandomNonEmptySubset(std::vector<T> elems, Random *const generator) {
    // randomly select a number of elems for this delta to contain. Must be at least 1
    auto num_elems = std::uniform_int_distribution<size_t>(1, elems.size())(*generator);

    // Permute the elems
    std::shuffle(elems.begin(), elems.end(), *generator);

    // truncate the list
    elems.resize(num_elems);

    return elems;
  }

  // Populate a block with random tuple according to the given parameters. Returns a mapping of the tuples in each slot.
  template <class Random>
  static std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> PopulateBlockRandomly(
      storage::DataTable *table, storage::RawBlock *block, double empty_ratio, Random *const generator) {
    const storage::BlockLayout &layout = table->GetBlockLayout();
    std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> result;
    std::bernoulli_distribution coin(empty_ratio);
    // TODO(Tianyu): Do we ever want to tune this for tests?
    const double null_ratio = 0.1;
    // TODO(Tianyu): Have to construct one since we don't have access to data table private members.
    storage::TupleAccessStrategy accessor(layout);
    auto initializer =
        storage::ProjectedRowInitializer::Create(layout, StorageTestUtil::ProjectionListAllColumns(layout));
    for (uint32_t i = 0; i < layout.NumSlots(); i++) {
      storage::TupleSlot slot;
      bool ret UNUSED_ATTRIBUTE = accessor.Allocate(block, &slot);
      TERRIER_ASSERT(ret && slot == storage::TupleSlot(block, i),
                     "slot allocation should happen sequentially and succeed");
      if (coin(*generator)) {
        // slot will be marked empty
        accessor.Deallocate(slot);
        continue;
      }
      auto *redo_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
      storage::ProjectedRow *redo = initializer.InitializeRow(redo_buffer);
      StorageTestUtil::PopulateRandomRow(redo, layout, null_ratio, generator);
      result[slot] = redo;
      // Copy without transactions to simulate a version-free block
      accessor.SetNotNull(slot, storage::VERSION_POINTER_COLUMN_ID);
      for (uint16_t j = 0; j < redo->NumColumns(); j++)
        storage::StorageUtil::CopyAttrFromProjection(accessor, slot, *redo, j);
    }
    TERRIER_ASSERT(block->GetInsertHead() == layout.NumSlots(), "The block should be considered full at this point");
    return result;
  }

  // Populate a block with random tuple according to the given parameters. Does not retain the values inserted for
  // performance. Returns the number of non-empty slots in the block after population.
  template <class Random>
  static uint32_t PopulateBlockRandomlyNoBookkeeping(storage::DataTable *table, storage::RawBlock *block,
                                                     double empty_ratio, Random *const generator) {
    const storage::BlockLayout layout = table->GetBlockLayout();
    uint32_t result = 0;
    std::bernoulli_distribution coin(empty_ratio);
    // TODO(Tianyu): Do we ever want to tune this for tests?
    const double null_ratio = 0.1;
    // TODO(Tianyu): Have to construct one since we don't have access to data table private members.
    storage::TupleAccessStrategy accessor(layout);
    accessor.InitializeRawBlock(table, block, storage::layout_version_t(0));
    storage::ProjectedRowInitializer initializer =
        storage::ProjectedRowInitializer::Create(layout, StorageTestUtil::ProjectionListAllColumns(layout));
    auto *redo_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *redo = initializer.InitializeRow(redo_buffer);
    for (uint32_t i = 0; i < layout.NumSlots(); i++) {
      storage::TupleSlot slot;
      bool ret UNUSED_ATTRIBUTE = accessor.Allocate(block, &slot);
      TERRIER_ASSERT(ret && slot == storage::TupleSlot(block, i),
                     "slot allocation should happen sequentially and succeed");
      if (coin(*generator)) {
        // slot will be marked empty
        accessor.Deallocate(slot);
        continue;
      }
      result++;
      StorageTestUtil::PopulateRandomRow(redo, layout, null_ratio, generator);
      // Copy without transactions to simulate a version-free block
      accessor.SetNotNull(slot, storage::VERSION_POINTER_COLUMN_ID);
      for (uint16_t j = 0; j < redo->NumColumns(); j++)
        storage::StorageUtil::CopyAttrFromProjection(accessor, slot, *redo, j);
    }
    TERRIER_ASSERT(block->GetInsertHead() == layout.NumSlots(), "The block should be considered full at this point");
    delete[] redo_buffer;
    return result;
  }

  template <class Random>
  static storage::ProjectedRowInitializer RandomInitializer(const storage::BlockLayout &layout, Random *generator) {
    return {layout, ProjectionListRandomColumns(layout, generator)};
  }

  // Returns true iff the underlying varlen is bit-wise identical. Compressions schemes and other metadata are ignored.
  static bool VarlenEntryEqualDeep(const storage::VarlenEntry &one, const storage::VarlenEntry &other) {
    if (one.Size() != other.Size()) return false;
    return memcmp(one.Content(), other.Content(), one.Size()) == 0;
  }

  template <class RowType1, class RowType2>
  static bool ProjectionListEqualDeep(const storage::BlockLayout &layout, const RowType1 *const one,
                                      const RowType2 *const other) {
    if (one->NumColumns() != other->NumColumns()) return false;
    for (uint16_t projection_list_index = 0; projection_list_index < one->NumColumns(); projection_list_index++) {
      // Check that the two point at the same column
      storage::col_id_t one_id = one->ColumnIds()[projection_list_index];
      storage::col_id_t other_id = other->ColumnIds()[projection_list_index];
      if (one_id != other_id) return false;

      // Check that the two have the same content bit-wise
      uint8_t attr_size = layout.AttrSize(one_id);
      const byte *one_content = one->AccessWithNullCheck(projection_list_index);
      const byte *other_content = other->AccessWithNullCheck(projection_list_index);
      // Either both are null or neither is null.

      if (one_content == nullptr || other_content == nullptr) {
        if (one_content == other_content) continue;
        return false;
      }

      if (layout.IsVarlen(one_id)) {
        // Need to follow pointers and throw away metadata and padding for equality comparison
        auto &one_entry = *reinterpret_cast<const storage::VarlenEntry *>(one_content),
             &other_entry = *reinterpret_cast<const storage::VarlenEntry *>(other_content);
        if (one_entry.Size() != other_entry.Size()) return false;
        if (memcmp(one_entry.Content(), other_entry.Content(), one_entry.Size()) != 0) return false;
      } else if (memcmp(one_content, other_content, attr_size) != 0) {
        // Otherwise, they should be bit-wise identical.
        return false;
      }
    }
    return true;
  }

  template <class RowType1, class RowType2>
  static bool ProjectionListEqualShallow(const storage::BlockLayout &layout, const RowType1 *const one,
                                         const RowType2 *const other) {
    EXPECT_EQ(one->NumColumns(), other->NumColumns());
    if (one->NumColumns() != other->NumColumns()) return false;
    for (uint16_t projection_list_index = 0; projection_list_index < one->NumColumns(); projection_list_index++) {
      // Check that the two point at the same column
      storage::col_id_t one_id = one->ColumnIds()[projection_list_index];
      storage::col_id_t other_id = other->ColumnIds()[projection_list_index];
      EXPECT_EQ(one_id, other_id);
      if (one_id != other_id) return false;

      // Check that the two have the same content bit-wise
      uint8_t attr_size = layout.AttrSize(one_id);
      const byte *one_content = one->AccessWithNullCheck(projection_list_index);
      const byte *other_content = other->AccessWithNullCheck(projection_list_index);
      // Either both are null or neither is null.
      if (one_content == nullptr || other_content == nullptr) {
        EXPECT_EQ(one_content, other_content);
        if (one_content == other_content) continue;
        return false;
      }
      // Otherwise, they should be bit-wise identical.
      if (memcmp(one_content, other_content, attr_size) != 0) return false;
    }
    return true;
  }

  static storage::ProjectedRow *ProjectedRowDeepCopy(const storage::BlockLayout &layout,
                                                     const storage::ProjectedRow &original) {
    byte *result_buf = common::AllocationUtil::AllocateAligned(original.Size());
    std::memcpy(result_buf, &original, original.Size());
    auto *result = reinterpret_cast<storage::ProjectedRow *>(result_buf);

    for (uint16_t projection_list_index = 0; projection_list_index < original.NumColumns(); projection_list_index++) {
      storage::col_id_t col_id = result->ColumnIds()[projection_list_index];
      const byte *original_val = original.AccessWithNullCheck(projection_list_index);
      if (!layout.IsVarlen(col_id)) continue;

      auto *original_entry = reinterpret_cast<const storage::VarlenEntry *>(original_val);
      if (original_entry == nullptr) continue;

      auto *copied_entry = reinterpret_cast<storage::VarlenEntry *>(result->AccessForceNotNull(projection_list_index));
      if (original_entry->IsInlined()) {
        *copied_entry = *original_entry;
      } else {
        byte *copied_content = common::AllocationUtil::AllocateAligned(original_entry->Size());
        std::memcpy(copied_content, original_entry->Content(), original_entry->Size());
        // Always needs reclaim because we just made a copy
        *copied_entry = storage::VarlenEntry::Create(copied_content, original_entry->Size(), true);
      }
    }
    return result;
  }

  /**
   * Tests if two sql tables with the same schema have identical visible tuples
   * @param layout layout of the two sql tables
   * @param table_one sql table to compare to table_two
   * @param table_two sql table to compare to table_one
   * @param table_one_tuples vector of all tuple slots in table one
   * @param tuple_slot_map mapping of tuple slots in table one to corresponding tuple slots in table two
   * @param txn_manager_one manager to begin txn to scan table_one
   * @param txn_manager_two manager to begin txn to scan table_two (can be the same as txn_manager_one)
   * @return true if tables are equal
   */
  static bool SqlTableEqualDeep(const storage::BlockLayout &layout, common::ManagedPointer<storage::SqlTable> table_one,
                                common::ManagedPointer<storage::SqlTable> table_two,
                                const std::vector<storage::TupleSlot> &table_one_tuples,
                                const std::unordered_map<storage::TupleSlot, storage::TupleSlot> &tuple_slot_map,
                                transaction::TransactionManager *txn_manager_one,
                                transaction::TransactionManager *txn_manager_two) {
    auto *txn_one = txn_manager_one->BeginTransaction();
    auto *txn_two = txn_manager_two->BeginTransaction();

    auto initializer =
        storage::ProjectedRowInitializer::Create(layout, StorageTestUtil::ProjectionListAllColumns(layout));
    auto *buffer_one = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *buffer_two = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *row_one = initializer.InitializeRow(buffer_one);
    storage::ProjectedRow *row_two = initializer.InitializeRow(buffer_two);

    // Select each tuple for both tables and perform equality
    bool result = true;
    for (auto &tuple : table_one_tuples) {
      TERRIER_ASSERT(tuple_slot_map.find(tuple) != tuple_slot_map.end(), "No mapping for this tuple slot");
      table_one->Select(common::ManagedPointer(txn_one), tuple, row_one);
      table_two->Select(common::ManagedPointer(txn_two), tuple_slot_map.at(tuple), row_two);
      if (!ProjectionListEqualDeep(layout, row_one, row_two)) {
        result = false;
        break;
      }
    }

    txn_manager_one->Commit(txn_one, transaction::TransactionUtil::EmptyCallback, nullptr);
    txn_manager_two->Commit(txn_two, transaction::TransactionUtil::EmptyCallback, nullptr);
    delete[] buffer_one;
    delete[] buffer_two;
    return result;
  }

  template <class RowType>
  static std::string PrintRow(const RowType &row, const storage::BlockLayout &layout) {
    std::ostringstream os;
    os << "num_cols: " << row.NumColumns() << std::endl;
    for (uint16_t i = 0; i < row.NumColumns(); i++) {
      storage::col_id_t col_id = row.ColumnIds()[i];
      const byte *attr = row.AccessWithNullCheck(i);
      if (attr == nullptr) {
        os << "col_id: " << col_id.UnderlyingValue() << " is NULL" << std::endl;
        continue;
      }

      if (layout.IsVarlen(col_id)) {
        auto *entry = reinterpret_cast<const storage::VarlenEntry *>(attr);
        os << "col_id: " << col_id.UnderlyingValue();
        os << " is varlen, ptr " << entry->Content();
        os << ", size " << entry->Size();
        os << ", reclaimable " << entry->NeedReclaim();
        os << ", content ";
        for (uint8_t pos = 0; pos < entry->Size(); pos++) {
          os << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint8_t>(entry->Content()[pos]);
        }
        os << std::endl;
      } else {
        os << "col_id: " << col_id.UnderlyingValue();
        os << " is ";
        for (uint8_t pos = 0; pos < layout.AttrSize(col_id); pos++) {
          os << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint8_t>(attr[pos]);
        }
        os << std::endl;
      }
    }
    return os.str();
  }

  // Write the given tuple (projected row) into a block using the given access strategy,
  // at the specified offset
  static void InsertTuple(const storage::ProjectedRow &tuple, const storage::TupleAccessStrategy &tested,
                          const storage::BlockLayout &layout, const storage::TupleSlot slot) {
    // Skip the version vector for tuples
    for (uint16_t projection_list_index = 0; projection_list_index < tuple.NumColumns(); projection_list_index++) {
      storage::col_id_t col_id(static_cast<uint16_t>(projection_list_index + storage::NUM_RESERVED_COLUMNS));
      const byte *val_ptr = tuple.AccessWithNullCheck(projection_list_index);
      if (val_ptr == nullptr)
        tested.SetNull(slot, storage::col_id_t(projection_list_index));
      else
        std::memcpy(tested.AccessForceNotNull(slot, col_id), val_ptr, layout.AttrSize(col_id));
    }
  }

  // Check that the written tuple is the same as the expected one. Does not follow varlen pointers to check that the
  // underlying values are the same
  static void CheckTupleEqualShallow(const storage::ProjectedRow &expected, const storage::TupleAccessStrategy &tested,
                                     const storage::BlockLayout &layout, const storage::TupleSlot slot) {
    for (uint16_t col = storage::NUM_RESERVED_COLUMNS; col < layout.NumColumns(); col++) {
      storage::col_id_t col_id(col);
      const byte *val_ptr = expected.AccessWithNullCheck(static_cast<uint16_t>(col - storage::NUM_RESERVED_COLUMNS));
      byte *col_slot = tested.AccessWithNullCheck(slot, col_id);
      if (val_ptr == nullptr) {
        EXPECT_TRUE(col_slot == nullptr);
      } else {
        EXPECT_TRUE(!memcmp(val_ptr, col_slot, layout.AttrSize(col_id)));
      }
    }
  }

  /**
   * Provides function for tests at large to force column OIDs so that they can function without a catalog.
   */
  static void ForceOid(catalog::Schema::Column *const col, catalog::col_oid_t oid) { col->SetOid(oid); }
  static void ForceOid(catalog::IndexSchema::Column *const col, catalog::indexkeycol_oid_t oid) { col->SetOid(oid); }

  /**
   * Generates a random GenericKey-compatible schema with the given number of columns using the given types.
   */
  template <typename Random>
  static catalog::IndexSchema RandomGenericKeySchema(const uint32_t num_cols, const std::vector<type::TypeId> &types,
                                                     Random *generator) {
    uint32_t max_varlen_size = 20;
    TERRIER_ASSERT(num_cols > 0, "Must have at least one column in your key schema.");

    std::vector<catalog::indexkeycol_oid_t> key_oids;
    key_oids.reserve(num_cols);

    for (uint32_t i = 0; i < num_cols; i++) {
      key_oids.emplace_back(i);
    }

    std::shuffle(key_oids.begin(), key_oids.end(), *generator);

    std::vector<catalog::IndexSchema::Column> key_cols;

    for (uint32_t i = 0; i < num_cols; i++) {
      auto key_oid = key_oids[i];
      auto type = *RandomTestUtil::UniformRandomElement(types, generator);
      auto is_nullable = static_cast<bool>(std::uniform_int_distribution(0, 1)(*generator));

      switch (type) {
        case type::TypeId::VARBINARY:
        case type::TypeId::VARCHAR: {
          auto varlen_size = std::uniform_int_distribution(0U, max_varlen_size)(*generator);
          key_cols.emplace_back("", type, varlen_size, is_nullable, parser::ConstantValueExpression(type));
          break;
        }
        default:
          key_cols.emplace_back("", type, is_nullable, parser::ConstantValueExpression(type));
          break;
      }
      ForceOid(&(key_cols.back()), key_oid);
    }

    return catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, false, false, false, true);
  }

  /**
   * Generates a random simple key (integral and not NULL-able) schema
   */
  template <typename Random>
  static catalog::IndexSchema RandomSimpleKeySchema(Random *generator, const uint16_t max_bytes) {
    const auto key_size = std::uniform_int_distribution(static_cast<uint16_t>(1), max_bytes)(*generator);

    const uint16_t max_cols = max_bytes;  // could have up to max_bytes TINYINTs
    std::vector<catalog::indexkeycol_oid_t> key_oids;
    key_oids.reserve(max_cols);

    for (auto i = 0; i < max_cols; i++) {
      key_oids.emplace_back(i);
    }

    std::shuffle(key_oids.begin(), key_oids.end(), *generator);

    std::vector<catalog::IndexSchema::Column> key_cols;

    uint8_t col = 0;

    for (uint16_t bytes_used = 0; bytes_used != key_size;) {
      auto max_offset = static_cast<uint8_t>(storage::index::NUMERIC_KEY_TYPES.size() - 1);
      for (const auto &type : storage::index::NUMERIC_KEY_TYPES) {
        if (key_size - bytes_used < type::TypeUtil::GetTypeSize(type)) {
          max_offset--;
        }
      }
      const uint8_t type_offset = std::uniform_int_distribution(static_cast<uint8_t>(0), max_offset)(*generator);
      const auto type = storage::index::NUMERIC_KEY_TYPES[type_offset];

      key_cols.emplace_back("", type, false, parser::ConstantValueExpression(type));
      ForceOid(&(key_cols.back()), key_oids[col++]);
      bytes_used = static_cast<uint16_t>(bytes_used + type::TypeUtil::GetTypeSize(type));
    }

    return catalog::IndexSchema(key_cols, storage::index::IndexType::BWTREE, false, false, false, true);
  }

 private:
  template <typename Random>
  static storage::BlockLayout RandomLayout(const uint16_t max_cols, Random *const generator, bool allow_varlen) {
    TERRIER_ASSERT(max_cols > storage::NUM_RESERVED_COLUMNS, "There should be at least 2 cols (reserved for version).");
    // We probably won't allow tables with fewer than 2 columns
    const uint16_t num_attrs =
        std::uniform_int_distribution<uint16_t>(storage::NUM_RESERVED_COLUMNS + 1, max_cols)(*generator);
    std::vector<uint16_t> possible_attr_sizes{1, 2, 4, 8}, attr_sizes(num_attrs);
    if (allow_varlen) possible_attr_sizes.push_back(storage::VARLEN_COLUMN);

    for (uint16_t i = 0; i < storage::NUM_RESERVED_COLUMNS; i++) {
      attr_sizes[i] = 8;
    }

    for (uint16_t i = storage::NUM_RESERVED_COLUMNS; i < num_attrs; i++)
      attr_sizes[i] = *RandomTestUtil::UniformRandomElement(&possible_attr_sizes, generator);
    return storage::BlockLayout(attr_sizes);
  }

  template <typename Random>
  static catalog::Schema *RandomSchema(const uint16_t max_cols, Random *const generator, bool allow_varlen) {
    const uint16_t num_attrs = std::uniform_int_distribution<uint16_t>(1, max_cols)(*generator);
    std::vector<type::TypeId> possible_attr_types{type::TypeId::BOOLEAN, type::TypeId::SMALLINT, type::TypeId::INTEGER,
                                                  type::TypeId::DECIMAL};
    if (allow_varlen) possible_attr_types.push_back(type::TypeId::VARCHAR);

    std::vector<catalog::Schema::Column> columns;
    columns.reserve(num_attrs);

    for (uint16_t i = 0; i < num_attrs; i++) {
      auto random_type = *RandomTestUtil::UniformRandomElement(&possible_attr_types, generator);

      catalog::Schema::Column col;
      if (random_type == type::TypeId::VARCHAR) {
        col = catalog::Schema::Column("col" + std::to_string(i), random_type, MAX_TEST_VARLEN_SIZE, false,
                                      parser::ConstantValueExpression(type::TypeId::INTEGER));
      } else {
        col = catalog::Schema::Column("col" + std::to_string(i), random_type, false,
                                      parser::ConstantValueExpression(type::TypeId::INTEGER));
      }
      col.SetOid(catalog::col_oid_t(i));
      columns.push_back(col);
    }

    return new catalog::Schema(columns);
  }
};
}  // namespace terrier
