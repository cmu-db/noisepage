#include "storage/sql_table.h"

#include <parser/expression/constant_value_expression.h>
#include <map>
#include <string>
#include <vector>

#include "common/macros.h"
#include "storage/storage_util.h"
#include "type/transient_value_peeker.h"
#include "type/type_util.h"
#include "util/time_util.h"

namespace terrier::storage {

SqlTable::SqlTable(const common::ManagedPointer<BlockStore> store, const catalog::Schema &schema)
    : block_store_(store) {
  // Initialize a DataTable
  tables_ = {{layout_version_t(0),
              CreateTable(common::ManagedPointer<const catalog::Schema>(&schema), store, layout_version_t(0))}};
}

bool SqlTable::Select(const common::ManagedPointer<transaction::TransactionContext> txn, const TupleSlot slot,
                      ProjectedRow *const out_buffer, layout_version_t layout_version) const {
  // get the version of current tuple slot
  const auto tuple_version = slot.GetBlock()->data_table_->layout_version_;

  TERRIER_ASSERT(tuple_version <= layout_version,
                 "The iterator should not go to data tables with more recent version than the current transaction.");

  if (tuple_version == layout_version) {
    // when current version is same as the intended layout version, get the tuple without transformation
    return tables_.at(tuple_version).data_table_->Select(txn, slot, out_buffer);
  }

  // the tuple exists in an older version.
  // TODO(schema-change): handle versions from add and/or drop column only
  col_id_t ori_header[out_buffer->NumColumns()];
  AttrSizeMap size_map;

  auto desired_v = tables_.at(layout_version);
  auto tuple_v = tables_.at(tuple_version);
  auto missing UNUSED_ATTRIBUTE = AlignHeaderToVersion(out_buffer, tuple_v, desired_v, &ori_header[0], &size_map);
  auto result = tables_.at(tuple_version).data_table_->Select(txn, slot, out_buffer, &size_map);

  // copy back the original header
  std::memcpy(out_buffer->ColumnIds(), ori_header, sizeof(col_id_t) * out_buffer->NumColumns());

  // fill in missing columns and default values
  FillMissingColumns(out_buffer, desired_v);
  return result;
}

std::pair<bool, TupleSlot> SqlTable::Update(const common::ManagedPointer<transaction::TransactionContext> txn,
                                            RedoRecord *const redo, layout_version_t layout_version) const {
  TERRIER_ASSERT(redo->GetTupleSlot() != TupleSlot(nullptr, 0), "TupleSlot was never set in this RedoRecord.");
  TERRIER_ASSERT(redo == reinterpret_cast<LogRecord *>(txn->redo_buffer_.LastRecord())
                             ->LogRecord::GetUnderlyingRecordBodyAs<RedoRecord>(),
                 "This RedoRecord is not the most recent entry in the txn's RedoBuffer. Was StageWrite called "
                 "immediately before?");
  // TODO(Schema-Change): need to take care of tuple migration if the update touches the new columns added
  //  The migration should be a delete (MVCC style) in old datatable followed by an insert in new datatable.

  // get the version of current tuple slot
  auto curr_tuple = redo->GetTupleSlot();
  auto returned_slot = curr_tuple;

  const auto tuple_version = curr_tuple.GetBlock()->data_table_->layout_version_;

  TERRIER_ASSERT(tuple_version <= layout_version,
                 "The iterator should not go to data tables with more recent version than the current transaction.");

  bool result;
  if (tuple_version == layout_version) {
    result = tables_.at(layout_version).data_table_->Update(txn, curr_tuple, *(redo->Delta()));
  } else {
    // tuple in an older version, check if all modified columns are in the datatable version where the tuple is in
    col_id_t ori_header[redo->Delta()->NumColumns()];
    AttrSizeMap size_map;

    auto desired_v = tables_.at(layout_version);
    auto tuple_v = tables_.at(tuple_version);
    auto missing = AlignHeaderToVersion(redo->Delta(), tuple_v, desired_v, &ori_header[0], &size_map);

    if (!missing) {
      result = tuple_v.data_table_->Update(txn, redo->GetTupleSlot(), *(redo->Delta()));
      std::memcpy(redo->Delta()->ColumnIds(), ori_header, sizeof(col_id_t) * redo->Delta()->NumColumns());
    } else {
      // touching columns that are in the desired schema, but not the actual schema
      // do an delete followed by an insert

      // get projected row from redo (This projection is deterministic for identical set of columns)
      std::vector<col_id_t> col_ids;
      for (const auto &it : desired_v.column_id_to_oid_map_) col_ids.emplace_back(it.first);
      auto initializer = ProjectedRowInitializer::Create(desired_v.layout_, col_ids);
      auto *const buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
      auto *pr = initializer.InitializeRow(buffer);

      // fill in values to the projection
      result = Select(txn, curr_tuple, pr, layout_version);

      if (result) {
        // delete it from old datatable
        result = tuple_v.data_table_->Delete(txn, curr_tuple);
        if (result) {
          // insert it to new datatable

          // apply the change
          StorageUtil::ApplyDelta(desired_v.layout_, *(redo->Delta()), pr);
          const auto slot UNUSED_ATTRIBUTE = desired_v.data_table_->Insert(txn, *pr);
        }
      }
      delete[] buffer;
    }
  }
  if (!result) {
    // For MVCC correctness, this txn must now abort for the GC to clean up the version chain in the DataTable
    // correctly.
    txn->SetMustAbort();
  }
  return std::make_pair(result, returned_slot);
}

TupleSlot SqlTable::Insert(const common::ManagedPointer<transaction::TransactionContext> txn, RedoRecord *const redo,
                           layout_version_t layout_version) const {
  TERRIER_ASSERT(redo->GetTupleSlot() == TupleSlot(nullptr, 0), "TupleSlot was set in this RedoRecord.");
  TERRIER_ASSERT(redo == reinterpret_cast<LogRecord *>(txn->redo_buffer_.LastRecord())
                             ->LogRecord::GetUnderlyingRecordBodyAs<RedoRecord>(),
                 "This RedoRecord is not the most recent entry in the txn's RedoBuffer. Was StageWrite called "
                 "immediately before?");
  const auto slot = tables_.at(layout_version).data_table_->Insert(txn, *(redo->Delta()));
  redo->SetTupleSlot(slot);
  return slot;
}

void SqlTable::Scan(const terrier::common::ManagedPointer<transaction::TransactionContext> txn,
                    DataTable::SlotIterator *const start_pos, ProjectedColumns *const out_buffer,
                    const layout_version_t layout_version) const {
  layout_version_t tuple_version = (*start_pos)->GetBlock()->layout_version_;

  TERRIER_ASSERT(out_buffer->NumColumns() <= tables_.at(layout_version).column_oid_to_id_map_.size(),
                 "The output buffer never returns the version pointer columns, so it should have "
                 "fewer attributes.");

  // Check for version match
  if (tuple_version == layout_version) {
    tables_.at(layout_version).data_table_->Scan(txn, start_pos, out_buffer);
    return;
  }

  col_id_t ori_header[out_buffer->NumColumns()];
  bool missing = false;
  uint32_t filled = 0;
  auto desired_v = tables_.at(layout_version);

  for (layout_version_t i = tuple_version; i <= layout_version && out_buffer->NumTuples() < out_buffer->MaxTuples();
       i++) {
    auto tuple_v = tables_.at(i);
    AttrSizeMap size_map;
    if (AlignHeaderToVersion(out_buffer, tuple_v, desired_v, &ori_header[0], &size_map)) missing = true;

    if (i != tuple_version) *start_pos = tuple_v.data_table_->begin();
    tuple_v.data_table_->IncrementalScan(txn, start_pos, out_buffer, filled);
    filled = out_buffer->NumTuples();
    // copy back the original header
    std::memcpy(out_buffer->ColumnIds(), ori_header, sizeof(col_id_t) * out_buffer->NumColumns());
  }

  if (missing) {
    for (uint32_t idx = 0; idx < out_buffer->NumTuples(); idx++) {
      ProjectedColumns::RowView row = out_buffer->InterpretAsRow(idx);
      FillMissingColumns(&row, desired_v);
    }
  }
}

// return false if it is null
static bool PeekValue(const type::TransientValue &transient_val, byte *value_output) {
  // Value output should be zero filled before calling
  if (transient_val.Null()) {
    // NullToSql(&expr) produces a NULL of expr's type.
    return false;
  }

  switch (transient_val.Type()) {
    case type::TypeId::BOOLEAN: {
      auto val = type::TransientValuePeeker::PeekBoolean(transient_val);
      memcpy(value_output, &val, type::TypeUtil::GetTypeSize(transient_val.Type()));
      break;
    }
    case type::TypeId::TINYINT: {
      auto val = type::TransientValuePeeker::PeekTinyInt(transient_val);
      memcpy(value_output, &val, type::TypeUtil::GetTypeSize(transient_val.Type()));
      break;
    }
    case type::TypeId::SMALLINT: {
      auto val = type::TransientValuePeeker::PeekSmallInt(transient_val);
      memcpy(value_output, &val, type::TypeUtil::GetTypeSize(transient_val.Type()));
      break;
    }
    case type::TypeId::INTEGER: {
      auto val = type::TransientValuePeeker::PeekInteger(transient_val);
      memcpy(value_output, &val, type::TypeUtil::GetTypeSize(transient_val.Type()));
      break;
    }
    case type::TypeId::BIGINT: {
      auto val = type::TransientValuePeeker::PeekBigInt(transient_val);
      memcpy(value_output, &val, type::TypeUtil::GetTypeSize(transient_val.Type()));
      break;
    }
    case type::TypeId::DATE: {
      // TODO(Schema-Change): find a way to handle it without codegen
      //      auto val = type::TransientValuePeeker::PeekDate(transient_val);
      //      auto ymd = util::TimeConvertor::YMDFromDate(val);
      //      auto year = static_cast<int32_t>(ymd.year());
      //      auto month = static_cast<uint32_t>(ymd.month());
      //      auto day = static_cast<uint32_t>(ymd.day());
      //      return DateToSql(year, month, day);
      break;
    }
    case type::TypeId::TIMESTAMP: {
      auto val = type::TransientValuePeeker::PeekTimestamp(transient_val);
      auto julian_usec = util::TimeConvertor::ExtractJulianMicroseconds(val);
      memcpy(value_output, &julian_usec, type::TypeUtil::GetTypeSize(transient_val.Type()));
      break;
    }
    case type::TypeId::DECIMAL: {
      auto val = type::TransientValuePeeker::PeekDecimal(transient_val);
      memcpy(value_output, &val, type::TypeUtil::GetTypeSize(transient_val.Type()));
      break;
    }
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY: {
      auto val = terrier::type::TransientValuePeeker::PeekVarChar(transient_val);
      memcpy(value_output, &val, val.size());
      break;
    }
    default:
      // TODO(Amadou): Add support for these types.
      TERRIER_ASSERT(false, "Should not peek on given type!");
  }
  return true;
}

template <class RowType>
void SqlTable::FillMissingColumns(RowType *out_buffer, const DataTableVersion &desired_version) const {
  const auto col_ids = out_buffer->ColumnIds();
  for (uint16_t i = 0; i < out_buffer->NumColumns(); i++) {
    if (out_buffer->AccessWithNullCheck(i) == nullptr) {
      // TODO(Schema-Change): Only handle constant value default
      const common::ManagedPointer<const parser::AbstractExpression> default_val =
          desired_version.default_value_map_.at(col_ids[i]);
      if (default_val->GetExpressionType() != parser::ExpressionType::VALUE_CONSTANT) continue;

      auto default_const = default_val.CastManagedPointerTo<const parser::ConstantValueExpression>()->GetValue();
      auto col_oid = desired_version.column_id_to_oid_map_.at(col_ids[i]);
      auto value_size = desired_version.schema_->GetColumn(col_oid).AttrSize();

      // zero out the temporary buffer before peeking
      byte output[value_size];
      memset(output, 0, value_size);

      if (PeekValue(default_const, &(output[0]))) {
        StorageUtil::CopyWithNullCheck(output, out_buffer, desired_version.schema_->GetColumn(col_oid).AttrSize(), i);
        out_buffer->SetNotNull(i);
      }
    }
  }
}

template void SqlTable::FillMissingColumns<ProjectedRow>(ProjectedRow *out_buffer,
                                                         const DataTableVersion &desired_version) const;
template void SqlTable::FillMissingColumns<ProjectedColumns::RowView>(ProjectedColumns::RowView *out_buffer,
                                                                      const DataTableVersion &desired_version) const;

template <class RowType>
bool SqlTable::AlignHeaderToVersion(RowType *const out_buffer, const DataTableVersion &tuple_version,
                                    const DataTableVersion &desired_version, col_id_t *cached_ori_header, AttrSizeMap *const size_map) const {
  bool missing_col = false;
  // reserve the original header, aka intended column ids'
  std::memcpy(cached_ori_header, out_buffer->ColumnIds(), sizeof(col_id_t) * out_buffer->NumColumns());

  // for each column id in the intended version of datatable, change it to match the current schema version
  for (uint16_t i = 0; i < out_buffer->NumColumns(); i++) {
    auto col_id =  out_buffer->ColumnIds()[i];
    TERRIER_ASSERT(col_id != VERSION_POINTER_COLUMN_ID,
                   "Output buffer should not read the version pointer column.");
    TERRIER_ASSERT(desired_version.column_id_to_oid_map_.count(col_id) > 0, "col_id missing");
    // Mark missing columns
    catalog::col_oid_t col_oid = desired_version.column_id_to_oid_map_.at(col_id);
    if (tuple_version.column_oid_to_id_map_.count(col_oid) > 0) {
      auto tuple_col_id = tuple_version.column_oid_to_id_map_.at(col_oid);
      out_buffer->ColumnIds()[i] = tuple_col_id;

      // If the physical stored attr has a larger size, we cannot copy the attribute with its size stored in the
      // tupleaccessor, but with explicit smaller size of the desired projectedrow's attribute
      // auto tuple_attr_size = tuple_version.layout_.AttrSize(tuple_col_id);
      // auto pr_attr_size = desired_version.layout_.AttrSize(col_id);
      // size_map->insert({col_id, std::make_pair(tuple_col_id, desired_version.layout_.AttrSize(col_id))});
    } else {
      missing_col = true;
      out_buffer->ColumnIds()[i] = IGNORE_COLUMN_ID;
    }

  }
  return missing_col;
}

template bool SqlTable::AlignHeaderToVersion<ProjectedRow>(ProjectedRow *const out_buffer,
                                                           const DataTableVersion &tuple_version,
                                                           const DataTableVersion &desired_version,
                                                           col_id_t *cached_ori_header, AttrSizeMap *const size_map) const;
template bool SqlTable::AlignHeaderToVersion<ProjectedColumns::RowView>(ProjectedColumns::RowView *const out_buffer,
                                                                        const DataTableVersion &tuple_version,
                                                                        const DataTableVersion &desired_version,
                                                                        col_id_t *cached_ori_header, AttrSizeMap *const size_map) const;

SqlTable::DataTableVersion SqlTable::CreateTable(
    const terrier::common::ManagedPointer<const terrier::catalog::Schema> schema,
    const terrier::common::ManagedPointer<terrier::storage::BlockStore> store,
    terrier::storage::layout_version_t version) {
  // Begin with the NUM_RESERVED_COLUMNS in the attr_sizes
  std::vector<uint16_t> attr_sizes;
  attr_sizes.reserve(NUM_RESERVED_COLUMNS + schema->GetColumns().size());

  for (uint8_t i = 0; i < NUM_RESERVED_COLUMNS; i++) {
    attr_sizes.emplace_back(8);
  }

  TERRIER_ASSERT(attr_sizes.size() == NUM_RESERVED_COLUMNS,
                 "attr_sizes should be initialized with NUM_RESERVED_COLUMNS elements.");

  for (const auto &column : schema->GetColumns()) {
    attr_sizes.push_back(column.AttrSize());
  }

  auto offsets = storage::StorageUtil::ComputeBaseAttributeOffsets(attr_sizes, NUM_RESERVED_COLUMNS);

  ColumnOidToIdMap col_oid_to_id;
  ColumnIdToOidMap col_id_to_oid;
  DefaultValueMap default_value_map;
  // Build the map from Schema columns to underlying columns
  for (const auto &column : schema->GetColumns()) {
    auto default_value = column.StoredExpression();
    switch (column.AttrSize()) {
      case VARLEN_COLUMN:
        if (default_value != nullptr) default_value_map[col_id_t(offsets[0])] = default_value;
        col_id_to_oid[col_id_t(offsets[0])] = column.Oid();
        col_oid_to_id[column.Oid()] = col_id_t(offsets[0]++);
        break;
      case 8:
        if (default_value != nullptr) default_value_map[col_id_t(offsets[1])] = default_value;
        col_id_to_oid[col_id_t(offsets[1])] = column.Oid();
        col_oid_to_id[column.Oid()] = col_id_t(offsets[1]++);
        break;
      case 4:
        if (default_value != nullptr) default_value_map[col_id_t(offsets[2])] = default_value;
        col_id_to_oid[col_id_t(offsets[2])] = column.Oid();
        col_oid_to_id[column.Oid()] = col_id_t(offsets[2]++);
        break;
      case 2:
        if (default_value != nullptr) default_value_map[col_id_t(offsets[3])] = default_value;
        col_id_to_oid[col_id_t(offsets[3])] = column.Oid();
        col_oid_to_id[column.Oid()] = col_id_t(offsets[3]++);
        break;
      case 1:
        if (default_value != nullptr) default_value_map[col_id_t(offsets[4])] = default_value;
        col_id_to_oid[col_id_t(offsets[4])] = column.Oid();
        col_oid_to_id[column.Oid()] = col_id_t(offsets[4]++);
        break;
      default:
        throw std::runtime_error("unexpected switch case value");
    }
  }

  auto layout = storage::BlockLayout(attr_sizes);
  return {
      new DataTable(block_store_, layout, version), layout, col_oid_to_id, col_id_to_oid, schema, default_value_map};
}

std::vector<col_id_t> SqlTable::ColIdsForOids(const std::vector<catalog::col_oid_t> &col_oids,
                                              layout_version_t layout_version) const {
  TERRIER_ASSERT(!col_oids.empty(), "Should be used to access at least one column.");
  std::vector<col_id_t> col_ids;

  // Build the input to the initializer constructor
  for (const catalog::col_oid_t col_oid : col_oids) {
    TERRIER_ASSERT(tables_.at(layout_version).column_oid_to_id_map_.count(col_oid) > 0,
                   "Provided col_oid does not exist in the table.");
    const col_id_t col_id = tables_.at(layout_version).column_oid_to_id_map_.at(col_oid);
    col_ids.push_back(col_id);
  }

  return col_ids;
}

ProjectionMap SqlTable::ProjectionMapForOids(const std::vector<catalog::col_oid_t> &col_oids,
                                             layout_version_t layout_version) {
  // Resolve OIDs to storage IDs
  //  auto col_ids = ColIdsForOids(col_oids);

  // Use std::map to effectively sort OIDs by their corresponding ID
  std::map<col_id_t, catalog::col_oid_t> inverse_map;
  TERRIER_ASSERT(!col_oids.empty(), "Should be used to access at least one column.");
  // Build the input to the initializer constructor
  for (const catalog::col_oid_t col_oid : col_oids) {
    TERRIER_ASSERT(tables_.at(layout_version).column_oid_to_id_map_.count(col_oid) > 0,
                   "Provided col_oid does not exist in the table.");
    const col_id_t col_id = tables_.at(layout_version).column_oid_to_id_map_.at(col_oid);
    inverse_map[col_id] = col_oid;
  }

  // for (uint16_t i = 0; i < col_oids.size(); i++) inverse_map[col_ids[i]] = col_oids[i];

  // Populate the projection map using the in-order iterator on std::map
  // TODO(Schema-Change): Does this actually retain ordering?
  ProjectionMap projection_map;
  uint16_t i = 0;
  for (auto &iter : inverse_map) projection_map[iter.second] = i++;

  return projection_map;
}

catalog::col_oid_t SqlTable::OidForColId(const col_id_t col_id, layout_version_t layout_version) const {
  //  const auto oid_to_id = std::find_if(table_.column_map_.cbegin(), table_.column_map_.cend(),
  //                                      [&](const auto &oid_to_id) -> bool { return oid_to_id.second == col_id; });
  //  return oid_to_id->first;
  return tables_.at(layout_version).column_id_to_oid_map_.at(col_id);
}

}  // namespace terrier::storage
