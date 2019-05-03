#include "execution/sql/table.h"

#include <iostream>
#include <utility>

#include "execution/sql/catalog.h"

// TODO(siva): Hack! Fix me!
i32 current_partition = -1;

namespace tpl::sql {

// ---------------------------------------------------------
// Table
// ---------------------------------------------------------

void Table::Insert(Block &&block) {
  // Sanity check
  TPL_ASSERT(block.num_cols() == num_columns(), "Column count mismatch");
  for (u32 i = 0; i < num_columns(); i++) {
    TPL_ASSERT(schema().GetColumnInfo(i)->type.Equals(block.GetColumnData(i)->type()), "Column type mismatch");
  }

  num_tuples_ += block.num_tuples();
  blocks_.emplace_back(std::move(block));
}

namespace {

void DumpColValue(std::ostream &os, const Type &type, const ColumnSegment &col, u32 row_idx) {
  switch (type.type_id()) {
    case TypeId::Boolean: {
      break;
    }
    case TypeId::SmallInt: {
      if (type.nullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << col.TypedAccessAt<i16>(row_idx);
      }
      break;
    }
    case TypeId::Integer: {
      if (type.nullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << col.TypedAccessAt<i32>(row_idx);
      }
      break;
    }
    case TypeId::BigInt: {
      if (type.nullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << col.TypedAccessAt<i64>(row_idx);
      }
      break;
    }
    case TypeId::Decimal:
    case TypeId::Date:
    case TypeId::Char:
    case TypeId::Varchar: {
      break;
    }
  }
}

}  // namespace

void Table::Dump(std::ostream &os) const {
  const auto &cols_meta = schema().columns();
  for (const auto &block : blocks_) {
    for (u32 row_idx = 0; row_idx < block.num_tuples(); row_idx++) {
      for (u32 col_idx = 0; col_idx < cols_meta.size(); col_idx++) {
        if (col_idx != 0) {
          os << ", ";
        }
        const auto *col_vector = block.GetColumnData(col_idx);
        DumpColValue(os, cols_meta[col_idx].type, *col_vector, row_idx);
      }
      os << "\n";
    }
  }
}

// ---------------------------------------------------------
// Table Block Iterator
// ---------------------------------------------------------

TableBlockIterator::TableBlockIterator(u16 table_id) noexcept
    : table_id_(table_id), table_(nullptr), curr_block_(nullptr) {}

bool TableBlockIterator::Init() {
  // Lookup the table
  const Catalog *catalog = Catalog::Instance();
  table_ = catalog->LookupTableById(static_cast<TableId>(table_id_));

  // If the table wasn't found, we didn't initialize
  if (table_ == nullptr) {
    return false;
  }

  // Setup the block position boundaries
  curr_block_ = nullptr;
  pos_ = table_->begin();
  end_ = table_->end();
  return true;
}

bool TableBlockIterator::Advance() {
  if (pos_ == end_) {
    return false;
  }

  curr_block_ = &*pos_;
  ++pos_;
  return true;
}

}  // namespace tpl::sql
