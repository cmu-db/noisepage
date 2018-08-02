#pragma once
#include <random>
#include <vector>
#include "common/test_util.h"
#include "storage/storage_defs.h"
#include "storage/storage_utils.h"

namespace terrier::testutil {

// Returns a random layout that is guaranteed to be valid.
template<typename Random>
storage::BlockLayout RandomLayout(Random &generator, uint16_t max_cols = UINT16_MAX) {
  PELOTON_ASSERT(max_cols > 1);
  // We probably won't allow tables with fewer than 2 columns
  uint16_t num_attrs = std::uniform_int_distribution<uint16_t>(2, max_cols)(generator);
  std::vector<uint8_t> possible_attr_sizes{1, 2, 4, 8}, attr_sizes(num_attrs);
  attr_sizes[0] = 8;
  for (uint16_t i = 1; i < num_attrs; i++)
    attr_sizes[i] = *testutil::UniformRandomElement(possible_attr_sizes, generator);
  return {num_attrs, attr_sizes};
}

// Fill the given location with the specified amount of random bytes, using the
// given generator as a source of randomness.
template<typename Random>
void FillWithRandomBytes(uint32_t num_bytes, byte *out, Random &generator) {
  std::uniform_int_distribution<uint8_t> dist(0, UINT8_MAX);
  for (uint32_t i = 0; i < num_bytes; i++) out[i] = static_cast<byte>(dist(generator));
}

template<typename Random>
void GenerateRandomRow(storage::ProjectedRow *row, const storage::BlockLayout &layout, Random &generator,
                       const double null_bias = 0.1) {
  // For every column in the project list, populate its attribute with random bytes or set to null based on coin flip
  for (uint16_t projection_list_idx = 0; projection_list_idx < row->NumColumns(); projection_list_idx++) {
    uint16_t col = row->ColumnIds()[projection_list_idx];
    std::bernoulli_distribution coin(1 - null_bias);

    if (coin(generator)) {
      FillWithRandomBytes(layout.attr_sizes_[col], row->AccessForceNotNull(projection_list_idx), generator);
    }
  }
}

std::vector<uint16_t> ProjectionListAllColumns(const storage::BlockLayout &layout) {
  std::vector<uint16_t> col_ids(layout.num_cols_ - 1u);
  // Add all of the column ids from the layout to the projection list
  // 0 is version vector so we skip it
  for (uint16_t col = 1; col < layout.num_cols_; col++) {
    col_ids[col - 1] = col;
  }
  return col_ids;
}

template<typename Random>
std::vector<uint16_t> ProjectionListRandomColumns(const storage::BlockLayout &layout, Random &generator) {
  // randomly select a number of columns for this delta to contain. Must be at least 1, but shouldn't be num_cols since
  // we exclude the version vector column
  uint16_t num_cols = std::uniform_int_distribution(1, layout.num_cols_ - 1)(generator);

  std::vector<uint16_t> col_ids(num_cols);
  // Add all of the column ids from the layout to the projection list
  // 0 is version vector so we skip it
  for (uint16_t col = 1; col < layout.num_cols_; col++) {
    col_ids.push_back(col);
  }

  // permute the column ids for our random delta
  std::shuffle(col_ids.begin(), col_ids.end(), generator);

  // truncate the projection list
  col_ids.resize(num_cols);

  return col_ids;
}

bool ProjectionListEqual(const storage::BlockLayout &layout,
                         const storage::ProjectedRow *one,
                         const storage::ProjectedRow *other) {
  if (one->NumColumns() != other->NumColumns()) return false;
  for (uint16_t projection_list_index = 0; projection_list_index < one->NumColumns(); projection_list_index++) {
    if (one->ColumnIds()[projection_list_index] != other->ColumnIds()[projection_list_index]) return false;
  }

  for (uint16_t projection_list_index = 0; projection_list_index < one->NumColumns(); projection_list_index++) {
    uint8_t attr_size = layout.attr_sizes_[one->ColumnIds()[projection_list_index]];
    const byte *one_content = one->AccessWithNullCheck(projection_list_index);
    const byte *other_content = other->AccessWithNullCheck(projection_list_index);

    if (!((one_content == nullptr && other_content == nullptr)
        || storage::ReadBytes(attr_size, one_content) == storage::ReadBytes(attr_size, other_content)))
      return false;
  }

  return true;
}
}  // namespace terrier::testutil
