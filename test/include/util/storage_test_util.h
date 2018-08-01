#pragma once
#include <random>
#include <vector>
#include "storage/storage_defs.h"

namespace terrier::testutil {

template<typename Random, double null_bias = 0.1>
storage::ProjectedRow *GenerateDelta(const storage::BlockLayout &layout, const std::vector<uint16_t> &col_ids, Random &generator) {

  uint32_t contents_size = storage::ProjectedRow::RowSize(layout, col_ids);
  byte *contents = new byte[contents_size];

  storage::ProjectedRow *delta = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, contents_size);

  // For every column in the project list, populate its attribute with random bytes or set to null based on coin flip
  for (uint16_t projection_list_idx = 0; projection_list_idx < delta->NumColumns(); projection_list_idx++) {
    uint16_t col = delta->ColumnIds()[projection_list_idx];
    std::bernoulli_distribution coin(1 - null_bias);

    if (coin(generator)) {
      FillWithRandomBytes(layout.attr_sizes_[col], delta->AccessForceNotNull(projection_list_idx), generator);
    }
  }

  return delta;
}

template<typename Random, double null_bias = 0.1>
storage::ProjectedRow *RandomDelta(const storage::BlockLayout &layout, Random &generator) {

  // randomly select a number of columns for this delta to contain. Must be at least 1, but shouldn't be num_cols since
  // we exclude the version vector column
  uint16_t num_cols = std::uniform_int_distribution(1, layout.num_cols_ - 1)(generator)

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

  return GenerateDelta<Random, null_bias>(layout, col_ids, generator);
}

template<typename Random, double null_bias = 0.1>
storage::ProjectedRow *RandomTuple(const storage::BlockLayout &layout, Random &generator) {

  std::vector<uint16_t> col_ids(layout.num_cols_ - 1);
  // Add all of the column ids from the layout to the projection list
  // 0 is version vector so we skip it
  for (uint16_t col = 1; col < layout.num_cols_; col++) {
    col_ids.push_back(col);
  }

  return GenerateDelta<Random, null_bias>(layout, col_ids, generator);
}
}
