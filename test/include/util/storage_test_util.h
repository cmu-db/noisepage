#pragma once
#include <random>
#include <vector>
#include "storage/storage_defs.h"
#include "common/test_util.h"

namespace terrier::testutil {

// Returns a random layout that is guaranteed to be valid.
template<typename Random>
storage::BlockLayout RandomLayout(Random &generator, uint16_t max_cols = UINT16_MAX) {
  PELOTON_ASSERT(max_cols > 1);
  // We probably won't allow tables with 0 columns
  uint16_t num_attrs = std::uniform_int_distribution<uint16_t>(1, max_cols)(generator);
  std::vector<uint8_t> possible_attr_sizes{1, 2, 4, 8}, attr_sizes(num_attrs);
  for (uint16_t i = 0; i < num_attrs; i++)
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

///////////////////////////////////////////////////////////

template<typename Random>
storage::ProjectedRow *GenerateDelta(const storage::BlockLayout &layout,
                                     const std::vector<uint16_t> &col_ids,
                                     Random &generator,
                                     double null_bias = 0.1) {

  uint32_t contents_size = storage::ProjectedRow::RowSize(layout, col_ids);
  byte *contents = new byte[contents_size];

  storage::ProjectedRow *delta = storage::ProjectedRow::InitializeProjectedRow(layout, col_ids, contents);

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

template<typename Random>
storage::ProjectedRow *RandomDelta(const storage::BlockLayout &layout, Random &generator, double null_bias = 0.1) {

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

  return GenerateDelta<Random>(layout, col_ids, generator, null_bias);
}

template<typename Random>
storage::ProjectedRow *RandomTuple(const storage::BlockLayout &layout, Random &generator, double null_bias = 0.1) {

  std::vector<uint16_t> col_ids(layout.num_cols_ - 1);
  // Add all of the column ids from the layout to the projection list
  // 0 is version vector so we skip it
  for (uint16_t col = 1; col < layout.num_cols_; col++) {
    col_ids.push_back(col);
  }

  return GenerateDelta<Random>(layout, col_ids, generator, null_bias);
}
}
