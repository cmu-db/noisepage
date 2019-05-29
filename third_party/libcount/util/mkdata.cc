#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <map>
#include <set>
#include <utility>

using std::map;
using std::multimap;

const size_t MIN_PRECISION = 4;
const size_t MAX_PRECISION = 18;

typedef std::pair<double, size_t> Estimate;
typedef std::set<Estimate> Estimates;
typedef std::map<size_t, Estimates> EstimateDataSet;

extern const double rawEstimateData[15][201];
extern const double biasData[15][201];

size_t ArrayLength(const double *arr, size_t maxlen) {
  const double EPSILON = 0.00001;
  const double ZERO = 0.0;
  for (size_t i = 0; i < maxlen; ++i) {
    if (fabs(arr[i] - ZERO) < EPSILON) {
      return i;
    }
  }
  return maxlen;
}

int main(int argc, char *argv[]) {
  // The Estimate dataset is a map; the key is the precision level, the value
  // is an Estimates set that contains a sorted list of all estimates for that
  // precision level.
  EstimateDataSet dataset;

  // For every precision level...
  for (size_t p = MIN_PRECISION; p <= MAX_PRECISION; ++p) {
    // Determine the index into the two dimenensional rawEstimateData array
    // that holds the series of data associated with precision 'p'.
    const size_t p_index = (p - MIN_PRECISION);

    // Determine how many elements are in the series. Most series have 200,
    // but the first few precision levels have fewer.
    const size_t subarray_len = ArrayLength(rawEstimateData[p_index], 201);

    // Construct a set to hold all the estimates and their positions in the
    // array, which is kept sorted in increasing order of the estimate value,
    // with index being the tie-breaker.
    Estimates estimates;
    for (size_t elem = 0; elem < subarray_len; ++elem) {
      estimates.insert(Estimate(rawEstimateData[p_index][elem], elem));
    }

    // Store all sorted estimates into the master dataset map.
    dataset[p] = estimates;
  }

  // Generate C code for the sorted estimate data, written to standard output.

  // Preamble.
  printf("extern const double ESTIMATE_DATA[15][201] = {\n");

  for (size_t p = MIN_PRECISION; p <= MAX_PRECISION; ++p) {
    // Get an alias to the estimates for this precision level.
    const Estimates &estimates = dataset[p];
    assert(estimates.size() > 0);
    printf("  // Precision %lu\n", p);
    printf("  { ");

    Estimates::const_iterator ei = estimates.begin();
    Estimates::const_iterator ee = estimates.end();

    printf("%f", ei->first);
    ++ei;

    for (; ei != ee; ++ei) {
      printf(", %f", ei->first);
    }

    printf(" }");
    if (p != MAX_PRECISION) {
      printf(",");
    }

    printf("\n\n");
  }

  printf("};\n\n");

  // Generate C code for the sorted bias data, written to standard output.
  
  printf("extern const double BIAS_DATA[15][201] = {\n");

  for (size_t p = MIN_PRECISION; p <= MAX_PRECISION; ++p) {
    const Estimates &estimates = dataset[p];
    assert(estimates.size() > 0);
    printf("  // Precision %lu\n", p);
    printf("  { ");

    Estimates::const_iterator ei = estimates.begin();
    Estimates::const_iterator ee = estimates.end();

    double bias = biasData[p - MIN_PRECISION][ei->second];
    printf("%f", bias);
    ++ei;

    for (; ei != ee; ++ei) {
      double bias = biasData[p - MIN_PRECISION][ei->second];
      printf(", %f", bias);
    }

    printf(" }");
    if (p != MAX_PRECISION) {
      printf(",");
    }
    printf("\n\n");
  }

  printf("};\n\n");

  return 0;
}
