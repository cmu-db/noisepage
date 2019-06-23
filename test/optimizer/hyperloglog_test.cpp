#include <random>

#include "gtest/gtest.h"
#include "loggers/optimizer_logger.h"
#include "optimizer/statistics/hyperloglog.h"

#include "util/test_harness.h"

namespace terrier::optimizer {

class HyperLogLogTests : public TerrierTest {};

// NOLINTNEXTLINE
TEST_F(HyperLogLogTests, SimpleTest) {
  HyperLogLog<long> hll{9};

  const uint64_t kIterations = 1000000;
  const uint64_t kTrueCardinality = 100;
  for (uint64_t i = 0; i < kIterations; ++i) {
    long key = static_cast<long>(i % kTrueCardinality);
    hll.Update(key);
  }
  const uint64_t estimate = hll.EstimateCardinality();

  std::cout << "actual cardinality:    " << kTrueCardinality << std::endl;
  std::cout << "estimated cardinality: " << estimate << std::endl;
}

// 100k values with 10k distinct.
// NOLINTNEXTLINE
TEST_F(HyperLogLogTests, Dataset1Test) {
  HyperLogLog<long> hll{9};
  int threshold = 100000;
  int ratio = 10;
  double error = hll.RelativeError();
  for (int i = 1; i <= threshold; i++) {
    auto v = i / ratio;
    hll.Update(v);
  }
  auto actual = threshold / ratio;
  auto estimated  = hll.EstimateCardinality();
  OPTIMIZER_LOG_TRACE("Estimated[{0}] <-> Actual[{1}]", estimated, actual);

  // Check to make sure that our estimate is in within the expected error range.
  EXPECT_LE(estimated, threshold / ratio * (1 + error));
  EXPECT_GE(estimated, threshold / ratio * (1 - error));
  UNUSED_ATTRIBUTE double true_error =
      (threshold * 1.0 - static_cast<int>(estimated)) / static_cast<double>(estimated);
  OPTIMIZER_LOG_TRACE("Estimated cardinality is [{0}] times of real value", true_error);
}

//// 100k values with 1k distinct.
//// This case HLL does not perform very well.
//// NOLINTNEXTLINE
// TEST_F(HyperLogLogTests, Dataset2Test) {
//  HyperLogLog<int> hll{8};
//  int threshold = 100000;
//  int ratio = 100;
//  double error = hll.RelativeError() + 0.05;
//  for (int i = 1; i <= threshold; i++) {
//    auto v = i / ratio;
//    hll.Update(v);
//  }
//  uint64_t cardinality = hll.EstimateCardinality();
//  EXPECT_LE(cardinality, threshold / ratio * (1 + error));
//  EXPECT_GE(cardinality, threshold / ratio * (1 - error));
//}
//
//// 100k values with 100 distinct.
//// NOLINTNEXTLINE
// TEST_F(HyperLogLogTests, Dataset3Test) {
//  HyperLogLog<std::string> hll{8};
//  int threshold = 100000;
//  int ratio = 1000;
//  double error = hll.RelativeError();
//  for (int i = 1; i <= threshold; i++) {
//    auto v = std::to_string(i / ratio);
//    hll.Update(v);
//  }
//  uint64_t cardinality = hll.EstimateCardinality();
//  EXPECT_LE(cardinality, threshold / ratio * (1 + error));
//  EXPECT_GE(cardinality, threshold / ratio * (1 - error));
//}
//
//// 100k values with 100k distinct.
// TEST_F(HyperLogLogTests, Dataset4Test) {
//  HyperLogLog<double> hll{8};
//  int threshold = 100000;
//  int ratio = 1;
//  double error = hll.RelativeError();
//  for (int i = 1; i <= threshold; i++) {
//    auto v = i / ratio;
//    hll.Update(v);
//  }
//  uint64_t cardinality = hll.EstimateCardinality();
//  EXPECT_LE(cardinality, threshold / ratio * (1 + error));
//  EXPECT_GE(cardinality, threshold / ratio * (1 - error));
//}
//
//// HLL performance with different precisions.
//// In general, the higher the precision, the smaller the error.
// TEST_F(HyperLogLogTests, PrecisionTest) {
//  int threshold = 100000;
//  int ratio = 10;
//  HyperLogLog<int> hll_10{10};
//  double error_10 = hll_10.RelativeError() + 0.001;
//  HyperLogLog<int> hll_14{14};
//  double error_14 = hll_14.RelativeError() + 0.001;
//  HyperLogLog<int> hll_4{4};
//  double error_4 = hll_4.RelativeError() + 0.05;  // precision 4 tend to be worse
//  for (int i = 1; i <= threshold; i++) {
//    auto v = i / ratio;
//    hll_4.Update(v);
//    hll_10.Update(v);
//    hll_14.Update(v);
//  }
//  EXPECT_LE(hll_4.EstimateCardinality(), threshold / ratio * (1 + error_4));
//  EXPECT_GE(hll_4.EstimateCardinality(), threshold / ratio * (1 - error_4));
//
//  EXPECT_LE(hll_10.EstimateCardinality(), threshold / ratio * (1 + error_10));
//  EXPECT_GE(hll_10.EstimateCardinality(), threshold / ratio * (1 - error_10));
//
//  EXPECT_LE(hll_14.EstimateCardinality(), threshold / ratio * (1 + error_14));
//  EXPECT_GE(hll_14.EstimateCardinality(), threshold / ratio * (1 - error_14));
//}

// 100M values with 10M distinct. Comment out due to long running time.
// TEST_F(HyperLogLogTests, LargeDatasetTest) {
//   HyperLogLog hll{};
//   int threshold = 100000000;
//   int ratio = 10;
//   double error = hll.RelativeError();
//   for (int i = 1; i <= 100000000; i++) {
//     type::Value v = type::ValueFactory::GetIntegerValue(i / 10);
//     hll.Update(v);
//   }
//   uint64_t cardinality = hll.EstimateCardinality();
//   EXPECT_LE(cardinality, threshold / ratio * (1 + error));
//   EXPECT_GE(cardinality, threshold / ratio * (1 - error));
// }

// Hyperloglog should be able to handle different value types.
// TEST_F(HyperLogLogTests, DataTypeTest) {
//  HyperLogLog<int> hll{};
//  // integer
//  type::Value tiny_int = type::ValueFactory::GetTinyIntValue(1);
//  hll.Update(tiny_int);
//  type::Value timestamp = type::ValueFactory::GetTimestampValue(1493003492);
//  hll.Update(timestamp);
//  // double
//  type::Value decimal = type::ValueFactory::GetDecimalValue(12.9998435);
//  hll.Update(decimal);
//  // string
//  std::string str{"database"};
//  type::Value str_val = type::ValueFactory::GetVarcharValue(str);
//  hll.Update(str_val);
//  // Null
//  type::Value null =
//      type::ValueFactory::GetNullValueByType(type::TypeId::BOOLEAN);
//  hll.Update(str_val);
//
//  hll.EstimateCardinality();
//}

}  // namespace terrier::optimizer
