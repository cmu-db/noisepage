#include <random>
#include <vector>
#include <memory>

#include "execution/tpl_test.h"  // NOLINT

#include "execution/sql/value.h"
#include "execution/sql/value_functions.h"
#include "execution/util/timer.h"

namespace tpl::sql::test {

class RealFunctionsTests : public TplTest {};

TEST_F(RealFunctionsTests, TrigFunctionsTest) {
  std::vector<double> inputs, arc_inputs;

  std::mt19937 gen;
  std::uniform_real_distribution dist(1.0, 1000.0);
  std::uniform_real_distribution arc_dist(-1.0, 1.0);
  for (u32 i = 0; i < 100; i++) {
    inputs.push_back(dist(gen));
    arc_inputs.push_back(arc_dist(gen));
  }

#define CHECK_FUNC(TPL_FUNC, C_FUNC)          \
  do {                                        \
    Real arg(input);                          \
    Real ret(0.0);                            \
    TPL_FUNC::Execute<false>(&arg, &ret);     \
                                              \
    EXPECT_FALSE(ret.is_null);                \
    EXPECT_DOUBLE_EQ(C_FUNC(input), ret.val); \
  } while (false)

  // Check some of the trig functions on all inputs
  for (const auto input : inputs) {
    CHECK_FUNC(Cos, std::cos);
    CHECK_FUNC(Cot, cotan);
    CHECK_FUNC(Sin, std::sin);
    CHECK_FUNC(Tan, std::tan);
  }

  for (const auto input : arc_inputs) {
    CHECK_FUNC(ACos, std::acos);
    CHECK_FUNC(ASin, std::asin);
    CHECK_FUNC(ATan, std::atan);
  }

#undef CHECK_FUNC
}

TEST_F(RealFunctionsTests, DISABLED_PerfTrigFunctionsTest) {
  constexpr const u32 num_elems = 1000000;
  std::vector<std::unique_ptr<Real>> inputs(num_elems), arc_inputs(num_elems);

  std::mt19937 gen;
  std::uniform_real_distribution dist(1.0, 1000.0);
  std::uniform_real_distribution arc_dist(-1.0, 1.0);
  for (u32 i = 0; i < num_elems; i++) {
    inputs[i] = std::make_unique<Real>(dist(gen));
    arc_inputs[i] = std::make_unique<Real>(arc_dist(gen));
  }

  for (auto sel : {1, 2, 3, 4, 5, 6, 7, 8}) {
    // Setup nulls
    std::uniform_int_distribution null_dist(0, 9);
    for (u32 i = 0; i < num_elems; i++) {
      inputs[i]->is_null = (null_dist(gen) < sel);
      arc_inputs[i]->is_null = (null_dist(gen) < sel);
    }

#define BENCH_FUNC(TPL_FUNC, C_FUNC)                                                                         \
  do {                                                                                                       \
    util::Timer<std::milli> timer;                                                                           \
    timer.Start();                                                                                           \
    double x1 = 0.0, x2 = 0.0;                                                                               \
    for (const auto &input : inputs) {                                                                       \
      Real arg(input->val);                                                                                  \
      Real ret(0.0);                                                                                         \
      TPL_FUNC::Execute<false>(&arg, &ret);                                                                  \
      x1 += ret.is_null + ret.val;                                                                           \
    }                                                                                                        \
    timer.Stop();                                                                                            \
    double bf_ms = timer.elapsed();                                                                          \
    timer.Start();                                                                                           \
    for (const auto &input : inputs) {                                                                       \
      Real arg(input->val);                                                                                  \
      Real ret(0.0);                                                                                         \
      TPL_FUNC::Execute<true>(&arg, &ret);                                                                   \
      x2 += ret.is_null + ret.val;                                                                           \
    }                                                                                                        \
    timer.Stop();                                                                                            \
    double branch_ms = timer.elapsed();                                                                      \
    std::cerr << #TPL_FUNC << ": BF=" << bf_ms << "(" << x1 << "), Branch=" << branch_ms << "(" << x2 << ")" \
              << std::endl;                                                                                  \
  } while (false)

    std::cout << "=== Sel: " << sel << " ===" << std::endl;
    BENCH_FUNC(Cos, std::cos);
    BENCH_FUNC(Cot, cotan);
    BENCH_FUNC(Sin, std::sin);
    BENCH_FUNC(Tan, std::tan);
  }
#undef BENCH_FUNC
}

}  // namespace tpl::sql::test
