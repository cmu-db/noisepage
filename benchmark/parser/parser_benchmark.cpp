#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "parser/postgresparser.h"

namespace terrier {

/**
 * Magic macro for our parser microbencmarks
 */
#define PARSER_BENCHMARK_EXECUTE(QUERIES, TYPE)                        \
  for (const auto &sql : QUERIES) {                                   \
    auto result = parser_.BuildParseTree(sql);                         \
    auto stmt = result.GetStatement(0).CastManagedPointerTo<TYPE>(); \
    TERRIER_ASSERT(stmt != nullptr, "Failed to get ##TYPE object");    \
  }

class ParserBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    // We only need to bring up the parser for these benchmarks

    // SIMPLE SELECT
    // clang-format off
    simple_select_ = {
        "SELECT * FROM foo",
        "SELECT a, b, c, d FROM foo WHERE e = 123",
        "SELECT COUNT(DISTINCT id) FROM foo WHERE wu_tang_clan = 'nuthin to fuck wit'"
    };
    // clang-format on

    // LARGE SELECT
    std::ostringstream os;
    for (int i = 0; i < 10000; i++) {
      os << (i != 0 ? " OR " : "") << "my_column = '" << i << "'";
    }
    large_select_ = { "SELECT * FROM foo WHERE " + os.str() + ";" };
  }

  void TearDown(const benchmark::State &state) final {
    // Nothing to do, nothing to see...
  }

  parser::PostgresParser parser_;
  std::vector<std::string> simple_select_;
  std::vector<std::string> large_select_;
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ParserBenchmark, SimpleSelect)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    PARSER_BENCHMARK_EXECUTE(simple_select_, parser::SelectStatement);
  }
  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ParserBenchmark, LargeSelect)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    PARSER_BENCHMARK_EXECUTE(large_select_, parser::SelectStatement);
  }
  state.SetItemsProcessed(state.iterations());
}

// Parser Benchmarks!
BENCHMARK_REGISTER_F(ParserBenchmark, SimpleSelect)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(ParserBenchmark, LargeSelect)->Unit(benchmark::kNanosecond);

}  // namespace terrier
