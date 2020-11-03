#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "parser/postgresparser.h"

namespace noisepage {

/**
 * Magic macro for our parser microbencmarks
 */
#define PARSER_BENCHMARK_EXECUTE(QUERIES, TYPE)                                                                       \
  for (const auto &sql : QUERIES) {                                                                                   \
    auto result = parser::PostgresParser::BuildParseTree(sql);                                                        \
    NOISEPAGE_ASSERT(result->GetStatement(0).CastManagedPointerTo<TYPE>() != nullptr, "Failed to get ##TYPE object"); \
  }

class ParserBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    // We only need to bring up the parser for these benchmarks

    // -------------------------------
    // SELECT
    // -------------------------------

    // SIMPLE
    // clang-format off
    selects_simple_ = {
        "SELECT * FROM foo WHERE id = 123;",
        "SELECT a, b, c, d FROM foo WHERE e = 123;",
        "SELECT col + 1 AS xxx FROM foo WHERE noisepage = 'dangerous';",
        "SELECT COUNT(DISTINCT id) FROM foo WHERE wu_tang_clan = 'nuthin to fuck wit';"
    };
    // clang-format on

    // COMPLEX
    {
      std::ostringstream os;
      for (int i = 0; i < 10000; i++) {
        os << (i != 0 ? " OR " : "") << "my_column = '" << i << "'";
      }
      selects_complex_ = {"SELECT * FROM foo INNER JOIN bar ON " + os.str() + ";"};
    }

    // -------------------------------
    // UPDATE
    // -------------------------------

    // SIMPLE
    // clang-format off
    updates_simple_ = {
        "UPDATE xxx SET a = 999 WHERE id = 123;",
        "UPDATE xxx SET val = val + 1 WHERE id = 123;",
        "UPDATE xxx SET col0 = 1, col1 = 2, col3 = 3 WHERE id = 123 AND kb = 'awesome';",
        "UPDATE xxx SET col0 = col1 + col2 + col3 WHERE col0 != col1;"
    };
    // clang-format on

    // COMPLEX
    {
      std::ostringstream os;
      for (int i = 0; i < 1000; i++) {
        os << (i != 0 ? " + " : "") << i;
      }
      updates_complex_ = {"UPDATE xxx SET val = " + os.str() + " WHERE val = 123;"};
    }

    // -------------------------------
    // INSERT
    // -------------------------------

    // SIMPLE
    // clang-format off
    inserts_simple_ = {
        "INSERT INTO xxx VALUES (1, 2, 3)",
        "INSERT INTO xxx (col1, col2, col3, col4) VALUES (DEFAULT, NULL, 3, 4);",
        "INSERT INTO xxx (col1, col2, col3, col4) VALUES ('DEFAULT', 'NULL', DEFAULT, NULL);",
        "INSERT INTO xxx (\"col1XXXXXXXXXXXXXXXXXX\") VALUES ('TKBM');",
    };
    // clang-format on

    // COMPLEX
    {
      std::ostringstream os1;
      std::ostringstream os2;
      for (int i = 0; i < 10000; i++) {
        os1 << (i != 0 ? ", " : "") << "\"col" << i << "\"";
        os2 << (i != 0 ? ", " : "") << i;
      }
      inserts_complex_ = {"INSERT INTO xxx (" + os1.str() + ") VALUES (" + os2.str() + ");"};
    }

    // -------------------------------
    // DELETE
    // -------------------------------

    // SIMPLE
    // clang-format off
    deletes_simple_ = {
        "DELETE FROM xxx WHERE id = 123;",
        "DELETE FROM xxx WHERE id IS NULL;",
        "DELETE FROM xxx WHERE id = 123 AND noisepage = 'dangerous';",
        "DELETE FROM xxx WHERE col0 != col1;"
    };
    // clang-format on

    // COMPLEX
    std::ostringstream os;
    for (int i = 0; i < 10000; i++) {
      os << (i != 0 ? " OR " : "") << "my_column = '" << i << "'";
    }
    deletes_complex_ = {"DELETE FROM xxx WHERE " + os.str() + ";"};
  }

  void TearDown(const benchmark::State &state) final {
    // Nothing to do, nothing to see...
  }

  std::vector<std::string> selects_simple_;
  std::vector<std::string> selects_complex_;
  std::vector<std::string> updates_simple_;
  std::vector<std::string> updates_complex_;
  std::vector<std::string> inserts_simple_;
  std::vector<std::string> inserts_complex_;
  std::vector<std::string> deletes_simple_;
  std::vector<std::string> deletes_complex_;
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ParserBenchmark, SelectsSimple)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    PARSER_BENCHMARK_EXECUTE(selects_simple_, parser::SelectStatement);
  }
  state.SetItemsProcessed(state.iterations() * selects_simple_.size());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ParserBenchmark, SelectsComplex)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    PARSER_BENCHMARK_EXECUTE(selects_complex_, parser::SelectStatement);
  }
  state.SetItemsProcessed(state.iterations() * selects_complex_.size());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ParserBenchmark, UpdatesSimple)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    PARSER_BENCHMARK_EXECUTE(updates_simple_, parser::UpdateStatement);
  }
  state.SetItemsProcessed(state.iterations() * updates_simple_.size());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ParserBenchmark, UpdatesComplex)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    PARSER_BENCHMARK_EXECUTE(updates_complex_, parser::UpdateStatement);
  }
  state.SetItemsProcessed(state.iterations() * updates_complex_.size());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ParserBenchmark, InsertsSimple)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    PARSER_BENCHMARK_EXECUTE(inserts_simple_, parser::InsertStatement);
  }
  state.SetItemsProcessed(state.iterations() * inserts_simple_.size());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ParserBenchmark, InsertsComplex)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    PARSER_BENCHMARK_EXECUTE(inserts_complex_, parser::InsertStatement);
  }
  state.SetItemsProcessed(state.iterations() * inserts_complex_.size());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ParserBenchmark, DeletesSimple)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    PARSER_BENCHMARK_EXECUTE(deletes_simple_, parser::DeleteStatement);
  }
  state.SetItemsProcessed(state.iterations() * deletes_simple_.size());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ParserBenchmark, DeletesComplex)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    PARSER_BENCHMARK_EXECUTE(deletes_complex_, parser::DeleteStatement);
  }
  state.SetItemsProcessed(state.iterations() * deletes_complex_.size());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ParserBenchmark, NOOPs)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto result = parser::PostgresParser::BuildParseTree(";");
    NOISEPAGE_ASSERT(result->GetStatements().empty(), "Unexpected return result for NOOP");
  }
  state.SetItemsProcessed(state.iterations());
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(ParserBenchmark, SelectsSimple)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(ParserBenchmark, SelectsComplex)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(ParserBenchmark, UpdatesSimple)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(ParserBenchmark, UpdatesComplex)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(ParserBenchmark, InsertsSimple)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(ParserBenchmark, InsertsComplex)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(ParserBenchmark, DeletesSimple)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(ParserBenchmark, DeletesComplex)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(ParserBenchmark, NOOPs)->Unit(benchmark::kNanosecond);
// clang-format on

}  // namespace noisepage
