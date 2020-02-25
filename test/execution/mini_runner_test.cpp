#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/ast/ast_dump.h"
#include "execution/compiler/compiler.h"
#include "execution/compiler/expression_util.h"
#include "execution/compiler/output_checker.h"
#include "execution/compiler/output_schema_util.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/output.h"
#include "execution/executable_query.h"
#include "execution/execution_util.h"
#include "execution/sema/sema.h"
#include "execution/sql/value.h"
#include "execution/sql_test.h"  // NOLINT
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/bytecode_module.h"
#include "execution/vm/llvm_engine.h"
#include "execution/vm/module.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/delete_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/index_join_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

namespace terrier::execution::compiler::test {

/**
 * Taken from Facebook's folly library.
 * DoNotOptimizeAway helps to ensure that a certain variable
 * is not optimized away by the compiler.
 */

template <typename T>
struct DoNotOptimizeAwayNeedsIndirect {
  using Decayed = typename std::decay<T>::type;

  // First two constraints ensure it can be an "r" operand.
  // std::is_pointer check is because callers seem to expect that
  // doNotOptimizeAway(&x) is equivalent to doNotOptimizeAway(x).
  constexpr static bool value = !std::is_trivially_copyable<Decayed>::value ||
      sizeof(Decayed) > sizeof(long) || std::is_pointer<Decayed>::value;
};

template <typename T>
auto DoNotOptimizeAway(const T& datum) -> typename std::enable_if<
    !DoNotOptimizeAwayNeedsIndirect<T>::value>::type {
  // The "r" constraint forces the compiler to make datum available
  // in a register to the asm block, which means that it must have
  // computed/loaded it.  We use this path for things that are <=
  // sizeof(long) (they have to fit), trivial (otherwise the compiler
  // doesn't want to put them in a register), and not a pointer (because
  // DoNotOptimizeAway(&foo) would otherwise be a foot gun that didn't
  // necessarily compute foo).
  //
  // An earlier version of this method had a more permissive input operand
  // constraint, but that caused unnecessary variation between clang and
  // gcc benchmarks.
  asm volatile("" ::"r"(datum));
}

class MiniRunnersTest : public SqlBasedTest {
 public:
  void SetUp() override {
    SqlBasedTest::SetUp();
    // Make the test tables
    auto exec_ctx = MakeExecCtx();
    sql::TableGenerator table_generator{exec_ctx.get(), BlockStore(), NSOid()};
    table_generator.GenerateTestTables(false);
  }

  static constexpr vm::ExecutionMode MODE = vm::ExecutionMode::Interpret;
};

TEST_F(MiniRunnersTest, OpIntegerAdd) {
  uint32_t sum = 0;
  for (size_t i = 0; i < 100000000; i++) {
    sum += i;
  }
  std::cout << sum << "\n";
  DoNotOptimizeAway(sum);
}

}  // namespace terrier::execution::compiler::test

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  terrier::execution::ExecutionUtil::InitTPL();
  int ret = RUN_ALL_TESTS();
  terrier::execution::ExecutionUtil::ShutdownTPL();
  return ret;
}
