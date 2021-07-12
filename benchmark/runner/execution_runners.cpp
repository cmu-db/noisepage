#include <cstdio>
#include <functional>
#include <pqxx/pqxx>  // NOLINT
#include <random>
#include <utility>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "binder/bind_node_visitor.h"
#include "common/macros.h"
#include "common/scoped_timer.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_settings.h"
#include "execution/execution_util.h"
#include "execution/sql/ddl_executors.h"
#include "execution/table_generator/table_generator.h"
#include "execution/util/cpu_info.h"
#include "execution/vm/bytecode_handlers.h"
#include "execution/vm/module.h"
#include "gflags/gflags.h"
#include "loggers/loggers_util.h"
#include "main/db_main.h"
#include "optimizer/cost_model/forced_cost_model.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/optimizer.h"
#include "optimizer/properties.h"
#include "optimizer/query_to_operator_transformer.h"
#include "planner/plannodes/index_join_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
#include "runner/execution_runners_argument_generator.h"
#include "runner/execution_runners_data_config.h"
#include "runner/execution_runners_settings.h"
#include "self_driving/modeling/operating_unit.h"
#include "self_driving/modeling/operating_unit_defs.h"
#include "self_driving/modeling/operating_unit_recorder.h"
#include "storage/index/bplustree.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "traffic_cop/traffic_cop_util.h"

namespace noisepage::runner {

/**
 * ExecutionRunners Config Data
 */
ExecutionRunnersDataConfig config;

/**
 * ExecutionRunners Settings
 */
ExecutionRunnersSettings settings;

/**
 * Static db_main instance
 * This is done so all tests reuse the same DB Main instance
 */
DBMain *db_main = nullptr;

/**
 * Database OID
 * This is done so all tests can use the same database OID
 */
catalog::db_oid_t db_oid{0};

/**
 * Should start small-row only scans
 */
bool rerun_start = false;

/**
 * Empty global param vector
 */
std::vector<std::vector<parser::ConstantValueExpression>> empty_params = {};

/**
 * Templated function that can be passed to Google Benchmark's Apply() function.
 * Template argument f specifies the argument generator to invoke
 * @param b Benchmark
 */
template <ExecutionRunnersArgumentGenerator::GenArgFn f>
void GenBenchmarkArguments(benchmark::internal::Benchmark *b) {
  std::vector<std::vector<int64_t>> args;
  f(&args, settings, config);

  for (auto &arg : args) {
    b->Args(arg);
  }
}

void InvokeGC() {
  // Perform GC to do any cleanup
  auto gc = noisepage::runner::db_main->GetStorageLayer()->GetGarbageCollector();
  gc->PerformGarbageCollection();
  gc->PerformGarbageCollection();
}

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
  constexpr static bool VALUE = !std::is_trivially_copyable<Decayed>::value ||
                                sizeof(Decayed) > sizeof(int64_t) || std::is_pointer<Decayed>::value;
};

template <typename T>
auto DoNotOptimizeAway(const T &datum) -> typename std::enable_if<!DoNotOptimizeAwayNeedsIndirect<T>::VALUE>::type {
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

/**
 * To prevent the iterator op i from being optimized out on
 * clang/gcc compilers, gcc requires noinline and noclone
 * to prevent any inlining. noclone is used to prevent gcc
 * from doing interprocedural constant propagation.
 *
 * DoNotOptimizeAway is placed inside the for () loop to
 * ensure that the compiler does not blindly optimize away
 * the for loop.
 *
 * TIGHT_LOOP_OPERATION(uint32_t, PLUS, +)
 * Defines a function called uint32_t_PLUS that adds integers.
 */
#ifdef __clang__
#define TIGHT_LOOP_OPERATION(type, name, op)                       \
  __attribute__((noinline)) type __##type##_##name(size_t count) { \
    type iterator = 1;                                             \
    for (size_t i = 1; i <= count; i++) {                          \
      iterator = iterator op i;                                    \
      DoNotOptimizeAway(iterator);                                 \
    }                                                              \
                                                                   \
    return iterator;                                               \
  }

#elif __GNUC__
#define TIGHT_LOOP_OPERATION(type, name, op)                                \
  __attribute__((noinline, noclone)) type __##type##_##name(size_t count) { \
    type iterator = 1;                                                      \
    for (size_t i = 1; i <= count; i++) {                                   \
      iterator = iterator op i;                                             \
      DoNotOptimizeAway(iterator);                                          \
    }                                                                       \
                                                                            \
    return iterator;                                                        \
  }

#endif

static std::string ConstructTableName(type::TypeId left_type, type::TypeId right_type, int64_t num_left,
                                      int64_t num_right, size_t row, size_t car) {
  std::vector<type::TypeId> types = {left_type, right_type};
  std::vector<uint32_t> col_counts = {static_cast<uint32_t>(num_left), static_cast<uint32_t>(num_right)};
  return execution::sql::TableGenerator::GenerateTableName(types, col_counts, row, car);
}

/**
 * Construct a SQL clause.
 *
 * If alias is non-empty, then generates [left_alias].col# (similar for right_alias).
 * right_alias is only used if generating a predicate.
 * joiner is the string used for joining column statements together.
 *
 * is_predicate describes whether it's generating a predicate or just a projection.
 */
static std::string ConstructSQLClause(type::TypeId left_type, type::TypeId right_type, int64_t num_left,
                                      int64_t num_right, const std::string &joiner, const std::string &left_alias,
                                      bool is_predicate, const std::string &right_alias) {
  std::stringstream fragment;

  std::vector<type::TypeId> types = {left_type, right_type};
  std::vector<int64_t> number = {num_left, num_right};
  bool emit_alias = !left_alias.empty();
  bool wrote = false;
  for (size_t i = 0; i < types.size(); i++) {
    if (types[i] == type::TypeId::INVALID) {
      // Skip invalid types (that means don't care)
      continue;
    }

    auto type = type::TypeUtil::TypeIdToString(types[i]);
    for (auto col = 1; col <= number[i]; col++) {
      if (wrote) {
        fragment << joiner;
      }

      if (emit_alias) {
        fragment << left_alias << ".";
      }

      fragment << type << col;
      if (is_predicate) {
        fragment << " = ";
        if (!right_alias.empty()) {
          fragment << right_alias << ".";
        }
        fragment << type << col;
      }

      wrote = true;
    }
  }
  return fragment.str();
}

static std::string ConstructIndexScanPredicate(type::TypeId key_type, int64_t key_num, int64_t lookup_size) {
  auto type = type::TypeUtil::TypeIdToString(key_type);
  std::stringstream predicatess;
  for (auto j = 1; j <= key_num; j++) {
    if (lookup_size == 1) {
      predicatess << type << j << " = $1";
    } else {
      predicatess << type << j << " >= $1";
      predicatess << " AND " << type << j << " <= $2";
    }

    if (j != key_num) predicatess << " AND ";
  }
  return predicatess.str();
}

static void GenIdxScanParameters(type::TypeId type_param, int64_t num_rows, int64_t lookup_size, int64_t num_iters,
                                 std::vector<std::vector<parser::ConstantValueExpression>> *real_params) {
  std::mt19937 generator{};
  std::vector<std::pair<uint32_t, uint32_t>> bounds;
  for (int i = 0; i < num_iters; i++) {
    // Pick a range [0, 10). The span is num_rows - lookup_size / 10 which controls
    // the span of numbers within a given range.
    int num_regions = 10;
    int64_t span = (num_rows - lookup_size) / num_regions;
    auto range =
        std::uniform_int_distribution(static_cast<uint32_t>(0), static_cast<uint32_t>(num_regions - 1))(generator);
    auto low_key = std::uniform_int_distribution(static_cast<uint32_t>(0),
                                                 static_cast<uint32_t>((span >= 1) ? (span - 1) : 0))(generator);
    low_key += range * span;

    std::vector<parser::ConstantValueExpression> param;
    if (lookup_size == 1) {
      if (type_param != type::TypeId::VARCHAR) {
        param.emplace_back(type_param, execution::sql::Integer(low_key));
      } else {
        std::string val = std::to_string(low_key);
        param.emplace_back(type_param, execution::sql::StringVal(val.c_str()));
      }
      bounds.emplace_back(low_key, low_key);
    } else {
      auto high_key = low_key + lookup_size - 1;
      if (type_param != type::TypeId::VARCHAR) {
        param.emplace_back(type_param, execution::sql::Integer(low_key));
        param.emplace_back(type_param, execution::sql::Integer(high_key));
      } else {
        std::string val = std::to_string(low_key);
        param.emplace_back(type_param, execution::sql::StringVal(val.c_str()));
        val = std::to_string(high_key);
        param.emplace_back(type_param, execution::sql::StringVal(val.c_str()));
      }
      bounds.emplace_back(low_key, high_key);
    }

    real_params->emplace_back(std::move(param));
  }
}

class ExecutionRunners : public benchmark::Fixture {
 public:
  static execution::query_id_t query_id;
  static execution::vm::ExecutionMode mode;
  const uint64_t optimizer_timeout_ = 1000000;

  static std::unique_ptr<planner::AbstractPlanNode> PassthroughPlanChecker(
      UNUSED_ATTRIBUTE common::ManagedPointer<transaction::TransactionContext> txn,
      std::unique_ptr<planner::AbstractPlanNode> plan) {
    return plan;
  }

  void ExecuteSeqScan(benchmark::State *state);
  void ExecuteInsert(benchmark::State *state);
  void ExecuteUpdate(benchmark::State *state);
  void ExecuteDelete(benchmark::State *state);
  void ExecuteCreateIndex(benchmark::State *state);
  void ExecuteIndexOperation(benchmark::State *state, bool is_insert);

  std::unique_ptr<planner::AbstractPlanNode> IndexScanChecker(
      size_t num_keys, common::ManagedPointer<transaction::TransactionContext> txn,
      std::unique_ptr<planner::AbstractPlanNode> plan) {
    if (plan->GetPlanNodeType() != planner::PlanNodeType::INDEXSCAN) throw "Expected IndexScan";

    auto *idx_scan = reinterpret_cast<planner::IndexScanPlanNode *>(plan.get());
    if (idx_scan->GetLoIndexColumns().size() != num_keys) throw "Number keys mismatch";

    return plan;
  }

  std::unique_ptr<planner::AbstractPlanNode> IndexNLJoinChecker(
      std::string build_tbl, size_t num_cols, common::ManagedPointer<transaction::TransactionContext> txn,
      std::unique_ptr<planner::AbstractPlanNode> plan) {
    if (plan->GetPlanNodeType() != planner::PlanNodeType::INDEXNLJOIN) throw "Expected IndexNLJoin";
    if (plan->GetChildrenSize() != 1) throw "Expected 1 child";
    if (plan->GetChild(0)->GetPlanNodeType() != planner::PlanNodeType::SEQSCAN) throw "Expected child IdxScan";

    auto *nlplan = reinterpret_cast<const planner::IndexJoinPlanNode *>(plan.get());
    if (nlplan->GetLoIndexColumns().size() != num_cols) throw "Number keys mismatch";

    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    auto build_oid = accessor->GetTableOid(std::move(build_tbl));
    if (nlplan->GetTableOid() != build_oid) throw "inner index does not match";

    return plan;
  }

  std::unique_ptr<planner::AbstractPlanNode> ChildIndexScanChecker(
      common::ManagedPointer<transaction::TransactionContext> txn, std::unique_ptr<planner::AbstractPlanNode> plan) {
    if (plan->GetChild(0)->GetPlanNodeType() != planner::PlanNodeType::INDEXSCAN) throw "Expected IndexScan";

    return plan;
  }

  std::unique_ptr<planner::AbstractPlanNode> UpdateIndexScanChecker(
      common::ManagedPointer<transaction::TransactionContext> txn, std::unique_ptr<planner::AbstractPlanNode> plan) {
    if (plan->GetPlanNodeType() != planner::PlanNodeType::UPDATE) throw "Expected Update";
    auto *upd = reinterpret_cast<planner::UpdatePlanNode *>(plan.get());
    if (!upd->GetIndexOids().empty()) throw "Update index oids not empty";
    if (plan->GetChild(0)->GetPlanNodeType() != planner::PlanNodeType::INDEXSCAN) throw "Expected IndexScan";

    return plan;
  }

  std::unique_ptr<planner::AbstractPlanNode> JoinNonSelfChecker(
      std::string build_tbl, common::ManagedPointer<transaction::TransactionContext> txn,
      std::unique_ptr<planner::AbstractPlanNode> plan) {
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    auto build_oid = accessor->GetTableOid(std::move(build_tbl));

    if (plan->GetPlanNodeType() != planner::PlanNodeType::HASHJOIN) throw "Expected HashJoin";
    if (plan->GetChild(0)->GetPlanNodeType() != planner::PlanNodeType::SEQSCAN) throw "Expected Left SeqScan";
    if (plan->GetChild(1)->GetPlanNodeType() != planner::PlanNodeType::SEQSCAN) throw "Expected Right SeqScan";

    // Assumes the build side is the left (since that is the HashJoinLeftTranslator)
    auto *l_scan = reinterpret_cast<const planner::SeqScanPlanNode *>(plan->GetChild(0));
    if (l_scan->GetTableOid() != build_oid) {
      // Don't modify join_predicate/left/right hash keys because derivedindex DerivedValueExpression
      // Don't modify output_schema since just output 1 side tuple anyways
      plan->SwapChildren();
    }

    return plan;
  }

  TIGHT_LOOP_OPERATION(uint32_t, PLUS, +);
  TIGHT_LOOP_OPERATION(uint32_t, MULTIPLY, *);
  TIGHT_LOOP_OPERATION(uint32_t, DIVIDE, /);
  TIGHT_LOOP_OPERATION(uint32_t, GEQ, >=);
  TIGHT_LOOP_OPERATION(double, PLUS, +);
  TIGHT_LOOP_OPERATION(double, MULTIPLY, *);
  TIGHT_LOOP_OPERATION(double, DIVIDE, /);
  TIGHT_LOOP_OPERATION(double, GEQ, >=);

  void SetUp(const benchmark::State &state) final {
    catalog_ = db_main->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main->GetTransactionLayer()->GetTransactionManager();
    metrics_manager_ = db_main->GetMetricsManager();
  }

  static execution::exec::ExecutionSettings GetExecutionSettings(bool pipeline_metrics_enabled = true) {
    execution::exec::ExecutionSettings exec_settings;
    exec_settings.is_parallel_execution_enabled_ = false;
    exec_settings.is_counters_enabled_ = false;
    exec_settings.is_pipeline_metrics_enabled_ = pipeline_metrics_enabled;
    return exec_settings;
  }

  static execution::exec::ExecutionSettings GetParallelExecutionSettings(size_t num_threads, bool counters) {
    execution::exec::ExecutionSettings exec_settings;
    exec_settings.is_pipeline_metrics_enabled_ = true;
    exec_settings.is_parallel_execution_enabled_ = (num_threads != 0);
    exec_settings.number_of_parallel_execution_threads_ = num_threads;
    exec_settings.is_counters_enabled_ = counters;
    exec_settings.is_static_partitioner_enabled_ = true;
    return exec_settings;
  }

  std::pair<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::OutputSchema>>
  OptimizeSqlStatement(
      const std::string &query, std::unique_ptr<optimizer::AbstractCostModel> cost_model,
      std::unique_ptr<selfdriving::PipelineOperatingUnits> pipeline_units,
      const std::function<std::unique_ptr<planner::AbstractPlanNode>(
          common::ManagedPointer<transaction::TransactionContext>, std::unique_ptr<planner::AbstractPlanNode>)>
          &checker = std::function<std::unique_ptr<planner::AbstractPlanNode>(
              common::ManagedPointer<transaction::TransactionContext>, std::unique_ptr<planner::AbstractPlanNode>)>(
              PassthroughPlanChecker),
      common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params = nullptr,
      common::ManagedPointer<std::vector<type::TypeId>> param_types = nullptr,
      execution::exec::ExecutionSettings *exec_settings_arg = nullptr) {
    auto txn = txn_manager_->BeginTransaction();
    auto stmt_list = parser::PostgresParser::BuildParseTree(query);

    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid);
    binder.BindNameToNode(common::ManagedPointer(stmt_list), params, param_types);

    auto out_plan =
        trafficcop::TrafficCopUtil::Optimize(common::ManagedPointer(txn), common::ManagedPointer(accessor),
                                             common::ManagedPointer(stmt_list), db_oid, db_main->GetStatsStorage(),
                                             std::move(cost_model), optimizer_timeout_, params)
            ->TakePlanNodeOwnership();

    out_plan = checker(common::ManagedPointer(txn), std::move(out_plan));
    if (out_plan->GetPlanNodeType() == planner::PlanNodeType::CREATE_INDEX) {
      execution::sql::DDLExecutors::CreateIndexExecutor(
          common::ManagedPointer<planner::CreateIndexPlanNode>(
              reinterpret_cast<planner::CreateIndexPlanNode *>(out_plan.get())),
          common::ManagedPointer<catalog::CatalogAccessor>(accessor));
    } else if (out_plan->GetPlanNodeType() == planner::PlanNodeType::DROP_INDEX) {
      execution::sql::DDLExecutors::DropIndexExecutor(
          common::ManagedPointer<planner::DropIndexPlanNode>(
              reinterpret_cast<planner::DropIndexPlanNode *>(out_plan.get())),
          common::ManagedPointer<catalog::CatalogAccessor>(accessor));
      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      return std::make_pair(nullptr, nullptr);
    }

    auto exec_settings = GetExecutionSettings();
    if (exec_settings_arg != nullptr) {
      exec_settings = *exec_settings_arg;
    }

    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        db_oid, common::ManagedPointer(txn), execution::exec::NoOpResultConsumer(), out_plan->GetOutputSchema().Get(),
        common::ManagedPointer(accessor), exec_settings, metrics_manager_, DISABLED, DISABLED);

    execution::compiler::ExecutableQuery::query_identifier.store(ExecutionRunners::query_id++);
    auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, exec_settings, accessor.get(),
                                                                       execution::compiler::CompilationMode::OneShot);

    // Some runners rely on counters to work correctly (i.e parallel create index).
    // Counter code relies on the ids of features extracted during code generation
    // to update numbers. However, the synthetic pipeline + features that are
    // set by the execution-runners do not have these feature ids available.
    //
    // For now, since a pipeline does not contain any duplicate feature types (i.e
    // there will not be 2 hashjoin_probes in 1 pipeline), we can assign feature ids
    // to our synthetic features by finding the matching feature from the feature
    // vector produced during codegen.
    auto pipeline = exec_query->GetPipelineOperatingUnits();
    for (auto &info : pipeline_units->units_) {
      auto other_feature = pipeline->units_[info.first];
      for (auto &oufeature : info.second) {
        for (const auto &other_oufeature : other_feature) {
          if (oufeature.GetExecutionOperatingUnitType() == other_oufeature.GetExecutionOperatingUnitType()) {
            oufeature.feature_id_ = other_oufeature.feature_id_;
            break;
          }
        }
      }
    }

    exec_query->SetPipelineOperatingUnits(std::move(pipeline_units));

    auto ret_val = std::make_pair(std::move(exec_query), out_plan->GetOutputSchema()->Copy());
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    return ret_val;
  }

  void HandleBuildDropIndex(bool is_build, int64_t tbl_cols, int64_t num_rows, int64_t num_key, type::TypeId type) {
    auto block_store = db_main->GetStorageLayer()->GetBlockStore();
    auto catalog = db_main->GetCatalogLayer()->GetCatalog();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

    auto txn = txn_manager->BeginTransaction();
    auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    auto exec_settings = ExecutionRunners::GetExecutionSettings();
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        db_oid, common::ManagedPointer(txn), nullptr, nullptr, common::ManagedPointer(accessor), exec_settings,
        metrics_manager_, DISABLED, DISABLED);

    execution::sql::TableGenerator table_generator(exec_ctx.get(), block_store, accessor->GetDefaultNamespace());
    if (is_build) {
      // This function builds a BPLUSTREE index
      table_generator.BuildExecutionRunnerIndex(type, tbl_cols, num_rows, num_key);
    } else {
      bool result = table_generator.DropExecutionRunnerIndex(type, tbl_cols, num_rows, num_key);
      if (!result) {
        throw "Drop Index has failed";
      }
    }

    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    InvokeGC();
  }

  // Used for the CREATE INDEX execution-runner
  void DropIndexByName(const std::string &name) {
    auto catalog = db_main->GetCatalogLayer()->GetCatalog();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

    auto txn = txn_manager->BeginTransaction();
    auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

    auto index_oid = accessor->GetIndexOid(name);
    accessor->DropIndex(index_oid);

    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    InvokeGC();
  }

  void BenchmarkExecQuery(int64_t num_iters, execution::compiler::ExecutableQuery *exec_query,
                          planner::OutputSchema *out_schema, bool commit,
                          std::vector<std::vector<parser::ConstantValueExpression>> *params = &empty_params,
                          execution::exec::ExecutionSettings *exec_settings_arg = nullptr) {
    transaction::TransactionContext *txn = nullptr;
    std::unique_ptr<catalog::CatalogAccessor> accessor = nullptr;
    std::vector<std::vector<parser::ConstantValueExpression>> param_ref = *params;

    execution::exec::NoOpResultConsumer consumer;
    execution::exec::OutputCallback callback = consumer;
    for (auto i = 0; i < num_iters; i++) {
      common::ManagedPointer<metrics::MetricsManager> metrics_manager = nullptr;
      if (i == num_iters - 1) {
        metrics_manager_->RegisterThread();
        metrics_manager = metrics_manager_;
      }

      txn = txn_manager_->BeginTransaction();
      accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

      auto exec_settings = GetExecutionSettings();
      if (exec_settings_arg != nullptr) {
        exec_settings = *exec_settings_arg;
      }

      auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
          db_oid, common::ManagedPointer(txn), callback, out_schema, common::ManagedPointer(accessor), exec_settings,
          metrics_manager, DISABLED, DISABLED);

      // Attach params to ExecutionContext
      if (static_cast<size_t>(i) < param_ref.size()) {
        exec_ctx->SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&param_ref[i]));
      }

      exec_query->Run(common::ManagedPointer(exec_ctx), mode);

      NOISEPAGE_ASSERT(!txn->MustAbort(), "Transaction should not be force-aborted");
      if (commit)
        txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      else
        txn_manager_->Abort(txn);

      if (i == num_iters - 1) {
        metrics_manager_->Aggregate();
        metrics_manager_->UnregisterThread();
      }
    }
  }

  void BenchmarkArithmetic(selfdriving::ExecutionOperatingUnitType type, size_t num_elem) {
    auto qid = ExecutionRunners::query_id++;
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    auto exec_settings = GetExecutionSettings();
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        db_oid, common::ManagedPointer(txn), nullptr, nullptr, common::ManagedPointer(accessor), exec_settings,
        metrics_manager_, DISABLED, DISABLED);
    exec_ctx->SetExecutionMode(static_cast<uint8_t>(mode));

    selfdriving::PipelineOperatingUnits units;
    selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
    exec_ctx->SetPipelineOperatingUnits(common::ManagedPointer(&units));
    pipe0_vec.emplace_back(execution::translator_id_t(1), type, num_elem, 4, 1, num_elem, 1, 0, 0, 0, 0);
    units.RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

    selfdriving::ExecOUFeatureVector ouvec;
    exec_ctx->InitializeOUFeatureVector(&ouvec, execution::pipeline_id_t(1));
    switch (type) {
      case selfdriving::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS: {
        exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
        uint32_t ret = __uint32_t_PLUS(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
        DoNotOptimizeAway(ret);
        break;
      }
      case selfdriving::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY: {
        exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
        uint32_t ret = __uint32_t_MULTIPLY(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
        DoNotOptimizeAway(ret);
        break;
      }
      case selfdriving::ExecutionOperatingUnitType::OP_INTEGER_DIVIDE: {
        exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
        uint32_t ret = __uint32_t_DIVIDE(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
        DoNotOptimizeAway(ret);
        break;
      }
      case selfdriving::ExecutionOperatingUnitType::OP_INTEGER_COMPARE: {
        exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
        uint32_t ret = __uint32_t_GEQ(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
        DoNotOptimizeAway(ret);
        break;
      }
      case selfdriving::ExecutionOperatingUnitType::OP_REAL_PLUS_OR_MINUS: {
        exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
        double ret = __double_PLUS(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
        DoNotOptimizeAway(ret);
        break;
      }
      case selfdriving::ExecutionOperatingUnitType::OP_REAL_MULTIPLY: {
        exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
        double ret = __double_MULTIPLY(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
        DoNotOptimizeAway(ret);
        break;
      }
      case selfdriving::ExecutionOperatingUnitType::OP_REAL_DIVIDE: {
        exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
        double ret = __double_DIVIDE(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
        DoNotOptimizeAway(ret);
        break;
      }
      case selfdriving::ExecutionOperatingUnitType::OP_REAL_COMPARE: {
        exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
        double ret = __double_GEQ(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
        DoNotOptimizeAway(ret);
        break;
      }
      case selfdriving::ExecutionOperatingUnitType::OP_VARCHAR_COMPARE: {
        uint32_t multiply_factor = sizeof(storage::VarlenEntry) / sizeof(uint32_t);
        auto *val = reinterpret_cast<storage::VarlenEntry *>(new uint32_t[num_elem * multiply_factor * 2]);
        for (size_t i = 0; i < num_elem * 2; ++i) {
          std::string str_val = std::to_string(i & 0xFF);
          val[i] = storage::VarlenEntry::Create(str_val);
        }
        uint32_t ret = 0;
        exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
        for (size_t i = 0; i < num_elem; i++) {
          ret += storage::VarlenEntry::Compare(val[i], val[i + num_elem]);
          DoNotOptimizeAway(ret);
        }
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
        DoNotOptimizeAway(ret);
        break;
      }
      default:
        UNREACHABLE("Unsupported ExecutionOperatingUnitType");
        break;
    }

    ouvec.Reset();
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

 protected:
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
};

execution::query_id_t ExecutionRunners::query_id = execution::query_id_t(0);
execution::vm::ExecutionMode ExecutionRunners::mode = execution::vm::ExecutionMode::Interpret;

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ0_ArithmeticRunners)(benchmark::State &state) {
  if (rerun_start) {
    return;
  }

  metrics_manager_->RegisterThread();

  // state.range(0) is the OperatingUnitType
  // state.range(1) is the size
  BenchmarkArithmetic(static_cast<selfdriving::ExecutionOperatingUnitType>(state.range(0)),
                      static_cast<size_t>(state.range(1)));

  metrics_manager_->Aggregate();
  metrics_manager_->UnregisterThread();

  state.SetItemsProcessed(state.range(1));
}

template <settings::Param param, typename T>
void DbMainSetParam(T value) {
  const common::action_id_t action_id(1);
  auto callback = [](common::ManagedPointer<common::ActionContext> action UNUSED_ATTRIBUTE) {};
  settings::setter_callback_fn setter_callback = callback;
  auto db_settings = db_main->GetSettingsManager();

  if (std::is_same<T, bool>::value) {
    auto action_context = std::make_unique<common::ActionContext>(action_id);
    db_settings->SetBool(param, value, common::ManagedPointer(action_context), setter_callback);
  } else if (std::is_integral<T>::value) {
    auto action_context = std::make_unique<common::ActionContext>(action_id);
    db_settings->SetInt(param, value, common::ManagedPointer(action_context), setter_callback);
  }
}

void NetworkQueriesOutputRunners(pqxx::work *txn) {
  std::ostream null{nullptr};
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER, type::TypeId::REAL};
  std::vector<int64_t> row_nums = {1, 3, 5, 7, 10, 50, 100, 500, 1000, 2000, 5000, 10000};

  bool metrics_enabled = db_main->GetSettingsManager()->GetBool(settings::Param::pipeline_metrics_enable);
  DbMainSetParam<settings::Param::counters_enable, bool>(false);
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        // Scale # iterations accordingly
        // Want to warmup the first query
        int iters = 1;
        if (row == 1 && col == 1 && type == type::TypeId::INTEGER) {
          iters += settings.warmup_iterations_num_;
        }

        for (int i = 0; i < iters; i++) {
          if (i != iters - 1 && metrics_enabled) {
            DbMainSetParam<settings::Param::pipeline_metrics_enable, bool>(false);
            metrics_enabled = false;
          } else if (i == iters - 1 && !metrics_enabled) {
            DbMainSetParam<settings::Param::pipeline_metrics_enable, bool>(true);
            metrics_enabled = true;
          }

          std::stringstream query_ss;
          std::string type_s = (type == type::TypeId::INTEGER) ? "int" : "real";

          query_ss << "SELECT nprunnersemit" << type_s << "(" << row << "," << col << ",";
          if (type == type::TypeId::INTEGER)
            query_ss << col << ",0)";
          else
            query_ss << "0," << col << ")";

          if (col > 1) {
            query_ss << ",";
            for (int j = 1; j < col; j++) {
              query_ss << "nprunnersdummy" << type_s << "()";
              if (j != col - 1) {
                query_ss << ",";
              }
            }
          }

          // Execute query
          pqxx::result r{txn->exec(query_ss.str())};

          // Get all the results
          for (const auto &result_row : r) {
            for (auto j = 0; j < col; j++) {
              null << result_row[j];
            }
          }
        }
      }
    }
  }
}

void NetworkQueriesCreateIndexRunners(pqxx::work *txn) {
  std::ostream null{nullptr};
  std::vector<int> num_threads = {1, 2, 4, 8, 16};
  std::vector<uint32_t> num_cols = {1, 2, 4, 8, 15};
  std::vector<type::TypeId> types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  std::vector<uint32_t> row_nums = {1,     10,    100,   200,    500,    1000,   2000,   5000,
                                    10000, 20000, 50000, 100000, 300000, 500000, 1000000};

  // Extract to restore
  int original_threads = db_main->GetSettingsManager()->GetInt(settings::Param::num_parallel_execution_threads);
  bool counters = db_main->GetSettingsManager()->GetBool(settings::Param::counters_enable);
  bool metrics_enabled = db_main->GetSettingsManager()->GetBool(settings::Param::pipeline_metrics_enable);
  for (auto thread : num_threads) {
    DbMainSetParam<settings::Param::num_parallel_execution_threads, int>(thread);
    DbMainSetParam<settings::Param::counters_enable, bool>(true);

    for (auto type : types) {
      for (auto col : num_cols) {
        for (auto row : row_nums) {
          // Scale # iterations accordingly
          // Want to warmup the first query
          int iters = 1;
          if (row == 1 && col == 1 && type == type::TypeId::INTEGER) {
            iters += settings.warmup_iterations_num_;
          }

          for (int i = 0; i < iters; i++) {
            if (i != iters - 1 && metrics_enabled) {
              DbMainSetParam<settings::Param::pipeline_metrics_enable, bool>(false);
              metrics_enabled = false;
            } else if (i == iters - 1 && !metrics_enabled) {
              DbMainSetParam<settings::Param::pipeline_metrics_enable, bool>(true);
              metrics_enabled = true;
            }

            std::string create_query;
            {
              std::stringstream query_ss;
              auto type_name = type::TypeUtil::TypeIdToString(type);
              auto table_name = ConstructTableName(type, type::TypeId::INVALID, 15, 0, row, row);
              query_ss << "CREATE INDEX executionrunners__" << row << " ON " << table_name << "(";
              for (size_t j = 1; j <= col; j++) {
                query_ss << type_name << j;
                if (j != col) {
                  query_ss << ",";
                } else {
                  query_ss << ")";
                }
              }
              create_query = query_ss.str();
            }
            txn->exec(create_query);

            std::string delete_query;
            {
              std::stringstream query_ss;
              query_ss << "DROP INDEX executionrunners__" << row;
              delete_query = query_ss.str();
            }
            txn->exec(delete_query);
          }
        }
      }
    }
  }
  DbMainSetParam<settings::Param::num_parallel_execution_threads, int>(original_threads);
  DbMainSetParam<settings::Param::counters_enable, bool>(counters);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ0_OutputRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_reals = state.range(1);
  auto row_num = state.range(2);
  auto num_col = num_integers + num_reals;

  std::stringstream output;
  output << "struct OutputStruct {\n";
  for (auto i = 0; i < num_integers; i++) output << "col" << i << " : Integer\n";
  for (auto i = num_integers; i < num_col; i++) output << "col" << i << " : Real\n";
  output << "}\n";

  output << "struct QueryState {\nexecCtx: *ExecutionContext\n}\n";
  output << "struct P1_State {\noutput_buffer: *OutputBuffer\nexecFeatures: ExecOUFeatureVector\n}\n";
  output << "fun Query0_Init(queryState: *QueryState) -> nil {\nreturn}\n";
  output << "fun Query0_Pipeline1_InitPipelineState(queryState: *QueryState, pipelineState: *P1_State) -> nil {\n";
  output << "\tpipelineState.output_buffer = @resultBufferNew(queryState.execCtx)\n";
  output << "\treturn\n";
  output << "}\n";
  output << "fun Query0_Pipeline1_TearDownPipelineState(queryState: *QueryState, pipelineState: *P1_State) -> nil {\n";
  output << "\t@resultBufferFree(pipelineState.output_buffer)\n";
  output << "\t@execOUFeatureVectorReset(&pipelineState.execFeatures)\n";
  output << "}\n";

  // pipeline
  output << "fun Query0_Pipeline1_SerialWork(queryState: *QueryState, pipelineState: *P1_State) -> nil {\n";
  if (num_col > 0) {
    output << "\tvar out: *OutputStruct\n";
    output << "\tfor(var it = 0; it < " << row_num << "; it = it + 1) {\n";
    output << "\t\tout = @ptrCast(*OutputStruct, @resultBufferAllocRow(pipelineState.output_buffer))\n";
    output << "\t}\n";
  }
  output << "}\n";

  output << "fun Query0_Pipeline1_Init(queryState: *QueryState) -> nil {\n";
  output << "\tvar threadStateContainer = @execCtxGetTLS(queryState.execCtx)\n";
  output << "\t@tlsReset(threadStateContainer, @sizeOf(P1_State), Query0_Pipeline1_InitPipelineState, "
            "Query0_Pipeline1_TearDownPipelineState, queryState)\n";
  output << "\treturn\n";
  output << "}\n";

  output << "fun Query0_Pipeline1_Run(queryState: *QueryState) -> nil {\n";
  output
      << "\tvar pipelineState = @ptrCast(*P1_State, @tlsGetCurrentThreadState(@execCtxGetTLS(queryState.execCtx)))\n";
  output << "\t@execOUFeatureVectorInit(queryState.execCtx, &pipelineState.execFeatures, 1, false)\n";
  output << "\t@execCtxStartPipelineTracker(queryState.execCtx, 1)\n";
  output << "\tQuery0_Pipeline1_SerialWork(queryState, pipelineState)\n";
  output << "\t@resultBufferFinalize(pipelineState.output_buffer)\n";
  output << "\t@execCtxEndPipelineTracker(queryState.execCtx, 0, 1, &pipelineState.execFeatures)\n";
  output << "\treturn\n";
  output << "}\n";

  output << "fun Query0_Pipeline1_TearDown(queryState: *QueryState) -> nil {\n";
  output << "\t@tlsClear(@execCtxGetTLS(queryState.execCtx))\n";
  output << "\treturn\n";
  output << "}\n";

  output << "fun Query0_TearDown(queryState: *QueryState) -> nil {\nreturn\n}\n";

  output << "fun main(queryState: *QueryState) -> nil {\n";
  output << "\tQuery0_Pipeline1_Init(queryState)\n";
  output << "\tQuery0_Pipeline1_Run(queryState)\n";
  output << "\tQuery0_Pipeline1_TearDown(queryState)\n";
  output << "\treturn\n";
  output << "}\n";

  std::vector<planner::OutputSchema::Column> cols;
  for (auto i = 0; i < num_integers; i++) {
    std::stringstream col;
    col << "col" << i;
    cols.emplace_back(col.str(), type::TypeId::INTEGER, nullptr);
  }

  for (auto i = 0; i < num_reals; i++) {
    std::stringstream col;
    col << "col" << i;
    cols.emplace_back(col.str(), type::TypeId::REAL, nullptr);
  }

  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto real_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::REAL);
  auto tuple_size = int_size * num_integers + real_size * num_reals;

  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
  auto schema = std::make_unique<planner::OutputSchema>(std::move(cols));

  auto exec_settings = GetExecutionSettings();
  execution::compiler::ExecutableQuery::query_identifier.store(ExecutionRunners::query_id++);
  execution::exec::NoOpResultConsumer consumer;
  execution::exec::OutputCallback callback = consumer;
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      db_oid, common::ManagedPointer(txn), callback, schema.get(), common::ManagedPointer(accessor), exec_settings,
      metrics_manager_, DISABLED, DISABLED);

  auto exec_query = execution::compiler::ExecutableQuery(output.str(), common::ManagedPointer(exec_ctx), false, 16,
                                                         exec_settings, txn->StartTime());

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, row_num,
                         tuple_size, num_col, 0, 1, 0, 0, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));
  exec_query.SetPipelineOperatingUnits(std::move(units));

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  BenchmarkExecQuery(settings.warmup_iterations_num_ + 1, &exec_query, schema.get(), true);
}

void ExecutionRunners::ExecuteIndexOperation(benchmark::State *state, bool is_insert) {
  auto key_num = state->range(0);
  uint64_t tbl_cols = state->range(1);
  auto num_rows = state->range(2);
  auto type = static_cast<type::TypeId>(state->range(3));
  auto num_index = state->range(4);
  auto target = num_rows + 1;
  if (settings.skip_large_rows_runs_ && num_rows > settings.warmup_rows_limit_) {
    return;
  }

  // Skip compiled
  if (noisepage::runner::ExecutionRunners::mode == execution::vm::ExecutionMode::Compiled) return;

  // Create the indexes for batch-insert
  auto cols = ConstructSQLClause(type, type::TypeId::INVALID, key_num, 0, ", ", "", false, "");
  auto tbl_name = ConstructTableName(type, type::TypeId::INVALID, tbl_cols, 0, num_rows, num_rows);
  for (auto i = 0; i < num_index; i++) {
    auto execution_settings = GetExecutionSettings(false);
    auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();

    std::stringstream query;
    query << "CREATE INDEX idx" << i << " ON " << tbl_name << " USING BPLUSTREE (" << cols << ")";
    auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units),
                                       PassthroughPlanChecker, nullptr, nullptr, &execution_settings);
    BenchmarkExecQuery(1, equery.first.get(), equery.second.get(), true, &empty_params, &execution_settings);
  }

  // Invoke GC to clean some data
  InvokeGC();
  InvokeGC();

  int64_t num_iters = 1 + settings.index_model_warmup_iterations_num_;
  for (int64_t iter = 0; iter < num_iters; iter++) {
    common::ManagedPointer<metrics::MetricsManager> metrics_manager = nullptr;
    if (iter == num_iters - 1) {
      metrics_manager_->RegisterThread();
      metrics_manager = metrics_manager_;
    }

    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    auto tbl_oid = accessor->GetTableOid(tbl_name);
    auto idx_oids = accessor->GetIndexOids(tbl_oid);

    auto exec_settings = GetExecutionSettings();
    execution::exec::NoOpResultConsumer consumer;
    execution::exec::OutputCallback callback = consumer;
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        db_oid, common::ManagedPointer(txn), callback, nullptr, common::ManagedPointer(accessor), exec_settings,
        metrics_manager, DISABLED, DISABLED);

    // A brief discussion of the features:
    // NUM_ROWS: size of the index
    // KEY_SIZE: size of the keys
    // KEY_NUM: number of keys
    // Cardinality field: number of indexes being inserted into (i.e batch size)
    selfdriving::ExecOUFeatureVector features;
    selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
    auto feature_type = is_insert ? selfdriving::ExecutionOperatingUnitType::INDEX_INSERT
                                  : selfdriving::ExecutionOperatingUnitType::INDEX_DELETE;
    auto type_size = type::TypeUtil::GetTypeSize(type);
    auto key_size = type_size * key_num;
    pipe0_vec.emplace_back(execution::translator_id_t(1), feature_type, num_rows, key_size, key_num, num_index, 1, 0, 0,
                           storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_UPPER_THRESHOLD,
                           storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_LOWER_THRESHOLD);
    selfdriving::PipelineOperatingUnits units;
    units.RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));
    exec_ctx->SetPipelineOperatingUnits(common::ManagedPointer<selfdriving::PipelineOperatingUnits>(&units));
    exec_ctx->InitializeOUFeatureVector(&features, execution::pipeline_id_t(1));

    // Initialize Storage Interface
    uint32_t col_oids[tbl_cols];
    for (uint64_t i = 0; i < tbl_cols; i++) {
      col_oids[i] = i + 1;
    }

    // No columns if deleting
    execution::sql::StorageInterface si(exec_ctx.get(), tbl_oid, col_oids, is_insert ? tbl_cols : 0, true);
    storage::ProjectedRow *tbl_pr = nullptr;
    storage::TupleSlot slot;
    if (is_insert) {
      OpStorageInterfaceGetTablePR(&tbl_pr, &si);
      for (uint64_t i = 0; i < tbl_cols; i++) {
        execution::sql::Integer value(target);
        if (type == type::TypeId::INTEGER) {
          OpPRSetInt(tbl_pr, i, &value);
        } else if (type == type::TypeId::BIGINT) {
          OpPRSetBigInt(tbl_pr, i, &value);
        }
      }

      OpStorageInterfaceTableInsert(&slot, &si);
    } else {
      bool has_more;
      uint32_t col_oids2[] = {1};
      bool done = false;
      execution::sql::TableVectorIterator tvi(exec_ctx.get(), tbl_oid.UnderlyingValue(), col_oids2, 1);
      OpTableVectorIteratorPerformInit(&tvi);
      OpTableVectorIteratorNext(&has_more, &tvi);
      while (has_more) {
        // We will delete the first tuple. We need to do this iteration to
        // ensure that the Tuple is visible to the current transaction.
        execution::sql::VectorProjectionIterator *vpi = nullptr;
        OpTableVectorIteratorGetVPI(&vpi, &tvi);

        bool vpi_next;
        OpVPIHasNext(&vpi_next, vpi);
        while (vpi_next) {
          done = true;
          OpVPIGetSlot(&slot, vpi);

          // Do a TableDelete
          bool result;
          OpStorageInterfaceTableDelete(&result, &si, &slot);
          if (!result) {
            OpAbortTxn(exec_ctx.get());
          }

          break;
        }

        if (done) {
          break;
        }

        // Advance TVI
        OpTableVectorIteratorNext(&has_more, &tvi);
      }

      if (!done) {
        throw "Expected tuple to be deleted";
      }
    }

    // Measure the core index operation
    OpExecutionContextStartPipelineTracker(exec_ctx.get(), execution::pipeline_id_t(1));
    for (auto idx : idx_oids) {
      storage::ProjectedRow *idx_pr;
      OpStorageInterfaceGetIndexPR(&idx_pr, &si, idx.UnderlyingValue());
      for (auto col = 0; col < key_num; col++) {
        if (is_insert) {
          execution::sql::Integer val(0);
          if (type == type::TypeId::INTEGER) {
            OpPRGetInt(&val, tbl_pr, col);
            OpPRSetInt(idx_pr, col, &val);
          } else if (type == type::TypeId::BIGINT) {
            OpPRGetBigInt(&val, tbl_pr, col);
            OpPRSetBigInt(idx_pr, col, &val);
          }
        } else {
          execution::sql::Integer value(target);
          if (type == type::TypeId::INTEGER) {
            OpPRSetInt(idx_pr, col, &value);
          } else if (type == type::TypeId::BIGINT) {
            OpPRSetBigInt(idx_pr, col, &value);
          }
        }
      }

      bool result = true;
      if (is_insert)
        OpStorageInterfaceIndexInsert(&result, &si);
      else
        OpStorageInterfaceIndexDelete(&si, &slot);

      if (!result) {
        OpAbortTxn(exec_ctx.get());
      }
    }
    OpExecutionContextEndPipelineTracker(exec_ctx.get(), execution::query_id_t(0), execution::pipeline_id_t(1),
                                         &features);

    // For inserts/deletes, abort the transaction.
    // If insert, don't want prior inserts to affect next insert.
    // If delete, need to make sure tuple is still visible
    txn_manager_->Abort(txn);

    if (iter == num_iters - 1) {
      metrics_manager_->Aggregate();
      metrics_manager_->UnregisterThread();
    }
  }

  // Drop the indexes
  for (auto i = 0; i < num_index; i++) {
    auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();

    std::stringstream query;
    query << "DROP INDEX idx" << i;
    OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  }

  InvokeGC();
  InvokeGC();
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ10_IndexInsertRunners)(benchmark::State &state) {
  ExecuteIndexOperation(&state, true);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ11_IndexDeleteRunners)(benchmark::State &state) {
  ExecuteIndexOperation(&state, false);
}

void ExecutionRunners::ExecuteSeqScan(benchmark::State *state) {
  auto num_integers = state->range(0);
  auto num_mix = state->range(1);
  auto tbl_ints = state->range(2);
  auto tbl_mix = state->range(3);
  auto row = state->range(4);
  auto car = state->range(5);
  auto varchar_mix = state->range(6);

  int num_iters = 1;
  if (row <= settings.warmup_rows_limit_) {
    num_iters += settings.warmup_iterations_num_;
  } else if (rerun_start || settings.skip_large_rows_runs_) {
    return;
  }

  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  type::TypeId mix_type;
  if (varchar_mix == 1)
    mix_type = type::TypeId::VARCHAR;
  else
    mix_type = type::TypeId::REAL;
  size_t tuple_size = int_size * num_integers;
  size_t num_col = num_integers + num_mix;

  // Adjust for type of MIX
  for (auto i = 0; i < num_mix; i++) {
    selfdriving::OperatingUnitRecorder::AdjustKeyWithType(mix_type, &tuple_size, &num_col);
  }

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, row,
                         tuple_size, num_col, car, 1, 0, 0, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, row,
                         tuple_size, num_col, 0, 1, 0, 0, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::string query_final;
  {
    std::stringstream query;
    auto cols = ConstructSQLClause(type::TypeId::INTEGER, mix_type, num_integers, num_mix, ", ", "", false, "");
    auto tbl_name = ConstructTableName(type::TypeId::INTEGER, mix_type, tbl_ints, tbl_mix, row, car);
    query << "SELECT " << (cols) << " FROM " << tbl_name;
    query_final = query.str();
  }

  auto equery = OptimizeSqlStatement(query_final, std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true);

  state->SetItemsProcessed(row);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ1_0_SeqScanRunners)(benchmark::State &state) { ExecuteSeqScan(&state); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ1_1_SeqScanRunners)(benchmark::State &state) { ExecuteSeqScan(&state); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ2_0_IndexScanRunners)(benchmark::State &state) {
  auto type = static_cast<type::TypeId>(state.range(0));
  auto tbl_cols = state.range(1);
  size_t key_num = state.range(2);
  auto num_rows = state.range(3);
  auto lookup_size = state.range(4);
  auto is_build = state.range(5);

  if (lookup_size == 0) {
    if (is_build < 0) {
      throw "Invalid is_build argument for IndexScan";
    }

    HandleBuildDropIndex(is_build != 0, tbl_cols, num_rows, key_num, type);
    return;
  }

  int num_iters = 1;
  if (lookup_size <= settings.warmup_rows_limit_) {
    num_iters += settings.warmup_iterations_num_;
  } else if (rerun_start || settings.skip_large_rows_runs_) {
    return;
  }

  size_t tuple_size = 0;
  size_t num_col = key_num;
  for (size_t i = 0; i < num_col; i++) {
    selfdriving::OperatingUnitRecorder::AdjustKeyWithType(type, &tuple_size, &key_num);
  }

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, lookup_size,
                         tuple_size, key_num, 0, 1, 0, 0, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::IDX_SCAN, num_rows,
                         tuple_size, key_num, lookup_size, 1, 0, 0,
                         storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_UPPER_THRESHOLD,
                         storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_LOWER_THRESHOLD);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::vector<parser::ConstantValueExpression> params;
  std::vector<type::TypeId> param_types;
  if (type != type::TypeId::VARCHAR) {
    params.emplace_back(type, execution::sql::Integer(0));
  } else {
    std::string val = std::string("0");
    params.emplace_back(type, execution::sql::StringVal(val.c_str()));
  }
  param_types.push_back(type);
  if (lookup_size > 1) {
    if (type != type::TypeId::VARCHAR) {
      params.emplace_back(type, execution::sql::Integer(0));
    } else {
      std::string val = std::string("0");
      params.emplace_back(type, execution::sql::StringVal(val.c_str()));
    }
    param_types.push_back(type);
  }

  std::vector<std::vector<parser::ConstantValueExpression>> real_params;
  GenIdxScanParameters(type, num_rows, lookup_size, num_iters, &real_params);

  std::stringstream query;
  auto cols = ConstructSQLClause(type, type::TypeId::INVALID, num_col, 0, ", ", "", false, "");
  std::string predicate = ConstructIndexScanPredicate(type, num_col, lookup_size);
  auto table_name = ConstructTableName(type, type::TypeId::INVALID, tbl_cols, 0, num_rows, num_rows);
  query << "SELECT " << cols << " FROM  " << table_name << " WHERE " << predicate;
  auto f = std::bind(&ExecutionRunners::IndexScanChecker, this, num_col, std::placeholders::_1, std::placeholders::_2);
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units), f,
                                     common::ManagedPointer<std::vector<parser::ConstantValueExpression>>(&params),
                                     common::ManagedPointer<std::vector<type::TypeId>>(&param_types));
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true, &real_params);

  state.SetItemsProcessed(state.range(2));
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ2_1_IndexJoinRunners)(benchmark::State &state) {
  auto type = type::TypeId::INTEGER;
  auto tbl_cols = 15;
  auto key_num = state.range(0);
  auto outer = state.range(1);
  auto inner = state.range(2);
  auto is_build = state.range(3);

  if (rerun_start) {
    return;
  }

  // Don't run large if skipping
  if (settings.skip_large_rows_runs_ && outer >= settings.warmup_rows_limit_) {
    return;
  }

  if (outer == 0) {
    if (is_build < 0) {
      throw "Invalid is_build argument for IndexJoin";
    }

    HandleBuildDropIndex(is_build != 0, tbl_cols, inner, key_num, type);
    return;
  }

  // No warmup
  int num_iters = 1;
  auto type_size = type::TypeUtil::GetTypeTrueSize(type);
  auto tuple_size = type_size * key_num;

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;

  // We only ever emit min(outer, inner) # of tuples
  // Even though there are no matches, it still might be a good idea to see what the relation is
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT,
                         std::min(inner, outer), tuple_size, key_num, 0, 1, 0, 0, 0, 0);

  // Outer table scan happens
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, outer,
                         tuple_size, key_num, outer, 1, 0, 0, 0, 0);

  // For each in outer, match 1 tuple in inner
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::IDX_SCAN, inner,
                         tuple_size, key_num, 1, 1, outer, 0,
                         storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_UPPER_THRESHOLD,
                         storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_LOWER_THRESHOLD);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  auto cols = ConstructSQLClause(type, type::TypeId::INVALID, key_num, 0, ", ", "a", false, "");
  auto predicate = ConstructSQLClause(type, type::TypeId::INVALID, key_num, 0, " AND ", "a", true, "b");

  auto outer_tbl = ConstructTableName(type, type::TypeId::INVALID, tbl_cols, 0, outer, outer);
  auto inner_tbl = ConstructTableName(type, type::TypeId::INVALID, tbl_cols, 0, inner, inner);

  std::stringstream query;
  query << "SELECT " << cols << " FROM " << outer_tbl << " AS a, " << inner_tbl << " AS b WHERE " << predicate;
  auto f = std::bind(&ExecutionRunners::IndexNLJoinChecker, this, inner_tbl, key_num, std::placeholders::_1,
                     std::placeholders::_2);
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units), f);
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true);

  state.SetItemsProcessed(state.range(2));
}

void ExecutionRunners::ExecuteInsert(benchmark::State *state) {
  auto num_ints = state->range(0);
  auto num_reals = state->range(1);
  auto num_cols = state->range(2);
  auto num_rows = state->range(3);

  // TODO(wz2): Re-enable compiled inserts once runtime is sensible
  if (noisepage::runner::ExecutionRunners::mode == execution::vm::ExecutionMode::Compiled) return;

  if (rerun_start || (num_rows > settings.warmup_rows_limit_ && settings.skip_large_rows_runs_)) return;

  // Create temporary table schema
  std::vector<catalog::Schema::Column> cols;
  std::vector<std::pair<type::TypeId, int64_t>> info = {{type::TypeId::INTEGER, num_ints},
                                                        {type::TypeId::REAL, num_reals}};
  int col_no = 1;
  for (auto &i : info) {
    for (auto j = 1; j <= i.second; j++) {
      std::stringstream col_name;
      col_name << "col" << col_no++;
      if (i.first == type::TypeId::INTEGER) {
        cols.emplace_back(
            col_name.str(), i.first, false,
            noisepage::parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(0)));
      } else {
        cols.emplace_back(col_name.str(), i.first, false,
                          noisepage::parser::ConstantValueExpression(type::TypeId::REAL, execution::sql::Real(0.f)));
      }
    }
  }
  catalog::Schema tmp_schema(cols);

  // Create table
  catalog::table_oid_t tbl_oid;
  {
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    tbl_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "tmp_table", tmp_schema);
    auto &schema = accessor->GetSchema(tbl_oid);
    auto *tmp_table = new storage::SqlTable(db_main->GetStorageLayer()->GetBlockStore(), schema);
    accessor->SetTablePointer(tbl_oid, tmp_table);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  std::string tuple_row;
  {
    std::stringstream tuple;
    tuple << "(";
    for (uint32_t i = 1; i <= num_cols; i++) {
      tuple << i;
      if (i != num_cols) {
        tuple << ",";
      } else {
        tuple << ")";
      }
    }
    tuple_row = tuple.str();
  }

  // Hack to preallocate some memory
  std::string reserves;
  std::string query;
  query.reserve(tuple_row.length() * num_rows + num_rows + 100);

  query += "INSERT INTO tmp_table VALUES ";
  for (uint32_t idx = 0; idx < num_rows; idx++) {
    query += tuple_row;
    if (idx != num_rows - 1) {
      query += ", ";
    }
  }

  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto real_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::REAL);
  auto tuple_size = int_size * num_ints + real_size * num_reals;

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::INSERT, num_rows,
                         tuple_size, num_cols, num_rows, 1, 0, 0, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  auto num_iters = 1 + ((num_rows <= settings.warmup_rows_limit_) ? settings.warmup_iterations_num_ : 0);
  auto equery = OptimizeSqlStatement(query, std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true);

  // Drop the table
  {
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    accessor->DropTable(tbl_oid);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  InvokeGC();
  state->SetItemsProcessed(num_rows);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ6_0_InsertRunners)(benchmark::State &state) { ExecuteInsert(&state); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ6_1_InsertRunners)(benchmark::State &state) { ExecuteInsert(&state); }

void ExecutionRunners::ExecuteUpdate(benchmark::State *state) {
  auto num_integers = state->range(0);
  auto num_bigints = state->range(1);
  auto update_keys = state->range(2);
  auto tbl_ints = state->range(3);
  auto tbl_bigints = state->range(4);
  auto row = state->range(5);
  auto car = state->range(6);
  auto is_build = state->range(7);

  // A lookup size of 0 indicates a special query
  bool is_first_type = tbl_ints != 0;
  auto type = is_first_type ? (type::TypeId::INTEGER) : (type::TypeId::BIGINT);
  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto bigint_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::BIGINT);
  auto tuple_size = is_first_type ? (int_size * update_keys) : (bigint_size * update_keys);
  auto num_col = is_first_type ? num_integers : num_bigints;
  auto idx_size = is_first_type ? (int_size * num_col) : (bigint_size * num_col);
  if (car == 0) {
    if (is_build < 0) {
      throw "Invalid is_build argument for ExecuteUpdate";
    }

    HandleBuildDropIndex(is_build != 0, tbl_ints + tbl_bigints, row, num_col, type);
    return;
  }

  int num_iters = 1;
  if (car <= settings.warmup_rows_limit_) {
    num_iters += settings.warmup_iterations_num_;
  } else if (rerun_start || settings.skip_large_rows_runs_) {
    return;
  }

  // UPDATE [] SET [non-indexed columns] = [non-indexed clumns] WHERE [indexed cols]
  //
  // This will generate an UPDATE with an index scan child. Furthermore, the code-gen
  // code will not do a DELETE followed by an INSERT on the underlying table since
  // the UPDATE statement does not update any indexed columns.
  std::stringstream query;
  std::string tbl = ConstructTableName(type, type::TypeId::INVALID, tbl_ints + tbl_bigints, 0, row, row);
  query << "UPDATE " << tbl << " SET ";

  std::vector<catalog::Schema::Column> cols;
  {
    uint64_t limit = is_first_type ? tbl_ints : tbl_bigints;
    limit = std::min(limit, static_cast<uint64_t>(num_col + update_keys));
    auto type_name = type::TypeUtil::TypeIdToString(type);
    for (uint64_t j = num_col + 1; j <= limit; j++) {
      query << type_name << j << " = " << type_name << j;
      if (j != limit) query << ", ";
    }
  }

  std::vector<std::vector<parser::ConstantValueExpression>> real_params;
  std::pair<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::OutputSchema>> equery;
  auto cost = std::make_unique<optimizer::TrivialCostModel>();

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::UPDATE, car,
                         tuple_size, update_keys, car, 1, 0, 0, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::IDX_SCAN, row,
                         idx_size, num_col, car, 1, 0, 0,
                         storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_UPPER_THRESHOLD,
                         storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_LOWER_THRESHOLD);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::vector<parser::ConstantValueExpression> params;
  std::vector<type::TypeId> param_types;
  params.emplace_back(type, execution::sql::Integer(0));
  param_types.push_back(type);
  if (car > 1) {
    params.emplace_back(type, execution::sql::Integer(0));
    param_types.push_back(type);
  }

  GenIdxScanParameters(type, row, car, num_iters, &real_params);
  std::string predicate = ConstructIndexScanPredicate(type, num_col, car);
  query << " WHERE " << predicate;

  auto f = std::bind(&ExecutionRunners::UpdateIndexScanChecker, this, std::placeholders::_1, std::placeholders::_2);
  equery = OptimizeSqlStatement(query.str(), std::move(cost), std::move(units), f,
                                common::ManagedPointer<std::vector<parser::ConstantValueExpression>>(&params),
                                common::ManagedPointer<std::vector<type::TypeId>>(&param_types));

  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), false, &real_params);
  state->SetItemsProcessed(row);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ7_2_UpdateRunners)(benchmark::State &state) { ExecuteUpdate(&state); }

void ExecutionRunners::ExecuteDelete(benchmark::State *state) {
  auto num_integers = state->range(0);
  auto num_bigints = state->range(1);
  auto tbl_ints = state->range(2);
  auto tbl_bigints = state->range(3);
  auto row = state->range(4);
  auto car = state->range(5);
  auto is_build = state->range(6);

  if (row == 0) {
    state->SetItemsProcessed(row);
    InvokeGC();
    return;
  }

  // A lookup size of 0 indicates a special query
  auto type = tbl_ints != 0 ? (type::TypeId::INTEGER) : (type::TypeId::BIGINT);
  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto bigint_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::BIGINT);
  auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
  auto num_col = num_integers + num_bigints;
  auto tbl_col = tbl_ints + tbl_bigints;
  auto tbl_size = tbl_ints * int_size + tbl_bigints * bigint_size;
  if (car == 0) {
    if (is_build < 0) {
      throw "Invalid is_build argument for ExecuteDelete";
    }

    HandleBuildDropIndex(is_build != 0, tbl_col, row, num_col, type);
    return;
  }

  int num_iters = 1;
  if (car <= settings.warmup_rows_limit_) {
    num_iters += settings.warmup_iterations_num_;
  } else if (rerun_start || settings.skip_large_rows_runs_) {
    return;
  }

  std::stringstream query;
  std::vector<std::vector<parser::ConstantValueExpression>> real_params;
  std::pair<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::OutputSchema>> equery;
  auto cost = std::make_unique<optimizer::TrivialCostModel>();

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::DELETE, car, tbl_size,
                         tbl_col, car, 1, 0, 0, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::INDEX_DELETE, row,
                         tuple_size, num_col, car, 1, 0, 0,
                         storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_UPPER_THRESHOLD,
                         storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_LOWER_THRESHOLD);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::IDX_SCAN, row,
                         tuple_size, num_col, car, 1, 0, 0,
                         storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_UPPER_THRESHOLD,
                         storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_LOWER_THRESHOLD);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::vector<parser::ConstantValueExpression> params;
  std::vector<type::TypeId> param_types;
  params.emplace_back(type, execution::sql::Integer(0));
  param_types.push_back(type);
  if (car > 1) {
    params.emplace_back(type, execution::sql::Integer(0));
    param_types.push_back(type);
  }

  GenIdxScanParameters(type, row, car, num_iters, &real_params);
  std::string predicate = ConstructIndexScanPredicate(type, num_col, car);
  std::string tbl = ConstructTableName(type, type::TypeId::INVALID, tbl_col, 0, row, row);
  query << "DELETE FROM " << tbl << " WHERE " << predicate;

  auto f = std::bind(&ExecutionRunners::ChildIndexScanChecker, this, std::placeholders::_1, std::placeholders::_2);
  equery = OptimizeSqlStatement(query.str(), std::move(cost), std::move(units), f,
                                common::ManagedPointer<std::vector<parser::ConstantValueExpression>>(&params),
                                common::ManagedPointer<std::vector<type::TypeId>>(&param_types));

  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), false, &real_params);
  state->SetItemsProcessed(row);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ8_2_DeleteRunners)(benchmark::State &state) { ExecuteDelete(&state); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ3_SortRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_reals = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_reals = state.range(3);
  auto row = state.range(4);
  auto car = state.range(5);
  auto is_topk = state.range(6);

  int num_iters = 1;
  if (row <= settings.warmup_rows_limit_) {
    num_iters += settings.warmup_iterations_num_;
  } else if (rerun_start || settings.skip_large_rows_runs_) {
    return;
  }

  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto real_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::REAL);
  auto tuple_size = int_size * num_integers + real_size * num_reals;
  auto num_col = num_integers + num_reals;

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  selfdriving::ExecutionOperatingUnitFeatureVector pipe1_vec;
  auto table_car = car;
  auto output_num = row;
  selfdriving::ExecutionOperatingUnitType build_ou_type = selfdriving::ExecutionOperatingUnitType::SORT_BUILD;
  if (is_topk == 1) {
    build_ou_type = selfdriving::ExecutionOperatingUnitType::SORT_TOPK_BUILD;
    table_car = row;
    output_num = car;
  }
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, row,
                         tuple_size, num_col, table_car, 1, 0, 0, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), build_ou_type, row, tuple_size, num_col, car, 1, 0, 0, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SORT_ITERATE,
                         output_num, tuple_size, num_col, car, 1, 0, 0, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, output_num,
                         tuple_size, num_col, 0, 1, 0, 0, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  std::stringstream query;
  auto cols =
      ConstructSQLClause(type::TypeId::INTEGER, type::TypeId::REAL, num_integers, num_reals, ", ", "", false, "");
  auto tbl_name = ConstructTableName(type::TypeId::INTEGER, type::TypeId::REAL, tbl_ints, tbl_reals, row, table_car);
  query << "SELECT " << (cols) << " FROM " << tbl_name << " ORDER BY " << (cols);
  if (is_topk == 1) query << " LIMIT " << car;
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true);
  state.SetItemsProcessed(row);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ4_HashJoinSelfRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_bigints = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_bigints = state.range(3);
  auto row = state.range(4);
  auto car = state.range(5);

  if (rerun_start) {
    return;
  }

  if (settings.skip_large_rows_runs_ && row > settings.warmup_rows_limit_) {
    return;
  }

  // Size of the scan tuple
  // Size of hash key size, probe key size
  // Size of output since only output 1 side
  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto bigint_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::BIGINT);
  auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
  auto num_col = num_integers + num_bigints;

  auto hj_output = row * row / car;
  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  selfdriving::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, row,
                         tuple_size, num_col, car, 1, 0, 0, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::HASHJOIN_BUILD, row,
                         tuple_size, num_col, car, 1, 0, 0, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, row,
                         tuple_size, num_col, car, 1, 0, 0, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::HASHJOIN_PROBE, row,
                         tuple_size, num_col, hj_output, 1, 0, 0, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, hj_output,
                         tuple_size, num_col, 0, 1, 0, 0, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  std::stringstream query;
  auto tbl_name = ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, tbl_bigints, row, car);
  auto cols =
      ConstructSQLClause(type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints, ", ", "b", false, "");
  auto predicate = ConstructSQLClause(type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints, " AND ",
                                      tbl_name, true, "b");
  query << "SELECT " << cols << " FROM " << tbl_name << ", " << tbl_name << " as b WHERE " << predicate;
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::ForcedCostModel>(true), std::move(units));
  BenchmarkExecQuery(1, equery.first.get(), equery.second.get(), true);
  state.SetItemsProcessed(row);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ4_HashJoinNonSelfRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_bigints = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_bigints = state.range(3);
  auto build_row = state.range(4);
  auto build_car = state.range(5);
  auto probe_row = state.range(6);
  auto probe_car = state.range(7);
  auto matched_car = state.range(8);

  if (rerun_start) {
    return;
  }

  if (settings.skip_large_rows_runs_ && probe_row > settings.warmup_rows_limit_) {
    return;
  }

  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto bigint_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::BIGINT);
  auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
  auto num_col = num_integers + num_bigints;

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  selfdriving::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, build_row,
                         tuple_size, num_col, build_car, 1, 0, 0, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::HASHJOIN_BUILD,
                         build_row, tuple_size, num_col, build_car, 1, 0, 0, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, probe_row,
                         tuple_size, num_col, probe_car, 1, 0, 0, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::HASHJOIN_PROBE,
                         probe_row, tuple_size, num_col, matched_car, 1, 0, 0, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, matched_car,
                         tuple_size, num_col, 0, 1, 0, 0, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  auto build_tbl =
      ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, tbl_bigints, build_row, build_car);
  auto probe_tbl =
      ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, tbl_bigints, probe_row, probe_car);

  std::stringstream query;
  auto cols =
      ConstructSQLClause(type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints, ", ", "b", false, "");
  auto predicate = ConstructSQLClause(type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints, " AND ",
                                      build_tbl, true, "b");
  query << "SELECT " << cols << " FROM " << build_tbl << ", " << probe_tbl << " as b WHERE " << predicate;

  auto f =
      std::bind(&ExecutionRunners::JoinNonSelfChecker, this, build_tbl, std::placeholders::_1, std::placeholders::_2);
  auto cost = std::make_unique<optimizer::ForcedCostModel>(true);
  auto equery = OptimizeSqlStatement(query.str(), std::move(cost), std::move(units), f);
  BenchmarkExecQuery(1, equery.first.get(), equery.second.get(), true);
  state.SetItemsProcessed(matched_car);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ5_0_AggregateRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_varchars = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_varchars = state.range(3);
  auto row = state.range(4);
  auto car = state.range(5);

  int num_iters = 1;
  if (row <= settings.warmup_rows_limit_ && car <= settings.warmup_rows_limit_) {
    num_iters += settings.warmup_iterations_num_;
  } else if (rerun_start || settings.skip_large_rows_runs_) {
    return;
  }

  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  size_t tuple_size = int_size * num_integers;
  size_t num_col = num_integers + num_varchars;
  for (auto i = 0; i < num_varchars; i++) {
    selfdriving::OperatingUnitRecorder::AdjustKeyWithType(type::TypeId::VARCHAR, &tuple_size, &num_col);
  }
  auto out_cols = num_col + 1;     // pulling the count(*) out
  auto out_size = tuple_size + 4;  // count(*) is an integer

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  selfdriving::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, row,
                         tuple_size, num_col, car, 1, 0, 0, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::AGGREGATE_BUILD, row,
                         tuple_size, num_col, car, 1, 0, 0, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::AGGREGATE_ITERATE, car,
                         out_size, out_cols, car, 1, 0, 0, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, car, out_size,
                         out_cols, 0, 1, 0, 0, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  std::stringstream query;
  auto cols =
      ConstructSQLClause(type::TypeId::INTEGER, type::TypeId::VARCHAR, num_integers, num_varchars, ", ", "", false, "");
  auto tbl_name = ConstructTableName(type::TypeId::INTEGER, type::TypeId::VARCHAR, tbl_ints, tbl_varchars, row, car);
  query << "SELECT COUNT(*), " << cols << " FROM " << tbl_name << " GROUP BY " << cols;
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true);
  state.SetItemsProcessed(row);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ5_1_AggregateRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto tbl_ints = state.range(1);
  auto row = state.range(2);
  auto car = state.range(3);

  int num_iters = 1;
  if (row <= settings.warmup_rows_limit_ && car <= settings.warmup_rows_limit_) {
    num_iters += settings.warmup_iterations_num_;
  } else if (rerun_start || settings.skip_large_rows_runs_) {
    return;
  }

  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto tuple_size = int_size * num_integers;
  auto num_col = num_integers;
  auto out_cols = num_col;
  auto out_size = tuple_size;

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  selfdriving::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, row,
                         tuple_size, num_col, car, 1, 0, 0, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::AGGREGATE_BUILD, row,
                         0, num_col, 1, 1, 0, 0, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::AGGREGATE_ITERATE, 1,
                         out_size, out_cols, 1, 1, 0, 0, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, 1, out_size,
                         out_cols, 0, 1, 0, 0, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  std::stringstream query;
  auto cols = ConstructSQLClause(type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, 0, ", ", "", false, "");
  auto tbl_name = ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, 0, row, car);

  {
    query << "SELECT ";
    for (int i = 1; i <= num_integers; i++) {
      query << "SUM(" << (type::TypeUtil::TypeIdToString(type::TypeId::INTEGER)) << i << ")";
      if (i != num_integers) query << ", ";
    }

    query << " FROM " << tbl_name;
  }

  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true);
  state.SetItemsProcessed(row);
}

void ExecutionRunners::ExecuteCreateIndex(benchmark::State *state) {
  auto num_integers = state->range(0);
  auto num_mix = state->range(1);
  auto tbl_ints = state->range(2);
  auto tbl_mix = state->range(3);
  auto row = state->range(4);
  auto car = state->range(5);
  auto varchar_mix = state->range(6);
  auto num_threads = state->range(7);

  if (rerun_start || (row > settings.warmup_rows_limit_ && settings.skip_large_rows_runs_)) {
    return;
  }

  // Only generate counters if executing in parallel
  auto exec_settings = GetParallelExecutionSettings(num_threads, num_threads != 0);
  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  type::TypeId mix_type;
  if (varchar_mix == 1)
    mix_type = type::TypeId::VARCHAR;
  else
    mix_type = type::TypeId::BIGINT;
  size_t tuple_size = int_size * num_integers;
  size_t num_col = num_integers + num_mix;

  // Adjust for type of MIX
  for (auto i = 0; i < num_mix; i++) {
    selfdriving::OperatingUnitRecorder::AdjustKeyWithType(mix_type, &tuple_size, &num_col);
  }

  auto cols = ConstructSQLClause(type::TypeId::INTEGER, mix_type, num_integers, num_mix, ", ", "", false, "");
  auto tbl_name = ConstructTableName(type::TypeId::INTEGER, mix_type, tbl_ints, tbl_mix, row, car);

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::CREATE_INDEX, row,
                         tuple_size, num_col, car, 1, 0, num_threads,
                         storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_UPPER_THRESHOLD,
                         storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_LOWER_THRESHOLD);
  if (num_threads != 0) {
    pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::CREATE_INDEX_MAIN,
                           row, tuple_size, num_col, car, 1, 0, num_threads,
                           storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_UPPER_THRESHOLD,
                           storage::index::BPlusTreeBase::DEFAULT_INNER_NODE_SIZE_LOWER_THRESHOLD);
  }
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::stringstream query;
  std::string idx_name("runner_idx");
  query << "CREATE INDEX " << idx_name << " ON " << tbl_name << " USING BPLUSTREE (" << cols << ")";
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units),
                                     PassthroughPlanChecker, nullptr, nullptr, &exec_settings);
  BenchmarkExecQuery(1, equery.first.get(), equery.second.get(), true, &empty_params, &exec_settings);

  { DropIndexByName(idx_name); }

  InvokeGC();
  state->SetItemsProcessed(row);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ9_0_CreateIndexRunners)(benchmark::State &state) { ExecuteCreateIndex(&state); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ExecutionRunners, SEQ9_1_CreateIndexRunners)(benchmark::State &state) { ExecuteCreateIndex(&state); }

void InitializeRunnersState() {
  std::unordered_map<settings::Param, settings::ParamInfo> param_map;
  settings::SettingsManager::ConstructParamMap(param_map);

  size_t limit = 1000000000;
  auto sql_val = execution::sql::Integer(limit);
  auto sql_false = execution::sql::BoolVal(false);
  param_map.find(settings::Param::block_store_size)->second.value_ =
      parser::ConstantValueExpression(type::TypeId::INTEGER, sql_val);
  param_map.find(settings::Param::block_store_reuse)->second.value_ =
      parser::ConstantValueExpression(type::TypeId::INTEGER, sql_val);
  param_map.find(settings::Param::record_buffer_segment_size)->second.value_ =
      parser::ConstantValueExpression(type::TypeId::INTEGER, sql_val);
  param_map.find(settings::Param::record_buffer_segment_reuse)->second.value_ =
      parser::ConstantValueExpression(type::TypeId::INTEGER, sql_val);
  param_map.find(settings::Param::block_store_size)->second.max_value_ = limit;
  param_map.find(settings::Param::block_store_reuse)->second.max_value_ = limit;
  param_map.find(settings::Param::record_buffer_segment_size)->second.max_value_ = limit;
  param_map.find(settings::Param::record_buffer_segment_reuse)->second.max_value_ = limit;

  // Set Network Port
  param_map.find(settings::Param::port)->second.value_ = parser::ConstantValueExpression(
      type::TypeId::INTEGER, execution::sql::Integer(noisepage::runner::settings.port_));

  // Need to disable metrics thread
  param_map.find(settings::Param::use_metrics_thread)->second.value_ =
      parser::ConstantValueExpression(type::TypeId::BOOLEAN, sql_false);

  // Need to disable WAL
  param_map.find(settings::Param::wal_enable)->second.value_ =
      parser::ConstantValueExpression(type::TypeId::BOOLEAN, sql_false);

  auto db_main_builder = DBMain::Builder()
                             .SetUseGC(true)
                             .SetUseCatalog(true)
                             .SetUseStatsStorage(true)
                             .SetUseMetrics(true)
                             .SetBlockStoreSize(1000000000)
                             .SetBlockStoreReuse(1000000000)
                             .SetRecordBufferSegmentSize(1000000000)
                             .SetRecordBufferSegmentReuse(1000000000)
                             .SetUseExecution(true)
                             .SetUseTrafficCop(true)
                             .SetUseNetwork(true)
                             .SetUseSettingsManager(true)
                             .SetSettingsParameterMap(std::move(param_map))
                             .SetNetworkPort(noisepage::runner::settings.port_);

  db_main = db_main_builder.Build().release();

  auto block_store = db_main->GetStorageLayer()->GetBlockStore();
  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
  DbMainSetParam<settings::Param::pipeline_metrics_enable, bool>(true);
  DbMainSetParam<settings::Param::pipeline_metrics_sample_rate, int>(100);

  // Create the database
  auto txn = txn_manager->BeginTransaction();
  db_oid = catalog->CreateDatabase(common::ManagedPointer(txn), "test_db", true);

  // Load the database
  auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
  auto exec_settings = ExecutionRunners::GetExecutionSettings();
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      db_oid, common::ManagedPointer(txn), nullptr, nullptr, common::ManagedPointer(accessor), exec_settings,
      db_main->GetMetricsManager(), DISABLED, DISABLED);

  execution::sql::TableGenerator table_gen(exec_ctx.get(), block_store, accessor->GetDefaultNamespace());
  table_gen.GenerateExecutionRunnersData(settings, config);

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  InvokeGC();

  auto network_layer = noisepage::runner::db_main->GetNetworkLayer();
  auto server = network_layer->GetServer();
  server->RunServer();
}

void EndRunnersState() {
  noisepage::execution::ExecutionUtil::ShutdownTPL();
  db_main->GetMetricsManager()->Aggregate();
  db_main->GetMetricsManager()->ToOutput(nullptr);
  // free db main here so we don't need to use the loggers anymore
  delete db_main;
}

void RegisterRunners() {
  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ0_ArithmeticRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenArithArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ0_OutputRunners)
      ->Unit(benchmark::kMillisecond)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenOutputArguments>)
      ->Iterations(1);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ1_0_SeqScanRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenScanArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ1_1_SeqScanRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenScanMixedArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ2_0_IndexScanRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenIdxScanArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ2_1_IndexJoinRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenIdxJoinArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ3_SortRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenSortArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ4_HashJoinSelfRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenJoinSelfArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ4_HashJoinNonSelfRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenJoinNonSelfArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ5_0_AggregateRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenAggregateArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ5_1_AggregateRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenAggregateKeylessArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ6_0_InsertRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenInsertArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ6_1_InsertRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenInsertArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ7_2_UpdateRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenUpdateIndexArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ8_2_DeleteRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenDeleteIndexArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ9_0_CreateIndexRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenCreateIndexArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ9_1_CreateIndexRunners)
      ->Unit(benchmark::kMillisecond)
      ->Iterations(1)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenCreateIndexMixedArguments>);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ10_IndexInsertRunners)
      ->Unit(benchmark::kMillisecond)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenIndexInsertDeleteArguments>)
      ->Iterations(1);

  BENCHMARK_REGISTER_F(ExecutionRunners, SEQ11_IndexDeleteRunners)
      ->Unit(benchmark::kMillisecond)
      ->Apply(GenBenchmarkArguments<ExecutionRunnersArgumentGenerator::GenIndexInsertDeleteArguments>)
      ->Iterations(1);
}

}  // namespace noisepage::runner

/**
 * Bool for server ready
 */
bool network_queries_ready = false;

/**
 * Bool for network queries finished
 */
bool network_queries_finished = false;

/**
 * Mutex for access to above two booleans
 */
std::mutex network_queries_mutex;

/**
 * CVar for signalling
 */
std::condition_variable network_queries_cv;

using NetworkWorkFunction = std::function<void(pqxx::work *)>;

void RunNetworkQueries(const NetworkWorkFunction &work) {
  // GC does not run in a background thread!
  {
    std::unique_lock<std::mutex> lk(network_queries_mutex);
    network_queries_cv.wait(lk, [] { return network_queries_ready; });
  }

  std::string conn;
  {
    std::stringstream conn_ss;
    conn_ss << "postgresql://127.0.0.1:" << (noisepage::runner::settings.port_) << "/test_db";
    conn = conn_ss.str();
  }

  // Execute network queries
  try {
    pqxx::connection c{conn};
    pqxx::work txn{c};

    work(&txn);

    txn.commit();
  } catch (std::exception &e) {
  }

  {
    std::unique_lock<std::mutex> lk(network_queries_mutex);
    network_queries_finished = true;
    network_queries_cv.notify_one();
  }
}

void RunNetworkSequence(const NetworkWorkFunction &work) {
  noisepage::runner::db_main->GetMetricsManager()->Aggregate();
  noisepage::runner::db_main->GetMetricsManager()->ToOutput(nullptr);
  noisepage::runner::InvokeGC();

  auto thread = std::thread([=] { RunNetworkQueries(work); });

  {
    std::unique_lock<std::mutex> lk(network_queries_mutex);
    network_queries_ready = true;
    network_queries_cv.notify_one();
    network_queries_cv.wait(lk, [] { return network_queries_finished; });
  }

  noisepage::runner::db_main->GetMetricsManager()->Aggregate();
  noisepage::runner::db_main->GetMetricsManager()->ToOutput(nullptr);
  noisepage::runner::InvokeGC();

  thread.join();
}

void Shutdown() {
  noisepage::runner::db_main->ForceShutdown();
  delete noisepage::runner::db_main;
}

void RunBenchmarkSequence(int rerun_counter) {
  // As discussed, certain runners utilize multiple features.
  // In order for the modeller to work correctly, we first need to model
  // the dependent features and then subtract estimations/exact counters
  // from the composite to get an approximation for the target feature.
  std::vector<std::vector<std::string>> filters = {{"SEQ0"},
                                                   {"SEQ1_0", "SEQ1_1"},
                                                   {"SEQ2_0", "SEQ2_1"},
                                                   {"SEQ3"},
                                                   {"SEQ4"},
                                                   {"SEQ5_0", "SEQ5_1"},
                                                   {"SEQ6_0", "SEQ6_1"},
                                                   {"SEQ10"},
                                                   {"SEQ7_2"},
                                                   {"SEQ11"},
                                                   {"SEQ8_2"},
                                                   {"SEQ9_0", "SEQ9_1"}};
  std::vector<std::string> titles = {"OUTPUT", "SCANS",        "IDX_SCANS", "SORTS",        "HJ",     "AGGS",
                                     "INSERT", "INDEX_INSERT", "UPDATE",    "INDEX_DELETE", "DELETE", "CREATE_INDEX"};

  char buffer[64];
  const char *argv[2];
  argv[0] = "execution_runners";
  argv[1] = buffer;

  auto vm_modes = {noisepage::execution::vm::ExecutionMode::Interpret,
                   noisepage::execution::vm::ExecutionMode::Compiled};
  for (size_t i = 0; i < filters.size(); i++) {
    for (auto &filter : filters[i]) {
      for (auto mode : vm_modes) {
        noisepage::runner::ExecutionRunners::mode = mode;

        int argc = 2;
        snprintf(buffer, sizeof(buffer), "--benchmark_filter=%s", filter.c_str());
        benchmark::Initialize(&argc, const_cast<char **>(argv));
        benchmark::RunSpecifiedBenchmarks();
        std::this_thread::sleep_for(std::chrono::seconds(2));
        noisepage::runner::InvokeGC();
      }
    }

    std::this_thread::sleep_for(std::chrono::seconds(2));
    noisepage::runner::db_main->GetMetricsManager()->Aggregate();
    noisepage::runner::db_main->GetMetricsManager()->ToOutput(nullptr);

    if (!noisepage::runner::rerun_start) {
      snprintf(buffer, sizeof(buffer), "execution_%s.csv", titles[i].c_str());
    } else {
      snprintf(buffer, sizeof(buffer), "execution_%s_%d.csv", titles[i].c_str(), rerun_counter);
    }

    std::rename("pipeline.csv", buffer);
  }
}

void RunExecutionRunners() {
  noisepage::runner::rerun_start = false;
  for (int i = 0; i <= noisepage::runner::settings.rerun_iterations_; i++) {
    noisepage::runner::rerun_start = (i != 0);
    RunBenchmarkSequence(i);
  }

  for (int i = 0; i <= noisepage::runner::settings.rerun_iterations_; i++) {
    RunNetworkSequence(noisepage::runner::NetworkQueriesOutputRunners);
  }

  std::rename("pipeline.csv", "execution_NETWORK.csv");

  // Do post-processing
  std::vector<std::string> titles = {"OUTPUT", "SCANS",        "IDX_SCANS", "SORTS",        "HJ",     "AGGS",
                                     "INSERT", "INDEX_INSERT", "UPDATE",    "INDEX_DELETE", "DELETE", "CREATE_INDEX"};
  std::vector<std::string> adjusts = {"0", "1_0", "1_1", "2", "3", "4", "5_0", "5_1", "6", "7_0", "7_1", "8"};
  for (size_t t = 0; t < titles.size(); t++) {
    auto &title = titles[t];
    char target[64];
    snprintf(target, sizeof(target), "execution_%s.csv", title.c_str());

    for (int i = 1; i <= noisepage::runner::settings.rerun_iterations_; i++) {
      char source[64];
      snprintf(source, sizeof(target), "execution_%s_%d.csv", title.c_str(), i);

      std::string csv_line;
      std::ofstream of(target, std::ios_base::binary | std::ios_base::app);
      std::ifstream is(source, std::ios_base::binary);
      std::getline(is, csv_line);

      // Concat rest of data file
      of.seekp(0, std::ios_base::end);
      of << is.rdbuf();

      // Delete the _%d file
      std::remove(source);
    }

    char adjust[64];
    snprintf(adjust, sizeof(adjust), "execution_SEQ%s.csv", adjusts[t].c_str());
    std::rename(target, adjust);
  }

  {
    std::ifstream ifile("execution_NETWORK.csv");
    std::ofstream ofile("execution_SEQ0.csv", std::ios::app);

    std::string dummy;
    std::getline(ifile, dummy);
    ofile << ifile.rdbuf();
  }
  std::remove("execution_NETWORK.csv");
}

int main(int argc, char **argv) {
  // Initialize execution-runner arguments
  noisepage::runner::settings.InitializeFromArguments(argc, argv);

  // Initialize Benchmarks
  noisepage::runner::RegisterRunners();

  // Benchmark Config Environment Variables
  // Check whether we are being passed environment variables to override configuration parameter
  // for this benchmark run.
  const char *env_num_threads = std::getenv(noisepage::ENV_NUM_THREADS);
  if (env_num_threads != nullptr) noisepage::BenchmarkConfig::num_threads = atoi(env_num_threads);

  const char *env_logfile_path = std::getenv(noisepage::ENV_LOGFILE_PATH);
  if (env_logfile_path != nullptr) noisepage::BenchmarkConfig::logfile_path = std::string_view(env_logfile_path);

  noisepage::runner::InitializeRunnersState();
  if (noisepage::runner::settings.generate_test_data_) {
    RunNetworkSequence(noisepage::runner::NetworkQueriesCreateIndexRunners);
    std::rename("pipeline.csv", "execution_TEST_DATA.csv");
  } else {
    if (noisepage::runner::settings.target_runner_specified_) {
      // Pass straight through to gbenchmark
      benchmark::Initialize(&argc, argv);
      benchmark::RunSpecifiedBenchmarks();
      noisepage::runner::EndRunnersState();
    } else {
      RunExecutionRunners();
      Shutdown();
    }
  }

  noisepage::LoggersUtil::ShutDown();

  return 0;
}
