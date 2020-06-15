#include "execution/tpl.h"

#include <gflags/gflags.h>
#include <tbb/task_scheduler_init.h>
#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "execution/ast/ast_dump.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/output.h"
// This is needed because one of the header files above uses boolean.h on OSX which
// redefines TRUE to 1 and FALSE to 0, which causes issues.
#undef TRUE
#undef FALSE
#include "execution/parsing/parser.h"
#include "execution/parsing/scanner.h"
#include "execution/sema/error_reporter.h"
#include "execution/sema/sema.h"
#include "execution/sql/memory_pool.h"
#include "execution/sql/value.h"
#include "execution/sql/value_util.h"
#include "execution/table_generator/sample_output.h"
#include "execution/table_generator/table_generator.h"
#include "execution/util/cpu_info.h"
#include "execution/util/timer.h"
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/bytecode_module.h"
#include "execution/vm/llvm_engine.h"
#include "execution/vm/module.h"
#include "execution/vm/vm.h"
#include "llvm/Support/CommandLine.h"
#include "llvm/Support/MemoryBuffer.h"
#include "loggers/execution_logger.h"
#include "loggers/loggers_util.h"
#include "main/db_main.h"
#include "metrics/metrics_thread.h"
#include "parser/expression/constant_value_expression.h"
#include "settings/settings_manager.h"
#include "storage/garbage_collector.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/timestamp_manager.h"

// ---------------------------------------------------------
// CLI options
// ---------------------------------------------------------

llvm::cl::OptionCategory tpl_options_category("TPL Compiler Options",
                                              "Options for controlling the TPL compilation process.");
llvm::cl::opt<std::string> input_file(llvm::cl::Positional, llvm::cl::desc("<input file>"), llvm::cl::init(""),
                                      llvm::cl::cat(tpl_options_category));
llvm::cl::opt<bool> print_ast("print-ast", llvm::cl::desc("Print the programs AST"),
                              llvm::cl::cat(tpl_options_category));
llvm::cl::opt<bool> print_tbc("print-tbc", llvm::cl::desc("Print the generated TPL Bytecode"),
                              llvm::cl::cat(tpl_options_category));
llvm::cl::opt<std::string> output_name("output-name", llvm::cl::desc("Print the output name"),
                                       llvm::cl::init("schema10"), llvm::cl::cat(tpl_options_category));
llvm::cl::opt<bool> is_sql("sql", llvm::cl::desc("Is the input a SQL query?"), llvm::cl::cat(tpl_options_category));
llvm::cl::opt<bool> is_mini_runner("mini-runner", llvm::cl::desc("Is this used for the mini runner?"),
                                   llvm::cl::cat(tpl_options_category));

tbb::task_scheduler_init scheduler;

namespace terrier::execution {

static constexpr const char *K_EXIT_KEYWORD = ".exit";

/**
 * Compile the TPL source in \a source and run it in both interpreted and JIT
 * compiled mode
 * @param source The TPL source
 * @param name The name of the module/program
 */
static void CompileAndRun(const std::string &source, const std::string &name = "tmp-tpl") {
  // Initialize terrier objects
  auto db_main_builder = terrier::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).SetUseGCThread(true);
  if (is_mini_runner) {
    db_main_builder.SetUseMetrics(true)
        .SetUseMetricsThread(true)
        .SetBlockStoreSize(1000000)
        .SetBlockStoreReuse(1000000)
        .SetRecordBufferSegmentSize(1000000)
        .SetRecordBufferSegmentReuse(1000000);
  }
  auto db_main = db_main_builder.Build();

  if (is_mini_runner) {
    auto metrics_manager = db_main->GetMetricsManager();
    metrics_manager->EnableMetric(metrics::MetricsComponent::EXECUTION, 0);
    metrics_manager->RegisterThread();
  }

  // Get the correct output format for this test
  exec::SampleOutput sample_output;
  sample_output.InitTestOutput();
  auto output_schema = sample_output.GetSchema(output_name.data());

  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

  auto *txn = txn_manager->BeginTransaction();

  auto db_oid = catalog->CreateDatabase(common::ManagedPointer(txn), "test_db", true);
  auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
  auto ns_oid = accessor->GetDefaultNamespace();

  // Make the execution context
  exec::OutputPrinter printer(output_schema);
  exec::ExecutionContext exec_ctx{db_oid, common::ManagedPointer(txn), printer, output_schema,
                                  common::ManagedPointer(accessor)};
  // Add dummy parameters for tests
  std::vector<parser::ConstantValueExpression> params;
  params.emplace_back(type::TypeId::INTEGER, sql::Integer(37));
  params.emplace_back(type::TypeId::DECIMAL, sql::Real(37.73));
  params.emplace_back(type::TypeId::DATE, sql::DateVal(sql::Date::FromYMD(1937, 3, 7)));
  auto string_val = sql::ValueUtil::CreateStringVal(std::string_view("37 Strings"));
  params.emplace_back(type::TypeId::VARCHAR, string_val.first, std::move(string_val.second));
  exec_ctx.SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&params));

  // Generate test tables
  sql::TableGenerator table_generator{&exec_ctx, db_main->GetStorageLayer()->GetBlockStore(), ns_oid};
  table_generator.GenerateTestTables(is_mini_runner);
  // Comment out to make more tables available at runtime
  // table_generator.GenerateTPCHTables(<path_to_tpch_dir>);
  // table_generator.GenerateTableFromFile(<path_to_schema>, <path_to_data>);

  // Let's scan the source
  util::Region region("repl-ast");
  util::Region error_region("repl-error");
  sema::ErrorReporter error_reporter(&error_region);
  ast::Context context(&region, &error_reporter);

  parsing::Scanner scanner(source.data(), source.length());
  parsing::Parser parser(&scanner, &context);

  double parse_ms = 0.0, typecheck_ms = 0.0, codegen_ms = 0.0, interp_exec_ms = 0.0, adaptive_exec_ms = 0.0,
         jit_exec_ms = 0.0;

  //
  // Parse
  //

  ast::AstNode *root;
  {
    util::ScopedTimer<std::milli> timer(&parse_ms);
    root = parser.Parse();
  }

  if (error_reporter.HasErrors()) {
    EXECUTION_LOG_ERROR("Parsing errors: \n {}", error_reporter.SerializeErrors());
    throw std::runtime_error("Parsing Error!");
  }

  //
  // Type check
  //

  {
    util::ScopedTimer<std::milli> timer(&typecheck_ms);
    sema::Sema type_check(&context);
    type_check.Run(root);
  }

  if (error_reporter.HasErrors()) {
    EXECUTION_LOG_ERROR("Type-checking errors: \n {}", error_reporter.SerializeErrors());
    throw std::runtime_error("Type Checking Error!");
  }

  // Dump AST
  if (print_ast) {
    EXECUTION_LOG_INFO("\n{}", ast::AstDump::Dump(root));
  }

  //
  // TBC generation
  //

  std::unique_ptr<vm::BytecodeModule> bytecode_module;
  {
    util::ScopedTimer<std::milli> timer(&codegen_ms);
    bytecode_module = vm::BytecodeGenerator::Compile(root, &exec_ctx, name);
  }

  // Dump Bytecode
  if (print_tbc) {
    std::stringstream ss;
    bytecode_module->PrettyPrint(&ss);
    EXECUTION_LOG_INFO("\n{}", ss.str());
  }

  auto module = std::make_unique<vm::Module>(std::move(bytecode_module));

  //
  // Interpret
  //

  {
    exec_ctx.SetExecutionMode(static_cast<uint8_t>(vm::ExecutionMode::Interpret));
    util::ScopedTimer<std::milli> timer(&interp_exec_ms);

    if (is_sql) {
      std::function<int64_t(exec::ExecutionContext *)> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Interpret, &main)) {
        EXECUTION_LOG_ERROR(
            "Missing 'main' entry function with signature "
            "(*ExecutionContext)->int64");
        return;
      }
      EXECUTION_LOG_INFO("VM main() returned: {}", main(&exec_ctx));
    } else {
      std::function<int64_t()> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Interpret, &main)) {
        EXECUTION_LOG_ERROR("Missing 'main' entry function with signature ()->int64");
        return;
      }
      EXECUTION_LOG_INFO("VM main() returned: {}", main());
    }
  }

  //
  // Adaptive
  //

  if (!is_mini_runner) {
    exec_ctx.SetExecutionMode(static_cast<uint8_t>(vm::ExecutionMode::Adaptive));
    util::ScopedTimer<std::milli> timer(&adaptive_exec_ms);

    if (is_sql) {
      std::function<int64_t(exec::ExecutionContext *)> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Adaptive, &main)) {
        EXECUTION_LOG_ERROR(
            "Missing 'main' entry function with signature "
            "(*ExecutionContext)->int64");
        return;
      }
      EXECUTION_LOG_INFO("ADAPTIVE main() returned: {}", main(&exec_ctx));
    } else {
      std::function<int64_t()> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Adaptive, &main)) {
        EXECUTION_LOG_ERROR("Missing 'main' entry function with signature ()->int64");
        return;
      }
      EXECUTION_LOG_INFO("ADAPTIVE main() returned: {}", main());
    }
  }

  //
  // JIT
  //
  {
    exec_ctx.SetExecutionMode(static_cast<uint8_t>(vm::ExecutionMode::Compiled));
    util::ScopedTimer<std::milli> timer(&jit_exec_ms);

    if (is_sql) {
      std::function<int64_t(exec::ExecutionContext *)> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Compiled, &main)) {
        EXECUTION_LOG_ERROR(
            "Missing 'main' entry function with signature "
            "(*ExecutionContext)->int64");
        return;
      }
      EXECUTION_LOG_INFO("JIT main() returned: {}", main(&exec_ctx));
    } else {
      std::function<int64_t()> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Compiled, &main)) {
        EXECUTION_LOG_ERROR("Missing 'main' entry function with signature ()->int64");
        return;
      }
      EXECUTION_LOG_INFO("JIT main() returned: {}", main());
    }
  }

  // Dump stats
  EXECUTION_LOG_INFO(
      "Parse: {} ms, Type-check: {} ms, Code-gen: {} ms, Interp. Exec.: {} ms, "
      "Adaptive Exec.: {} ms, Jit+Exec.: {} ms",
      parse_ms, typecheck_ms, codegen_ms, interp_exec_ms, adaptive_exec_ms, jit_exec_ms);
  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // lma: When do we need this? I actually don't know...
  if (is_mini_runner) db_main->GetMetricsManager()->UnregisterThread();
}

/**
 * Run the TPL REPL
 */
static void RunRepl() {
  while (true) {
    std::string input;

    std::string line;
    do {
      std::getline(std::cin, line);

      if (line == K_EXIT_KEYWORD) {
        return;
      }

      input.append(line).append("\n");
    } while (!line.empty());

    CompileAndRun(input);
  }
}

/**
 * Compile and run the TPL program in the given filename
 * @param filename The name of the file on disk to compile
 */
static void RunFile(const std::string &filename) {
  auto file = llvm::MemoryBuffer::getFile(filename);
  if (std::error_code error = file.getError()) {
    EXECUTION_LOG_ERROR("There was an error reading file '{}': {}", filename, error.message());
    return;
  }

  EXECUTION_LOG_INFO("Compiling and running file: {}", filename);

  // Copy the source into a temporary, compile, and run
  CompileAndRun((*file)->getBuffer().str());
}

/**
 * Initialize all TPL subsystems
 */
void InitTPL() {
  execution::CpuInfo::Instance();

  execution::vm::LLVMEngine::Initialize();

  EXECUTION_LOG_INFO("TPL Bytecode Count: {}", execution::vm::Bytecodes::NumBytecodes());

  EXECUTION_LOG_INFO("TPL initialized ...");
}

/**
 * Shutdown all TPL subsystems
 */
void ShutdownTPL() {
  terrier::execution::vm::LLVMEngine::Shutdown();

  scheduler.terminate();
}

}  // namespace terrier::execution

void SignalHandler(int32_t sig_num) {
  if (sig_num == SIGINT) {
    terrier::execution::ShutdownTPL();
    exit(0);
  }
}

int main(int argc, char **argv) {  // NOLINT (bugprone-exception-escape)
  terrier::LoggersUtil::Initialize();

  // Parse options
  llvm::cl::HideUnrelatedOptions(tpl_options_category);
  llvm::cl::ParseCommandLineOptions(argc, argv);

  // Initialize a signal handler to call SignalHandler()
  struct sigaction sa;  // NOLINT
  sa.sa_handler = &SignalHandler;
  sa.sa_flags = SA_RESTART;

  sigfillset(&sa.sa_mask);

  if (sigaction(SIGINT, &sa, nullptr) == -1) {
    EXECUTION_LOG_ERROR("Cannot handle SIGNIT: {}", strerror(errno));
    return errno;
  }

  // Init TPL
  terrier::execution::InitTPL();

  EXECUTION_LOG_INFO("\n{}", terrier::execution::CpuInfo::Instance()->PrettyPrintInfo());

  EXECUTION_LOG_INFO("Welcome to TPL (ver. {}.{})", TPL_VERSION_MAJOR, TPL_VERSION_MINOR);

  // Either execute a TPL program from a source file, or run REPL
  if (!input_file.empty()) {
    terrier::execution::RunFile(input_file);
  } else if (argc == 1) {
    terrier::execution::RunRepl();
  }

  // Cleanup
  terrier::execution::ShutdownTPL();
  terrier::LoggersUtil::ShutDown();

  return 0;
}
