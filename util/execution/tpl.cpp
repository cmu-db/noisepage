#include <gflags/gflags.h>
#include <unistd.h>
#include <algorithm>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include "tbb/task_scheduler_init.h"

#include "execution/ast/ast_dump.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/output.h"
#include "execution/parsing/parser.h"
#include "execution/parsing/scanner.h"
#include "execution/sema/error_reporter.h"
#include "execution/sema/sema.h"
#include "execution/sql/memory_pool.h"
#include "execution/table_generator/sample_output.h"
#include "execution/table_generator/table_generator.h"
#include "execution/tpl.h"
#include "execution/util/cpu_info.h"
#include "execution/util/timer.h"
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/bytecode_module.h"
#include "execution/vm/llvm_engine.h"
#include "execution/vm/module.h"
#include "execution/vm/vm.h"
#include "llvm/Support/CommandLine.h"
#include "llvm/Support/MemoryBuffer.h"
#include "loggers/loggers_util.h"
#include "settings/settings_manager.h"
#include "storage/garbage_collector.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/timestamp_manager.h"

#define __SETTING_GFLAGS_DEFINE__      // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DEFINE__       // NOLINT

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
  storage::BlockStore block_store(1000, 1000);
  storage::RecordBufferSegmentPool buffer_pool(100000, 100000);
  transaction::TimestampManager tm_manager{};
  transaction::DeferredActionManager da_manager{&tm_manager};
  transaction::TransactionManager txn_manager(&tm_manager, &da_manager, &buffer_pool, true, nullptr);
  storage::GarbageCollector gc(&tm_manager, &da_manager, &txn_manager, nullptr);
  auto *txn = txn_manager.BeginTransaction();

  // Get the correct output format for this test
  exec::SampleOutput sample_output;
  sample_output.InitTestOutput();
  auto output_schema = sample_output.GetSchema(output_name.data());

  // Make the catalog accessor
  catalog::Catalog catalog(&txn_manager, &block_store);

  auto db_oid = catalog.CreateDatabase(txn, "test_db", true);
  auto accessor = std::unique_ptr<catalog::CatalogAccessor>(catalog.GetAccessor(txn, db_oid));
  auto ns_oid = accessor->GetDefaultNamespace();

  // Make the execution context
  exec::OutputPrinter printer(output_schema);
  exec::ExecutionContext exec_ctx{db_oid, txn, printer, output_schema, std::move(accessor)};

  // Generate test tables
  // TODO(Amadou): Read this in from a directory. That would require boost or experimental C++ though
  sql::TableGenerator table_generator{&exec_ctx, &block_store, ns_oid};
  table_generator.GenerateTestTables();
  // Comment out to make more tables available at runtime
  table_generator.GenerateTPCHTables("../../tpl_tables/tables/");
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

  {
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
  txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  catalog.TearDown();
  gc.PerformGarbageCollection();
  gc.PerformGarbageCollection();
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

  terrier::LoggersUtil::Initialize(false);

  execution::vm::LLVMEngine::Initialize();

  EXECUTION_LOG_INFO("TPL Bytecode Count: {}", execution::vm::Bytecodes::NumBytecodes());

  EXECUTION_LOG_INFO("TPL initialized ...");
}

/**
 * Shutdown all TPL subsystems
 */
void ShutdownTPL() {
  terrier::execution::vm::LLVMEngine::Shutdown();
  terrier::LoggersUtil::ShutDown();

  scheduler.terminate();

  LOG_INFO("TPL cleanly shutdown ...");
}

}  // namespace terrier::execution

void SignalHandler(int32_t sig_num) {
  if (sig_num == SIGINT) {
    terrier::execution::ShutdownTPL();
    exit(0);
  }
}

int main(int argc, char **argv) {  // NOLINT (bugprone-exception-escape)
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

  return 0;
}
