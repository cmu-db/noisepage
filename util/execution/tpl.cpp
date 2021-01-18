#include "execution/tpl.h"

#include <llvm/Support/CommandLine.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/Path.h>
#include <tbb/task_scheduler_init.h>
#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "execution/ast/ast_dump.h"
#include "execution/ast/ast_pretty_print.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/parsing/parser.h"
#include "execution/parsing/scanner.h"
#include "execution/sema/error_reporter.h"
#include "execution/sema/sema.h"
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
#include "loggers/execution_logger.h"
#include "main/db_main.h"
#include "metrics/metrics_thread.h"
#include "parser/expression/constant_value_expression.h"
#include "runner/mini_runners_data_config.h"
#include "settings/settings_manager.h"
#include "storage/garbage_collector.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/timestamp_manager.h"

// ---------------------------------------------------------
// CLI options
// ---------------------------------------------------------

// clang-format off
llvm::cl::OptionCategory TPL_OPTIONS_CATEGORY("TPL Compiler Options", "Options for controlling the TPL compilation process.");  // NOLINT
llvm::cl::opt<bool> PRINT_AST("print-ast", llvm::cl::desc("Print the programs AST"), llvm::cl::cat(TPL_OPTIONS_CATEGORY));  // NOLINT
llvm::cl::opt<bool> PRINT_TBC("print-tbc", llvm::cl::desc("Print the generated TPL Bytecode"), llvm::cl::cat(TPL_OPTIONS_CATEGORY));  // NOLINT
llvm::cl::opt<bool> PRETTY_PRINT("pretty-print", llvm::cl::desc("Pretty-print the source from the parsed AST"), llvm::cl::cat(TPL_OPTIONS_CATEGORY));  // NOLINT
llvm::cl::opt<bool> IS_SQL("sql", llvm::cl::desc("Is the input a SQL query?"), llvm::cl::cat(TPL_OPTIONS_CATEGORY));  // NOLINT
llvm::cl::opt<bool> TPCH("tpch", llvm::cl::desc("Should the TPCH database be loaded? Requires '-schema' and '-data' directories."), llvm::cl::cat(TPL_OPTIONS_CATEGORY));  // NOLINT
llvm::cl::opt<std::string> DATA_DIR("data", llvm::cl::desc("Where to find data files of tables to load"), llvm::cl::cat(TPL_OPTIONS_CATEGORY));  // NOLINT
llvm::cl::opt<std::string> INPUT_FILE(llvm::cl::Positional, llvm::cl::desc("<input file>"), llvm::cl::init(""), llvm::cl::cat(TPL_OPTIONS_CATEGORY));  // NOLINT
llvm::cl::opt<std::string> OUTPUT_NAME("output-name", llvm::cl::desc("Print the output name"), llvm::cl::init("schema10"), llvm::cl::cat(TPL_OPTIONS_CATEGORY));  // NOLINT
// clang-format on

tbb::task_scheduler_init scheduler;

namespace noisepage::execution {

static constexpr const char *K_EXIT_KEYWORD = ".exit";

/**
 * Compile TPL source code contained in @em source and execute it in all execution modes once.
 * @param source The TPL source.
 * @param name The name of the TPL file.
 */
static void CompileAndRun(const std::string &source, const std::string &name = "tmp-tpl") {
  // Initialize noisepage objects
  auto db_main_builder = noisepage::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).SetUseGCThread(true);
  auto db_main = db_main_builder.Build();

  // Get the correct output format for this test
  exec::SampleOutput sample_output;
  sample_output.InitTestOutput();
  const auto *output_schema = sample_output.GetSchema(OUTPUT_NAME.data());

  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

  auto *txn = txn_manager->BeginTransaction();

  auto db_oid = catalog->CreateDatabase(common::ManagedPointer(txn), "test_db", true);
  auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
  auto ns_oid = accessor->GetDefaultNamespace();

  // Make the execution context
  exec::ExecutionSettings exec_settings{};
  exec::OutputPrinter printer(output_schema);
  exec::OutputCallback callback = printer;
  exec::ExecutionContext exec_ctx{
      db_oid,        common::ManagedPointer(txn), callback, output_schema, common::ManagedPointer(accessor),
      exec_settings, db_main->GetMetricsManager()};
  // Add dummy parameters for tests
  std::vector<parser::ConstantValueExpression> params;
  params.emplace_back(type::TypeId::INTEGER, sql::Integer(37));
  params.emplace_back(type::TypeId::REAL, sql::Real(37.73));
  params.emplace_back(type::TypeId::DATE, sql::DateVal(sql::Date::FromYMD(1937, 3, 7)));
  auto string_val = sql::ValueUtil::CreateStringVal(std::string_view("37 Strings"));
  params.emplace_back(type::TypeId::VARCHAR, string_val.first, std::move(string_val.second));
  exec_ctx.SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&params));

  // Generate test tables
  sql::TableGenerator table_generator{&exec_ctx, db_main->GetStorageLayer()->GetBlockStore(), ns_oid};
  table_generator.GenerateTestTables();
  // Comment out to make more tables available at runtime
  // table_generator.GenerateTPCHTables(<path_to_tpch_dir>);
  // table_generator.GenerateTableFromFile(<path_to_schema>, <path_to_data>);

  // Let's parse the source
  util::Region err_region{"tmp-error-region"};
  util::Region context_region{"tmp-context-region"};
  sema::ErrorReporter error_reporter{&err_region};
  ast::Context context(&context_region, &error_reporter);

  parsing::Scanner scanner(source.data(), source.length());
  parsing::Parser parser(&scanner, &context);

  double parse_ms = 0.0,       // Time to parse the source
      typecheck_ms = 0.0,      // Time to perform semantic analysis
      codegen_ms = 0.0,        // Time to generate TBC
      interp_exec_ms = 0.0,    // Time to execute the program in fully interpreted mode
      adaptive_exec_ms = 0.0,  // Time to execute the program in adaptive mode
      jit_exec_ms = 0.0;       // Time to execute the program in JIT excluding compilation time

  //
  // Parse
  //

  ast::AstNode *root;
  {
    util::ScopedTimer<std::milli> timer(&parse_ms);
    root = parser.Parse();
  }

  if (error_reporter.HasErrors()) {
    EXECUTION_LOG_ERROR("Parsing error! Error: {}", error_reporter.SerializeErrors());
    return;
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
    EXECUTION_LOG_ERROR("Type-checking error! Error: {}", error_reporter.SerializeErrors());
    return;
  }

  // Dump AST
  if (PRINT_AST) {
    std::cout << ast::AstDump::Dump(root);  // NOLINT
  }

  // Pretty-print AST
  if (PRETTY_PRINT) {
    ast::AstPrettyPrint::Dump(std::cout, root);  // NOLINT
  }

  //
  // TBC generation
  //

  std::unique_ptr<vm::BytecodeModule> bytecode_module;
  {
    util::ScopedTimer<std::milli> timer(&codegen_ms);
    bytecode_module = vm::BytecodeGenerator::Compile(root, name);
  }

  // Dump Bytecode
  if (PRINT_TBC) {
    bytecode_module->Dump(std::cout);  // NOLINT
  }

  auto module = std::make_unique<vm::Module>(std::move(bytecode_module));

  //
  // Interpret
  //

  {
    exec_ctx.SetExecutionMode(static_cast<uint8_t>(vm::ExecutionMode::Interpret));
    util::ScopedTimer<std::milli> timer(&interp_exec_ms);

    if (IS_SQL) {
      std::function<int32_t(exec::ExecutionContext *)> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Interpret, &main)) {
        EXECUTION_LOG_ERROR("Missing 'main' entry function with signature (*ExecutionContext)->int32");
        return;
      }
      EXECUTION_LOG_INFO("VM main() returned: {}", main(&exec_ctx));
    } else {
      std::function<int32_t()> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Interpret, &main)) {
        EXECUTION_LOG_ERROR("Missing 'main' entry function with signature ()->int32");
        return;
      }
      EXECUTION_LOG_INFO("VM main() returned: {}", main());
    }
  }

  //
  // Adaptive
  //

  exec_ctx.SetExecutionMode(static_cast<uint8_t>(vm::ExecutionMode::Adaptive));
  util::ScopedTimer<std::milli> timer(&adaptive_exec_ms);

  if (IS_SQL) {
    std::function<int32_t(exec::ExecutionContext *)> main;
    if (!module->GetFunction("main", vm::ExecutionMode::Adaptive, &main)) {
      EXECUTION_LOG_ERROR("Missing 'main' entry function with signature (*ExecutionContext)->int32");
      return;
    }
    EXECUTION_LOG_INFO("ADAPTIVE main() returned: {}", main(&exec_ctx));
  } else {
    std::function<int32_t()> main;
    if (!module->GetFunction("main", vm::ExecutionMode::Adaptive, &main)) {
      EXECUTION_LOG_ERROR("Missing 'main' entry function with signature ()->int32");
      return;
    }
    EXECUTION_LOG_INFO("ADAPTIVE main() returned: {}", main());
  }

  //
  // JIT
  //
  {
    exec_ctx.SetExecutionMode(static_cast<uint8_t>(vm::ExecutionMode::Compiled));
    util::ScopedTimer<std::milli> timer(&jit_exec_ms);

    if (IS_SQL) {
      std::function<int32_t(exec::ExecutionContext *)> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Compiled, &main)) {
        EXECUTION_LOG_ERROR("Missing 'main' entry function with signature (*ExecutionContext)->int32");
        return;
      }
      util::Timer<std::milli> x;
      x.Start();
      EXECUTION_LOG_INFO("JIT main() returned: {}", main(&exec_ctx));
      x.Stop();
      EXECUTION_LOG_INFO("Jit exec: {} ms", x.GetElapsed());
    } else {
      std::function<int32_t()> main;
      if (!module->GetFunction("main", vm::ExecutionMode::Compiled, &main)) {
        EXECUTION_LOG_ERROR("Missing 'main' entry function with signature ()->int32");
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
}

/**
 * Compile and run the TPL program contained in the file with the given filename @em filename.
 * @param filename The name of TPL file to compile and run.
 */
static void RunFile(const std::string &filename) {
  auto file = llvm::MemoryBuffer::getFile(filename);
  if (std::error_code error = file.getError()) {
    EXECUTION_LOG_ERROR("There was an error reading file '{}': {}", filename, error.message());
    return;
  }

  EXECUTION_LOG_INFO("Compiling and running file: {}", filename);

  // Copy the source into a temporary, compile, and run
  CompileAndRun((*file)->getBuffer().str(), filename);
}

/**
 * Run the REPL.
 */
static void RunRepl() {
  const auto prompt_and_read_line = [] {
    std::string line;
    printf(">>> ");  // NOLINT
    std::getline(std::cin, line);
    return line;
  };

  while (true) {
    std::string line = prompt_and_read_line();

    // Exit?
    if (line == K_EXIT_KEYWORD) {
      return;
    }

    // Run file?
    if (llvm::StringRef line_ref(line); line_ref.startswith_lower(".run")) {
      auto pair = line_ref.split(' ');
      RunFile(pair.second);
      continue;
    }

    // Code ...
    std::string input;
    while (!line.empty()) {
      input.append(line).append("\n");
      line = prompt_and_read_line();
    }
    // Try to compile and run it
    CompileAndRun(input);
  }
}

/**
 * Initialize all TPL subsystems in preparation for execution.
 */
void InitTPL() {
  execution::CpuInfo::Instance();

  execution::vm::LLVMEngine::Initialize();

  EXECUTION_LOG_INFO("TPL Bytecode Count: {}", execution::vm::Bytecodes::NumBytecodes());

  EXECUTION_LOG_INFO("TPL initialized ...");
}

/**
 * Shutdown all TPL subsystems.
 */
void ShutdownTPL() {
  noisepage::execution::vm::LLVMEngine::Shutdown();

  scheduler.terminate();

  EXECUTION_LOG_INFO("TPL cleanly shutdown ...");
}

}  // namespace noisepage::execution

void SignalHandler(int32_t sig_num) {
  if (sig_num == SIGINT) {
    noisepage::execution::ShutdownTPL();
    exit(0);
  }
}

int main(int argc, char **argv) {
  noisepage::LoggersUtil::Initialize();

  // Parse options
  llvm::cl::HideUnrelatedOptions(TPL_OPTIONS_CATEGORY);
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
  noisepage::execution::InitTPL();

  EXECUTION_LOG_INFO("\n{}", noisepage::execution::CpuInfo::Instance()->PrettyPrintInfo());

  EXECUTION_LOG_INFO("Welcome to TPL (ver. {}.{})", TPL_VERSION_MAJOR, TPL_VERSION_MINOR);

  // Either execute a TPL program from a source file, or run REPL
  if (!INPUT_FILE.empty()) {
    noisepage::execution::RunFile(INPUT_FILE);
  } else {
    noisepage::execution::RunRepl();
  }

  // Cleanup
  noisepage::execution::ShutdownTPL();
  noisepage::LoggersUtil::ShutDown();

  return 0;
}
