#include "execution/compiler/operator/csv_scan_translator.h"

#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/loop.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/csv_scan_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::compiler {

namespace {
constexpr const char FIELD_PREFIX[] = "field";
}  // namespace

CSVScanTranslator::CSVScanTranslator(const planner::CSVScanPlanNode &plan, CompilationContext *compilation_context,
                                     Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, selfdriving::ExecutionOperatingUnitType::DUMMY),
      base_row_type_(GetCodeGen()->MakeFreshIdentifier("CSVRow")) {
  // CSV scans are serial, for now.
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);
  // Declare state.
  base_row_ = compilation_context->GetQueryState()->DeclareStateEntry(GetCodeGen(), "csvRow",
                                                                      GetCodeGen()->MakeExpr(base_row_type_));
}

void CSVScanTranslator::DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {
  auto *codegen = GetCodeGen();

  // Reserve now to reduce allocations.
  const auto output_schema = GetPlan().GetOutputSchema();
  auto fields = codegen->MakeEmptyFieldList();
  fields.reserve(output_schema->NumColumns());

  // Add columns to output.
  for (uint32_t idx = 0; idx < output_schema->NumColumns(); idx++) {
    auto field_name = codegen->MakeIdentifier(FIELD_PREFIX + std::to_string(idx));
    fields.emplace_back(codegen->MakeField(field_name, codegen->TplType(sql::TypeId::Varchar)));
  }

  decls->push_back(codegen->DeclareStruct(base_row_type_, std::move(fields)));
}

ast::Expr *CSVScanTranslator::GetField(uint32_t field_index) const {
  auto *codegen = GetCodeGen();
  ast::Identifier field_name = codegen->MakeIdentifier(FIELD_PREFIX + std::to_string(field_index));
  return codegen->AccessStructMember(base_row_.Get(codegen), field_name);
}

ast::Expr *CSVScanTranslator::GetFieldPtr(uint32_t field_index) const {
  return GetCodeGen()->AddressOf(GetField(field_index));
}

void CSVScanTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  UNREACHABLE("CSV support disabled because <charconv> missing on CI.");
#if 0
  auto *codegen = GetCodeGen();
  auto reader_var_base = codegen->MakeFreshIdentifier("csvReaderBase");
  auto reader_var = codegen->MakeFreshIdentifier("csvReader");
  function->Append(codegen->DeclareVarNoInit(reader_var_base, ast::BuiltinType::CSVReader));
  function->Append(codegen->DeclareVarWithInit(reader_var, codegen->AddressOf(reader_var_base)));
  function->Append(
      codegen->DeclareVarWithInit(codegen->MakeFreshIdentifier("isValid"),
                                  codegen->CSVReaderInit(codegen->MakeExpr(reader_var), GetCSVPlan().GetFileName())));
  Loop scan_loop(function, codegen->CSVReaderAdvance(codegen->MakeExpr(reader_var)));
  {
    // Read fields.
    const auto output_schema = GetPlan().GetOutputSchema();
    for (uint32_t i = 0; i < output_schema->NumColumns(); i++) {
      ast::Expr *field_ptr = GetFieldPtr(i);
      function->Append(codegen->CSVReaderGetField(codegen->MakeExpr(reader_var), i, field_ptr));
    }
    // Done.
    context->Push(function);
  }
  scan_loop.EndLoop();
  function->Append(codegen->CSVReaderClose(codegen->MakeExpr(reader_var)));
#endif
}

ast::Expr *CSVScanTranslator::GetTableColumn(catalog::col_oid_t col_oid) const {
  const auto output_schema = GetPlan().GetOutputSchema();
  if (col_oid.UnderlyingValue() > output_schema->NumColumns()) {
    throw EXECUTION_EXCEPTION(
        fmt::format("Codegen: out-of-bounds CSV column access @ idx={}", col_oid.UnderlyingValue()),
        common::ErrorCode::ERRCODE_DATA_EXCEPTION);
  }

  // Return the field converted to the appropriate type.
  auto *codegen = GetCodeGen();
  auto *field = GetField(col_oid.UnderlyingValue());
  auto output_type = sql::GetTypeId(GetPlan().GetOutputSchema()->GetColumn(col_oid.UnderlyingValue()).GetType());
  switch (output_type) {
    case sql::TypeId::Boolean:
      return codegen->CallBuiltin(ast::Builtin::ConvertStringToBool, {field});
    case sql::TypeId::TinyInt:
    case sql::TypeId::SmallInt:
    case sql::TypeId::Integer:
    case sql::TypeId::BigInt:
      return codegen->CallBuiltin(ast::Builtin::ConvertStringToInt, {field});
    case sql::TypeId::Float:
    case sql::TypeId::Double:
      return codegen->CallBuiltin(ast::Builtin::ConvertStringToReal, {field});
    case sql::TypeId::Date:
      return codegen->CallBuiltin(ast::Builtin::ConvertStringToDate, {field});
    case sql::TypeId::Timestamp:
      return codegen->CallBuiltin(ast::Builtin::ConvertStringToTime, {field});
    case sql::TypeId::Varchar:
      return field;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("Converting from string to {}", TypeIdToString(output_type)));
  }
}

}  // namespace noisepage::execution::compiler
