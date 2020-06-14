#include "execution/codegen/operators/csv_scan_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "execution/codegen/codegen.h"
#include "execution/codegen/compilation_context.h"
#include "execution/codegen/function_builder.h"
#include "execution/codegen/loop.h"
#include "execution/codegen/pipeline.h"
#include "execution/codegen/work_context.h"
#include "planner/plannodes/csv_scan_plan_node.h"

namespace terrier::execution::codegen {

namespace {
constexpr const char kFieldPrefix[] = "field";
}  // namespace

CSVScanTranslator::CSVScanTranslator(const planner::CSVScanPlanNode &plan, CompilationContext *compilation_context,
                                     Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline),
      base_row_type_(GetCodeGen()->MakeFreshIdentifier("CSVRow")) {
  // CSV scans are serial, for now.
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);
  // Declare state.
  base_row_ = compilation_context->GetQueryState()->DeclareStateEntry(GetCodeGen(), "csvRow",
                                                                      GetCodeGen()->MakeExpr(base_row_type_));
}

void CSVScanTranslator::DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {
  CodeGen *codegen = GetCodeGen();

  // Reserve now to reduce allocations.
  const auto output_schema = GetPlan().GetOutputSchema();
  auto fields = codegen->MakeEmptyFieldList();
  fields.reserve(output_schema->NumColumns());

  // Add columns to output.
  for (uint32_t idx = 0; idx < output_schema->NumColumns(); idx++) {
    auto field_name = codegen->MakeIdentifier(kFieldPrefix + std::to_string(idx));
    fields.emplace_back(codegen->MakeField(field_name, codegen->TplType(TypeId::Varchar)));
  }

  decls->push_back(codegen->DeclareStruct(base_row_type_, std::move(fields)));
}

ast::Expr *CSVScanTranslator::GetField(uint32_t field_index) const {
  CodeGen *codegen = GetCodeGen();
  ast::Identifier field_name = codegen->MakeIdentifier(kFieldPrefix + std::to_string(field_index));
  return codegen->AccessStructMember(base_row_.Get(codegen), field_name);
}

ast::Expr *CSVScanTranslator::GetFieldPtr(uint32_t field_index) const {
  return GetCodeGen()->AddressOf(GetField(field_index));
}

void CSVScanTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  auto codegen = GetCodeGen();
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
}

ast::Expr *CSVScanTranslator::GetTableColumn(uint16_t col_oid) const {
  const auto output_schema = GetPlan().GetOutputSchema();
  if (col_oid > output_schema->NumColumns()) {
    throw Exception(ExceptionType::CodeGen, fmt::format("out-of-bounds CSV column access @ idx={}", col_oid));
  }

  // Return the field converted to the appropriate type.
  auto codegen = GetCodeGen();
  auto field = GetField(col_oid);
  auto output_type = GetPlan().GetOutputSchema()->GetColumn(col_oid).GetType();
  switch (output_type) {
    case TypeId::Boolean:
      return codegen->CallBuiltin(ast::Builtin::ConvertStringToBool, {field});
    case TypeId::TinyInt:
    case TypeId::SmallInt:
    case TypeId::Integer:
    case TypeId::BigInt:
      return codegen->CallBuiltin(ast::Builtin::ConvertStringToInt, {field});
    case TypeId::Float:
    case TypeId::Double:
      return codegen->CallBuiltin(ast::Builtin::ConvertStringToReal, {field});
    case TypeId::Date:
      return codegen->CallBuiltin(ast::Builtin::ConvertStringToDate, {field});
    case TypeId::Timestamp:
      return codegen->CallBuiltin(ast::Builtin::ConvertStringToTime, {field});
    case TypeId::Varchar:
      return field;
    default:
      throw NotImplementedException(fmt::format("converting from string to {}", TypeIdToString(output_type)));
  }
}

}  // namespace terrier::execution::codegen
