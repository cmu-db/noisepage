#include "execution/ast/type.h"

#include <string>

#include "llvm/ADT/SmallString.h"
#include "llvm/Support/raw_ostream.h"

#include "execution/ast/type_visitor.h"

namespace tpl::ast {

namespace {

/**
 * Visitor class that walks a type hierarchy tree with the purpose of
 * pretty-printing to an injected output stream.
 */
class TypePrinter : public TypeVisitor<TypePrinter> {
 public:
  explicit TypePrinter(llvm::raw_ostream &out) : out_(out) {}

#define DECLARE_VISIT_TYPE(Type) void Visit##Type(const Type *type);
  TYPE_LIST(DECLARE_VISIT_TYPE)
#undef DECLARE_VISIT_TYPE

  void Print(const Type *type) { Visit(type); }

 private:
  llvm::raw_ostream &os() { return out_; }

 private:
  llvm::raw_ostream &out_;
};

void tpl::ast::TypePrinter::VisitBuiltinType(const BuiltinType *type) { os() << type->tpl_name(); }

void TypePrinter::VisitFunctionType(const FunctionType *type) {
  os() << "(";
  bool first = true;
  for (const auto &param : type->params()) {
    if (!first) {
      os() << ",";
    }
    first = false;
    Visit(param.type);
  }
  os() << ")->";
  Visit(type->return_type());
}

void TypePrinter::VisitStringType(const StringType *type) { os() << "string"; }

void TypePrinter::VisitPointerType(const PointerType *type) {
  os() << "*";
  Visit(type->base());
}

void TypePrinter::VisitStructType(const StructType *type) {
  os() << "struct{";
  bool first = true;
  for (const auto &field : type->fields()) {
    if (!first) {
      os() << ",";
    }
    first = false;
    Visit(field.type);
  }
  os() << "}";
}

void TypePrinter::VisitArrayType(const ArrayType *type) {
  os() << "[";
  if (type->HasUnknownLength()) {
    os() << "*";
  } else {
    os() << type->length();
  }
  os() << "]";
  Visit(type->element_type());
}

void tpl::ast::TypePrinter::VisitMapType(const MapType *type) {
  os() << "map[";
  Visit(type->key_type());
  os() << "]";
  Visit(type->value_type());
}

}  // namespace

// static
std::string Type::ToString(const Type *type) {
  llvm::SmallString<256> buffer;
  llvm::raw_svector_ostream stream(buffer);

  TypePrinter printer(stream);
  printer.Print(type);

  return buffer.str();
}

}  // namespace tpl::ast
