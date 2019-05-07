#include "execution/compiler/code_context.h"

#include "execution/compiler/compiler_defs.h"

namespace tpl::compiler {

CodeContext::CodeContext(util::Region *region)
: region_(region), error_reporter_(region_), ast_ctx_(region_, &error_reporter_), ast_factory_(region_), curr_fn_(nullptr),
  codeGen_(this),
    // lookup names must match ast/type.h's BUILTIN_TYPE_LIST
  nil_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast::Identifier("nil"))),
  bool_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast::Identifier("bool"))),
  i8_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast::Identifier("int8"))),
  i16_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast::Identifier("int16"))),
  i32_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast::Identifier("int32"))),
  i64_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast::Identifier("int64"))),
  i128_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast::Identifier("int128"))),
  u8_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast::Identifier("uint8"))),
  u16_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast::Identifier("uint16"))),
  u32_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast::Identifier("uint32"))),
  u64_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast::Identifier("uint64"))),
  u128_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast::Identifier("uint128"))),
  f32_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast::Identifier("float32"))),
  f64_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast::Identifier("float64")))
{

}

}