#include "execution/compiler/code_context.h"

#include "execution/compiler/compiler_defs.h"

namespace tpl::compiler {

CodeContext::CodeContext(util::Region *region)
: region_(region), error_reporter_(region_), ast_ctx_(region_, &error_reporter_), ast_factory_(region_), curr_fn_(nullptr),
    // lookup names must match ast/type.h's BUILTIN_TYPE_LIST
  nil_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast_ctx_.GetIdentifier("nil"))),
  bool_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast_ctx_.GetIdentifier("bool"))),
  i8_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast_ctx_.GetIdentifier("int8"))),
  i16_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast_ctx_.GetIdentifier("int16"))),
  i32_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast_ctx_.GetIdentifier("int32"))),
  i64_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast_ctx_.GetIdentifier("int64"))),
  i128_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast_ctx_.GetIdentifier("int128"))),
  u8_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast_ctx_.GetIdentifier("uint8"))),
  u16_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast_ctx_.GetIdentifier("uint16"))),
  u32_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast_ctx_.GetIdentifier("uint32"))),
  u64_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast_ctx_.GetIdentifier("uint64"))),
  u128_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast_ctx_.GetIdentifier("uint128"))),
  f32_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast_ctx_.GetIdentifier("float32"))),
  f64_type_(ast_factory_.NewIdentifierExpr(DUMMY_POS, ast_ctx_.GetIdentifier("float64")))
{

}

}