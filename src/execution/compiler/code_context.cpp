#include "execution/compiler/code_context.h"

namespace tpl::compiler {

CodeContext::CodeContext(util::Region *region)
    : region_(region),
      reporter_(region),
      ast_ctx_(region, &reporter_),
      ast_factory_(region),
      decls_(region_),
      curr_fn_(nullptr),
      // lookup names must match ast/type.h's BUILTIN_TYPE_LIST
      nil_type_(ast_ctx_.LookupBuiltinType(ast::Identifier("nil"))),
      bool_type_(ast_ctx_.LookupBuiltinType(ast::Identifier("bool"))),
      i8_type_(ast_ctx_.LookupBuiltinType(ast::Identifier("int8"))),
      i16_type_(ast_ctx_.LookupBuiltinType(ast::Identifier("int16"))),
      i32_type_(ast_ctx_.LookupBuiltinType(ast::Identifier("int32"))),
      i64_type_(ast_ctx_.LookupBuiltinType(ast::Identifier("int64"))),
      i128_type_(ast_ctx_.LookupBuiltinType(ast::Identifier("int128"))),
      u8_type_(ast_ctx_.LookupBuiltinType(ast::Identifier("uint8"))),
      u16_type_(ast_ctx_.LookupBuiltinType(ast::Identifier("uint16"))),
      u32_type_(ast_ctx_.LookupBuiltinType(ast::Identifier("uint32"))),
      u64_type_(ast_ctx_.LookupBuiltinType(ast::Identifier("uint64"))),
      u128_type_(ast_ctx_.LookupBuiltinType(ast::Identifier("uint128"))),
      f32_type_(ast_ctx_.LookupBuiltinType(ast::Identifier("float32"))),
      f64_type_(ast_ctx_.LookupBuiltinType(ast::Identifier("float64"))) {}

}  // namespace tpl::compiler