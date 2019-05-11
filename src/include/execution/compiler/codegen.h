#pragma once

#include "execution/ast/ast.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/util/common.h"
#include "execution/util/macros.h"

namespace tpl::util {
class Region;
}

namespace tpl::compiler {

class CodeContext;
class FunctionBuilder;

class CodeGen {
 private:
  friend class FunctionBuilder;
  friend class QueryState;

  util::Region *GetRegion();
 public:

  CodeContext *GetCodeContext() { return ctx_; }
  explicit CodeGen(CodeContext *ctx);

  DISALLOW_COPY_AND_MOVE(CodeGen);

  ast::AstNodeFactory *operator->() { return factory_; }

  FunctionBuilder *GetCurrentFunction();

  ast::Expr *Ty_Nil() const;
  ast::Expr *Ty_Bool() const;
  ast::Expr *Ty_Int8() const;
  ast::Expr *Ty_Int16() const;
  ast::Expr *Ty_Int32() const;
  ast::Expr *Ty_Int64() const;
  ast::Expr *Ty_Int128() const;
  ast::Expr *Ty_UInt8() const;
  ast::Expr *Ty_UInt16() const;
  ast::Expr *Ty_UInt32() const;
  ast::Expr *Ty_UInt64() const;
  ast::Expr *Ty_UInt128() const;
  ast::Expr *Ty_Float32() const;
  ast::Expr *Ty_Float64() const;

  ast::BlockStmt *EmptyBlock() const;

  ast::Identifier NewIdentifier();

  ast::Stmt *Call(ast::FunctionDecl *fn, util::RegionVector<ast::Expr*> &&args);

 private:
  u64 id_count_;
  CodeContext *ctx_;
  ast::AstNodeFactory *factory_;
};

}