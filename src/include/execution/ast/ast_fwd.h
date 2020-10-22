#pragma once

// Forward-declare a few of the common AST nodes here to reduce coupling between
// the SQL code generation components and the TPL AST systems.

namespace terrier::execution::ast {

class BlockStmt;
class Context;
class Expr;  // NOLINT it picks up the parser's global Expr
class Decl;
class FieldDecl;
class File;  // NOLINT it picks up madoka's File
class FunctionDecl;
class Stmt;
class StructDecl;
class VariableDecl;

}  // namespace terrier::execution::ast
