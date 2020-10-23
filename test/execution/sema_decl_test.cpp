#include <vector>

#include "execution/ast/ast_builder.h"
#include "execution/sema/sema.h"
#include "execution/tpl_test.h"

namespace terrier::execution::sema::test {

class SemaDeclTest : public TplTest, public ast::test::TestAstBuilder {
 public:
  void ResetErrorReporter() { ErrorReporter()->Reset(); }
};

struct SemaDeclTestCase {
  bool has_errors_;
  std::string msg_;
  ast::AstNode *tree_;
};

// NOLINTNEXTLINE
TEST_F(SemaDeclTest, DuplicateStructFields) {
  SemaDeclTestCase tests[] = {
      // Test case 1 should fail.
      {true, "Struct has duplicate 'a' fields",
       GenFile({
           // The single struct declaration in the file.
           DeclStruct(Ident("s"),
                      {
                          GenFieldDecl(Ident("a"), PrimIntTypeRepr()),  // a: int
                          GenFieldDecl(Ident("b"), PrimIntTypeRepr()),  // b: int
                          GenFieldDecl(Ident("a"), PrimIntTypeRepr()),  // a: int
                      }),
       })},

      // Test case 2 should fail.
      {true, "Struct has duplicate 'a' with different types",
       GenFile({
           // The single struct declaration in the file.
           DeclStruct(Ident("s"),
                      {
                          GenFieldDecl(Ident("a"), PrimIntTypeRepr()),            // a: int
                          GenFieldDecl(Ident("a"), PtrType(PrimBoolTypeRepr())),  // a: *bool
                      }),
       })},

      // Test case 3 should be fine.
      {false, "Struct has only unique fields and should pass type-checking",
       GenFile({
           // The single struct declaration in the file.
           DeclStruct(Ident("s"),
                      {
                          GenFieldDecl(Ident("c"), PrimIntTypeRepr()),  // c: int
                          GenFieldDecl(Ident("d"), PrimIntTypeRepr()),  // d: int
                      }),
       })},
  };

  for (const auto &test : tests) {
    Sema sema(Ctx());
    bool has_errors = sema.Run(test.tree_);
    EXPECT_EQ(test.has_errors_, has_errors) << test.msg_;
    ResetErrorReporter();
  }
}

}  // namespace terrier::execution::sema::test
