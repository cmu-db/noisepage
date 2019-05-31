#include "common/managed_pointer.h"
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include "gtest/gtest.h"

namespace terrier {

// NOLINTNEXTLINE
TEST(ManagedPointerTests, EqualityTest) {
  std::string val0 = "abcde";
  char *raw_ptr0 = val0.data();
  std::string val1 = "12345";
  char *raw_ptr1 = val1.data();

  common::ManagedPointer<char *> ptr0(&raw_ptr0);
  common::ManagedPointer<char *> ptr1(&raw_ptr1);
  common::ManagedPointer<char *> ptr2(&raw_ptr0);

  EXPECT_NE(ptr0, ptr1);
  EXPECT_EQ(ptr0, ptr2);
}

// NOLINTNEXTLINE
TEST(ManagedPointerTests, PointerAccessTest) {
  std::string val0 = "Peloton Is Dead";
  char *raw_ptr0 = val0.data();
  std::string val1 = "WuTang";
  char *raw_ptr1 = val1.data();

  common::ManagedPointer<char *> ptr0(&raw_ptr0);
  common::ManagedPointer<char *> ptr1(&raw_ptr1);

  EXPECT_EQ(*ptr0, raw_ptr0);
  EXPECT_NE(*ptr0, raw_ptr1);
}

// NOLINTNEXTLINE
TEST(ManagedPointerTests, OutputHashTest) {
  // Make sure that ManagedPointer has the same output and hashing
  // behavior as std::shared_ptr
  std::string orig = "ODBRIP";
  char *raw_ptr = orig.data();

  std::shared_ptr<char *> ptr0(&raw_ptr, [=](char **ptr) {
    // Do nothing in this custom delete function so that
    // the shared_ptr doesn't try to deallocate the string's memory
  });
  std::ostringstream os0;
  os0 << ptr0;
  std::hash<std::shared_ptr<char *>> hash_func0;
  size_t hash0 = hash_func0(ptr0);

  common::ManagedPointer<char *> ptr1(&raw_ptr);
  std::ostringstream os1;
  os1 << ptr1;
  std::hash<common::ManagedPointer<char *>> hash_func1;
  size_t hash1 = hash_func1(ptr1);

  EXPECT_EQ(os0.str(), os1.str());
  EXPECT_EQ(hash0, hash1);
}

}  // namespace terrier
