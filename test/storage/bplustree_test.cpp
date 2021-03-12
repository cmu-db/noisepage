#include <cstdlib>
#include <set>
#include <unordered_map>

#include "storage/index/bplustree.h"
#include "storage/storage_defs.h"
#include "test_util/multithread_test_util.h"
#include "test_util/test_harness.h"

namespace noisepage::storage::index {

unsigned int globalseed = 9;

class BPlusTreeTests : public TerrierTest {
 public:
  const uint32_t num_threads_ = 4;
  common::WorkerPool thread_pool_{num_threads_, {}};

 protected:
  void SetUp() override { thread_pool_.Startup(); }

  void TearDown() override { thread_pool_.Shutdown(); }
};

/**
 * This test verifies basic insertion in a node without split. Also makes sure that freeing
 * the node works without leaks.
 */
void BasicNodeInitPushBackTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  int size = 10;
  int depth = 0;
  int item_count = 10;  // Usually equal to size
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      size, BPlusTree<int, TupleSlot>::NodeType::LeafType, depth, item_count, p1, p2);

  // To check if we can read what we inserted
  std::vector<BPlusTree<int, TupleSlot>::KeyNodePointerPair> values;
  for (int key = 0; key < item_count; key++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = key;
    values.push_back(p1);
    node->PushBack(p1);
    EXPECT_EQ(node->GetSize(), key + 1);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;

  unsigned key = 0;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, key);
    key++;
  }

  // To Check if we are inserting at the correct place
  EXPECT_EQ(reinterpret_cast<char *>(node) + sizeof(BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>>),
            reinterpret_cast<char *>(node->Begin()));

  EXPECT_EQ(&(node->GetLowKeyPair()), node->GetElasticLowKeyPair());
  EXPECT_EQ(&(node->GetHighKeyPair()), node->GetElasticHighKeyPair());
  EXPECT_EQ(node->GetLowKeyPair().first, p1.first);
  EXPECT_EQ(node->GetHighKeyPair().first, p2.first);
  EXPECT_NE(&p1, &(node->GetLowKeyPair()));
  EXPECT_NE(&p2, &(node->GetHighKeyPair()));

  // Free the node - should not result in an ASAN
  node->FreeElasticNode();
  delete bplustree;
}

/**
 * Works similar to the push back test above, but uses the Insert function instead of
 * directly using PushBack on the node.
 */
void InsertElementInNodeTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  int size = 10;
  int depth = 0;
  int item_count = 10;  // Usually equal to size
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      size, BPlusTree<int, TupleSlot>::NodeType::LeafType, depth, item_count, p1, p2);

  for (int key = 0; key < item_count; key++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = key;
    EXPECT_EQ(node->InsertElementIfPossible(p1, node->Begin()), true);
    EXPECT_EQ(node->GetSize(), key + 1);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;

  unsigned key = 9;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, key);
    key--;
  }

  // To Check if we are inserting at the correct place
  EXPECT_EQ(reinterpret_cast<char *>(node) + sizeof(BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>>),
            reinterpret_cast<char *>(node->Begin()));

  EXPECT_EQ(&(node->GetLowKeyPair()), node->GetElasticLowKeyPair());
  EXPECT_EQ(&(node->GetHighKeyPair()), node->GetElasticHighKeyPair());
  EXPECT_EQ(node->GetLowKeyPair().first, p1.first);
  EXPECT_EQ(node->GetHighKeyPair().first, p2.first);
  EXPECT_NE(&p1, &(node->GetLowKeyPair()));
  EXPECT_NE(&p2, &(node->GetHighKeyPair()));

  // Free the node - should not result in an ASAN
  node->FreeElasticNode();
  delete bplustree;
}

/**
 * Randomly insert elements in a node.
 */
void InsertElementInNodeRandomTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  int size = 10;
  int depth = 0;
  int item_count = 10;  // Usually equal to size
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      size, BPlusTree<int, TupleSlot>::NodeType::LeafType, depth, item_count, p1, p2);

  std::map<int, int> positions;
  for (int key = 0; key < item_count; key++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = key;
    int k;
    k = rand_r(&globalseed) % (node->GetSize() + 1);
    while (positions.find(k) != positions.end()) k = (k + 1) % (node->GetSize() + 1);
    EXPECT_EQ(node->InsertElementIfPossible(p1, node->Begin() + k), true);
    positions[k] = key;
    EXPECT_EQ(node->GetSize(), key + 1);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;

  unsigned key = 0;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, positions[key]);
    key++;
  }

  // To Check if we are inserting at the correct place
  EXPECT_EQ(reinterpret_cast<char *>(node) + sizeof(BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>>),
            reinterpret_cast<char *>(node->Begin()));

  EXPECT_EQ(&(node->GetLowKeyPair()), node->GetElasticLowKeyPair());
  EXPECT_EQ(&(node->GetHighKeyPair()), node->GetElasticHighKeyPair());
  EXPECT_EQ(node->GetLowKeyPair().first, p1.first);
  EXPECT_EQ(node->GetHighKeyPair().first, p2.first);
  EXPECT_NE(&p1, &(node->GetLowKeyPair()));
  EXPECT_NE(&p2, &(node->GetHighKeyPair()));

  // Free the node - should not result in an ASAN
  node->FreeElasticNode();
  delete bplustree;
}

/**
 * Explicitly verifies splitting of a node
 */
void SplitNodeTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  int size = 10;
  int depth = 0;
  int item_count = 10;  // Usually equal to size
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      size, BPlusTree<int, TupleSlot>::NodeType::LeafType, depth, item_count, p1, p2);

  for (int key = 0; key < item_count; key++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = key;
    EXPECT_EQ(node->InsertElementIfPossible(p1, node->End()), true);
    EXPECT_EQ(node->GetSize(), key + 1);
  }

  auto newnode = node->SplitNode();

  using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;

  unsigned key = 0;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, key);
    key++;
  }

  EXPECT_EQ(key, 5);

  for (ElementType *element_p = newnode->Begin(); element_p != newnode->End(); element_p++) {
    EXPECT_EQ(element_p->first, key);
    key++;
  }

  EXPECT_EQ(key, 10);

  EXPECT_EQ(&(node->GetLowKeyPair()), node->GetElasticLowKeyPair());
  EXPECT_EQ(&(node->GetHighKeyPair()), node->GetElasticHighKeyPair());
  EXPECT_EQ(node->GetLowKeyPair().first, p1.first);
  EXPECT_EQ(node->GetHighKeyPair().first, p2.first);
  EXPECT_NE(&p1, &(node->GetLowKeyPair()));
  EXPECT_NE(&p2, &(node->GetHighKeyPair()));

  EXPECT_EQ(&(newnode->GetLowKeyPair()), newnode->GetElasticLowKeyPair());
  EXPECT_EQ(&(newnode->GetHighKeyPair()), newnode->GetElasticHighKeyPair());
  EXPECT_EQ(newnode->GetLowKeyPair().first, p1.first);
  EXPECT_EQ(newnode->GetHighKeyPair().first, p2.first);
  EXPECT_NE(&p1, &(newnode->GetLowKeyPair()));
  EXPECT_NE(&p2, &(newnode->GetHighKeyPair()));

  // Free the node - should not result in an ASAN
  node->FreeElasticNode();
  newnode->FreeElasticNode();
  delete bplustree;
}

/**
 * Tests whether the FindLocation function correctly fetches the location of a key in the node.
 */
void FindLocationTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  auto size = 10;
  auto depth = 0;
  auto item_count = 10;  // Usually equal to size
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      size, BPlusTree<int, TupleSlot>::NodeType::LeafType, depth, item_count, p1, p2);

  std::set<unsigned> s;
  while (node->GetSize() < node->GetItemCount()) {
    int k = rand_r(&globalseed);
    while (s.find(k) != s.end()) k++;
    s.insert(k);
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p;
    p.first = k;
    EXPECT_EQ(node->InsertElementIfPossible(
                  p, static_cast<BPlusTree<int, TupleSlot>::InnerNode *>(node)->FindLocation(k, bplustree)),
              true);
  }
  auto iter = node->Begin();
  for (auto &elem : s) {
    EXPECT_EQ(iter->first, elem);
    iter++;
  }

  // To Check if we are inserting at the correct place
  EXPECT_EQ(reinterpret_cast<char *>(node) + sizeof(BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>>),
            reinterpret_cast<char *>(node->Begin()));

  EXPECT_EQ(&(node->GetLowKeyPair()), node->GetElasticLowKeyPair());
  EXPECT_EQ(&(node->GetHighKeyPair()), node->GetElasticHighKeyPair());
  EXPECT_EQ(node->GetLowKeyPair().first, p1.first);
  EXPECT_EQ(node->GetHighKeyPair().first, p2.first);
  EXPECT_NE(&p1, &(node->GetLowKeyPair()));
  EXPECT_NE(&p2, &(node->GetHighKeyPair()));

  // Free the node - should not result in an ASAN
  node->FreeElasticNode();
  delete bplustree;
}

/**
 * Tests the PopBegin() function in a node.
 */
void PopBeginTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  int size = 10;
  int depth = 0;
  int item_count = 10;  // Usually equal to size
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      size, BPlusTree<int, TupleSlot>::NodeType::LeafType, depth, item_count, p1, p2);

  // To check if we can read what we inserted
  std::vector<BPlusTree<int, TupleSlot>::KeyNodePointerPair> values;
  for (int key = 0; key < item_count; key++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = key;
    node->PushBack(p1);
    EXPECT_EQ(node->GetSize(), key + 1);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;
  unsigned i = 0;
  while (node->PopBegin()) {
    i++;
    unsigned key = i;
    for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
      EXPECT_EQ(element_p->first, key);
      key++;
    }
    EXPECT_EQ(key, 10);
  }

  EXPECT_EQ(i, 10);

  // To Check if we are inserting at the correct place
  EXPECT_EQ(reinterpret_cast<char *>(node) + sizeof(BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>>),
            reinterpret_cast<char *>(node->Begin()));

  EXPECT_EQ(&(node->GetLowKeyPair()), node->GetElasticLowKeyPair());
  EXPECT_EQ(&(node->GetHighKeyPair()), node->GetElasticHighKeyPair());
  EXPECT_EQ(node->GetLowKeyPair().first, p1.first);
  EXPECT_EQ(node->GetHighKeyPair().first, p2.first);
  EXPECT_NE(&p1, &(node->GetLowKeyPair()));
  EXPECT_NE(&p2, &(node->GetHighKeyPair()));

  // Free the node - should not result in an ASAN
  node->FreeElasticNode();
  delete bplustree;
}

/**
 * Tests the PopEnd() function in a node.
 */
void PopEndTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  int size = 10;
  int depth = 0;
  int item_count = 10;  // Usually equal to size
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      size, BPlusTree<int, TupleSlot>::NodeType::LeafType, depth, item_count, p1, p2);

  // To check if we can read what we inserted
  for (int key = 0; key < item_count; key++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = key;
    node->PushBack(p1);
    EXPECT_EQ(node->GetSize(), key + 1);
  }

  // using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;
  unsigned key = 9;
  while (node->PopEnd()) {
    if (node->GetSize() <= 0) break;
    key--;
    auto last = node->RBegin();
    EXPECT_EQ(last->first, key);
  }

  EXPECT_EQ(key, 0);

  // To Check if we are inserting at the correct place
  EXPECT_EQ(reinterpret_cast<char *>(node) + sizeof(BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>>),
            reinterpret_cast<char *>(node->Begin()));

  EXPECT_EQ(&(node->GetLowKeyPair()), node->GetElasticLowKeyPair());
  EXPECT_EQ(&(node->GetHighKeyPair()), node->GetElasticHighKeyPair());
  EXPECT_EQ(node->GetLowKeyPair().first, p1.first);
  EXPECT_EQ(node->GetHighKeyPair().first, p2.first);
  EXPECT_NE(&p1, &(node->GetLowKeyPair()));
  EXPECT_NE(&p2, &(node->GetHighKeyPair()));

  // Free the node - should not result in an ASAN
  node->FreeElasticNode();
  delete bplustree;
}

/**
 * Verify that erasing an element from a node works correctly.
 */
void NodeElementEraseTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  int size = 10;
  int depth = 0;
  int item_count = 10;  // Usually equal to size
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      size, BPlusTree<int, TupleSlot>::NodeType::LeafType, depth, item_count, p1, p2);

  // To check if we can read what we inserted
  for (int key = 0; key < item_count; key++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = key;
    node->PushBack(p1);
    EXPECT_EQ(node->GetSize(), key + 1);
  }

  // using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;
  int key = 9;
  while (node->Erase(key)) {
    if (node->GetSize() <= 0) break;
    key--;
    auto last = node->RBegin();
    EXPECT_EQ(last->first, key);
  }

  EXPECT_EQ(key, 0);

  // To Check if we are inserting at the correct place
  EXPECT_EQ(reinterpret_cast<char *>(node) + sizeof(BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>>),
            reinterpret_cast<char *>(node->Begin()));

  EXPECT_EQ(&(node->GetLowKeyPair()), node->GetElasticLowKeyPair());
  EXPECT_EQ(&(node->GetHighKeyPair()), node->GetElasticHighKeyPair());
  EXPECT_EQ(node->GetLowKeyPair().first, p1.first);
  EXPECT_EQ(node->GetHighKeyPair().first, p2.first);
  EXPECT_NE(&p1, &(node->GetLowKeyPair()));
  EXPECT_NE(&p2, &(node->GetHighKeyPair()));

  // Free the node - should not result in an ASAN
  node->FreeElasticNode();
  delete bplustree;
}

/**
 * Test merging by explicitly calling MergeNode on two nodes
 */
void NodeMergeTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  int size = 10;
  int depth = 0;
  int item_count = 10;  // Usually equal to size
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      size, BPlusTree<int, TupleSlot>::NodeType::LeafType, depth, item_count, p1, p2);
  auto next_node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      size, BPlusTree<int, TupleSlot>::NodeType::LeafType, depth, item_count, p1, p2);

  for (int i = 0; i < (item_count / 2); i++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;
    p1.first = i;
    p2.first = i + 5;
    EXPECT_EQ(node->InsertElementIfPossible(p1, node->End()), true);
    EXPECT_EQ(next_node->InsertElementIfPossible(p2, next_node->End()), true);
    EXPECT_EQ(node->GetSize(), i + 1);
    EXPECT_EQ(next_node->GetSize(), i + 1);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;
  EXPECT_EQ(node->MergeNode(next_node), true);

  unsigned key = 0;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, key);
    key++;
  }
  EXPECT_EQ(key, 10);

  // To Check if we are inserting at the correct place
  EXPECT_EQ(reinterpret_cast<char *>(node) + sizeof(BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>>),
            reinterpret_cast<char *>(node->Begin()));

  EXPECT_EQ(&(node->GetLowKeyPair()), node->GetElasticLowKeyPair());
  EXPECT_EQ(&(node->GetHighKeyPair()), node->GetElasticHighKeyPair());
  EXPECT_EQ(node->GetLowKeyPair().first, p1.first);
  EXPECT_EQ(node->GetHighKeyPair().first, p2.first);
  EXPECT_NE(&p1, &(node->GetLowKeyPair()));
  EXPECT_NE(&p2, &(node->GetHighKeyPair()));

  // Free the node - should not result in an ASAN
  node->FreeElasticNode();
  next_node->FreeElasticNode();
  delete bplustree;
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, NodeStructuralTests) {
  BasicNodeInitPushBackTest();
  InsertElementInNodeTest();
  InsertElementInNodeRandomTest();
  SplitNodeTest();
  FindLocationTest();
  PopBeginTest();
  PopEndTest();
  NodeElementEraseTest();
  NodeMergeTest();
}

/**
 * Verify inserts into the B+ Tree without the root splitting.
 */
void BasicBPlusTreeInsertTestNoSplit() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  unsigned key_num = 100;
  auto bplustree = new BPlusTree<int, TupleSlot>;
  for (unsigned key = 0; key < key_num; key++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = key;
    bplustree->Insert(p1, predicate);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyElementPair;

  auto node = reinterpret_cast<BPlusTree<int, TupleSlot>::ElasticNode<ElementType> *>(bplustree->GetRoot());
  unsigned key = 0;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, key);
    key++;
  }
  EXPECT_EQ(key, key_num);

  delete bplustree;
}

/**
 * Verify inserts into B+ Tree splitting the root once.
 */
void BasicBPlusTreeInsertTestRootSplitOnce() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  unsigned key_num = 129;
  auto bplustree = new BPlusTree<int, TupleSlot>;
  for (unsigned key = 0; key < key_num; key++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = key;
    bplustree->Insert(p1, predicate);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyElementPair;
  using KeyPointerType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;

  auto node = reinterpret_cast<BPlusTree<int, TupleSlot>::ElasticNode<ElementType> *>(
      bplustree->GetRoot()->GetLowKeyPair().second);
  auto noderoot = reinterpret_cast<BPlusTree<int, TupleSlot>::ElasticNode<KeyPointerType> *>(bplustree->GetRoot());
  auto node2 = reinterpret_cast<BPlusTree<int, TupleSlot>::ElasticNode<ElementType> *>(noderoot->Begin()->second);
  unsigned key = 0;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, key);
    key++;
  }
  EXPECT_EQ(key, 64);
  for (ElementType *element_p = node2->Begin(); element_p != node2->End(); element_p++) {
    EXPECT_EQ(element_p->first, key);
    key++;
  }
  EXPECT_EQ(key, 129);

  // Count no of elements in root node - should be 1
  key = 0;
  for (KeyPointerType *element_p = noderoot->Begin(); element_p != noderoot->End(); element_p++) key++;

  EXPECT_EQ(key, 1);

  delete bplustree;
}

/**
 * Insert a large number of keys (2-3 fully filled levels with a branching factor of 128), and
 * verify that Scans at the leaf node work properly.
 */
void LargeKeyRandomInsertScanTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  // Insert keys
  unsigned key_num = 100000;
  auto bplustree = new BPlusTree<int, TupleSlot>;
  bplustree->SetInnerNodeSizeUpperThreshold(5);
  bplustree->SetLeafNodeSizeUpperThreshold(5);
  std::set<int> keys;
  for (unsigned i = 0; i < key_num; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand_r(&globalseed) % 500000;
    while (keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    bplustree->Insert(p1, predicate);
  }

  EXPECT_EQ(bplustree->SiblingForwardCheck(&keys), true);
  EXPECT_EQ(bplustree->SiblingBackwardCheck(&keys), true);

  delete bplustree;
}

/**
 * Tests random insert and delete and ensures scan works correctly.
 */
void RandomInsertAndDeleteScanTest() {
  auto predicate = [](const int slot) -> bool { return false; };

  // Insert keys
  auto bplustree = new BPlusTree<int, int>;
  bplustree->SetInnerNodeSizeUpperThreshold(10);
  bplustree->SetLeafNodeSizeUpperThreshold(10);
  bplustree->SetInnerNodeSizeLowerThreshold(4);
  bplustree->SetLeafNodeSizeLowerThreshold(4);
  std::set<int> keys;
  unsigned key_num = 1000;
  for (unsigned i = 0; i < key_num; i++) {
    BPlusTree<int, int>::KeyElementPair p1;
    int k = rand_r(&globalseed) % 500000;
    while (keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    p1.second = k;
    bplustree->Insert(p1, predicate);
  }

  EXPECT_EQ(bplustree->SiblingForwardCheck(&keys), true);
  EXPECT_EQ(bplustree->SiblingBackwardCheck(&keys), true);

  for (unsigned i = 0; i < (key_num / 2); i++) {
    BPlusTree<int, int>::KeyElementPair p1;
    unsigned key_index = rand_r(&globalseed) % keys.size();
    auto it = keys.begin();
    std::advance(it, key_index);
    int k = *it;
    keys.erase(k);
    p1.first = k;
    p1.second = k;
    bplustree->DeleteElement(p1);
  }

  EXPECT_EQ(bplustree->SiblingForwardCheck(&keys), true);
  EXPECT_EQ(bplustree->SiblingBackwardCheck(&keys), true);

  delete bplustree;
}

/**
 * Tests insertion of duplicate key-value pairs in the tree
 */
void DuplicateKeyValueInsertTest() {
  auto predicate = [](const int slot) -> bool { return false; };

  // Insert Keys
  auto bplustree = new BPlusTree<int, int>;
  bplustree->SetInnerNodeSizeUpperThreshold(5);
  bplustree->SetLeafNodeSizeUpperThreshold(5);
  std::unordered_map<int, std::set<int>> keys_values;
  int key_num = 100000;
  for (int i = 0; i < key_num; i++) {
    int k = i % 1000;                  // there will be 100 inserts for same key
    int v = rand_r(&globalseed) % 50;  // expect one duplicate value per key
    if (keys_values.count(k) == 0) {
      std::set<int> value_list;
      value_list.insert(v);
      keys_values[k] = value_list;
    } else {
      if (keys_values[k].count(v) == 0) {
        keys_values[k].insert(v);
      }
    }
    BPlusTree<int, int>::KeyElementPair p1;
    p1.first = k;
    p1.second = v;
    bplustree->Insert(p1, predicate);
  }
  EXPECT_EQ(bplustree->DuplicateKeyValuesCheck(&keys_values), true);

  delete bplustree;
}

/**
 * Tests ScanKey() function in the B+ Tree
 */
void ScanKeyTest() {
  auto predicate = [](const int slot) -> bool { return false; };

  // Insert Keys
  auto bplustree = new BPlusTree<int, int>;
  bplustree->SetInnerNodeSizeUpperThreshold(5);
  bplustree->SetLeafNodeSizeUpperThreshold(5);
  std::unordered_map<int, std::set<int>> keys_values;
  unsigned key_num = 100000;
  for (unsigned i = 0; i < key_num; i++) {
    int k = rand_r(&globalseed) % 1000;
    int v = rand_r(&globalseed) % 500000;
    int is_value_unique = 1;
    if (keys_values.count(k) == 0) {
      std::set<int> value_list;
      value_list.insert(v);
      keys_values[k] = value_list;
    } else {
      if (keys_values[k].count(v) != 0) {
        is_value_unique = 0;
      } else {
        keys_values[k].insert(v);
      }
    }
    bool is_inserted = bplustree->Insert(BPlusTree<int, int>::KeyElementPair(k, v), predicate);
    EXPECT_EQ(is_value_unique, is_inserted);
  }
  auto itr_map = keys_values.begin();
  while (itr_map != keys_values.end()) {
    int k = itr_map->first;
    std::set<int> values = keys_values[k];
    std::vector<int> result;
    bplustree->FindValueOfKey(k, &result);
    for (int i : result) {
      EXPECT_EQ(values.count(i), 1);
      values.erase(i);
    }
    EXPECT_EQ(values.size(), 0);
    itr_map++;
  }

  delete bplustree;
}

/**
 * Tests basic delete with no splitting.
 */
void BasicBPlusTreeDeleteNoSplitTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  unsigned key_num = 100;
  for (unsigned i = 0; i < key_num; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = i;
    bplustree->Insert(p1, predicate);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyElementPair;

  auto node = reinterpret_cast<BPlusTree<int, TupleSlot>::ElasticNode<ElementType> *>(bplustree->GetRoot());
  unsigned i = 0;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, i);
    i++;
  }
  EXPECT_EQ(i, key_num);

  // Delete all values
  for (i = 0; i < key_num; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = i;
    bplustree->DeleteElement(p1);
    EXPECT_EQ(bplustree->IsPresent(i), false);
  }
  EXPECT_EQ(bplustree->GetRoot() == nullptr, true);

  delete bplustree;
}

/**
 * Insert a large number of keys (2-3 levels full) and retrieve the keys.
 */
void LargeKeySequentialInsertAndRetrievalTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  unsigned key_num = 100000;
  auto bplustree = new BPlusTree<int, TupleSlot>;
  for (unsigned key = 0; key < key_num; key++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = key;
    bplustree->Insert(p1, predicate);
  }

  for (unsigned key = 0; key < key_num; key++) {
    EXPECT_EQ(bplustree->IsPresent(key), true);
  }

  for (unsigned key = key_num; key < (key_num * 2); key++) {
    EXPECT_EQ(bplustree->IsPresent(key), false);
  }

  delete bplustree;
}

/**
 * Insert a large number of keys (2-3 levels full) and delete few of them.
 */
void LargeKeySequentialInsertAndDeleteTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  unsigned key_num = 100000;
  for (unsigned key = 0; key < key_num; key++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = key;
    bplustree->Insert(p1, predicate);
  }

  for (unsigned key = 0; key < key_num; key++) {
    EXPECT_EQ(bplustree->IsPresent(key), true);
  }

  for (unsigned key = key_num; key < (key_num * 2); key++) {
    EXPECT_EQ(bplustree->IsPresent(key), false);
  }

  // delete certain elements
  for (unsigned key = 0; key < key_num; key += 2) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = key;
    bplustree->DeleteElement(p1);
  }

  for (unsigned key = 0; key < key_num; key += 2) {
    EXPECT_EQ(bplustree->IsPresent(key), false);
    EXPECT_EQ(bplustree->IsPresent(key + 1), true);
  }

  delete bplustree;
}

/**
 * Insert a large number of keys (2-3 levels full) randomly and retrieve them.
 */
void LargeKeyRandomInsertAndRetrievalTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  bplustree->SetInnerNodeSizeUpperThreshold(5);
  bplustree->SetLeafNodeSizeUpperThreshold(5);
  std::set<int> keys;
  unsigned key_num = 100000;
  for (unsigned i = 0; i < key_num; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand_r(&globalseed) % 500000;
    while (keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    bplustree->Insert(p1, predicate);
  }

  for (int i = 0; i < 500000; i++) {
    if (keys.find(i) != keys.end()) {
      EXPECT_EQ(bplustree->IsPresent(i), true);
    } else {
      EXPECT_EQ(bplustree->IsPresent(i), false);
    }
  }
  EXPECT_EQ(bplustree->GetRoot()->GetDepth(), 7);

  delete bplustree;
}

/**
 * Insert a large number of keys (2-3 levels full) randomly and delete few of them.
 */
void LargeKeyRandomInsertAndDeleteTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  std::set<int> keys;
  int key_num = 100000;
  for (int i = 0; i < key_num; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand_r(&globalseed) % (key_num * 5);
    while (keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    bplustree->Insert(p1, predicate);
  }

  for (int i = 0; i < (key_num * 5); i++) {
    if (keys.find(i) != keys.end()) {
      EXPECT_EQ(bplustree->IsPresent(i), true);
    } else {
      EXPECT_EQ(bplustree->IsPresent(i), false);
    }
  }

  auto iter = keys.begin();
  for (int i = 0; i < (key_num / 2); i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *iter;
    bplustree->DeleteElement(p1);
    iter++;
  }

  iter = keys.begin();
  for (int i = 0; i < (key_num / 2); i++) {
    EXPECT_EQ(bplustree->IsPresent(*iter), false);
    iter++;
  }
  for (int i = (key_num / 2); i < key_num; i++) {
    EXPECT_EQ(bplustree->IsPresent(*iter), true);
    iter++;
  }

  delete bplustree;
}

/**
 * Test duplicate key deletes from the tree.
 */
void DuplicateKeyDeleteTest() {
  auto predicate = [](const int slot) -> bool { return false; };

  // Insert keys
  auto bplustree = new BPlusTree<int, int>;
  bplustree->SetInnerNodeSizeUpperThreshold(10);
  bplustree->SetLeafNodeSizeUpperThreshold(10);
  bplustree->SetInnerNodeSizeLowerThreshold(4);
  bplustree->SetLeafNodeSizeLowerThreshold(4);
  std::map<int, std::set<int>> key_vals;
  int key_num = 10000;
  for (int i = 0; i < 10000; i++) {
    BPlusTree<int, int>::KeyElementPair p1;
    int k = i % (key_num / 100);                          // 100 different keys
    int v = rand_r(&globalseed) % ((key_num / 100) * 5);  // 100 values per key
    if (key_vals.count(k) == 0) {
      std::set<int> s;
      s.insert(v);
      key_vals[k] = s;
    } else {
      while (key_vals[k].count(v) != 0) v++;
      key_vals[k].insert(v);
    }
    p1.first = k;
    p1.second = v;
    bplustree->Insert(p1, predicate);
  }

  for (int i = 0; i < 100; i++) {
    BPlusTree<int, int>::KeyElementPair p1;
    int k = i;
    for (unsigned j = 0; j < 10; j++) {
      // delete 10 vals per key
      auto it = key_vals[k].begin();
      int v = (*it);
      key_vals[k].erase(v);
      p1.first = k;
      p1.second = v;
      bplustree->DeleteElement(p1);
    }
  }

  for (int i = 0; i < 100; i++) {
    int k = i;
    std::set<int> values = key_vals[k];
    std::vector<int> result;
    bplustree->FindValueOfKey(k, &result);
    for (int j : result) {
      EXPECT_EQ(values.count(j), 1);
      values.erase(j);
    }
  }

  delete bplustree;
}

void StructuralIntegrityTestWithRandomInsert() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  // The size is set to 2 more because of the following
  // When we split an inner node, we might end up deleting an element
  // from right side without putting anything in the right side
  // Hence the size may be 31 at some points if we used 64.
  bplustree->SetInnerNodeSizeUpperThreshold(66);
  bplustree->SetLeafNodeSizeUpperThreshold(66);
  bplustree->SetInnerNodeSizeLowerThreshold(32);
  bplustree->SetLeafNodeSizeLowerThreshold(32);
  std::set<int> keys;
  for (unsigned i = 0; i < 100000; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand_r(&globalseed) % 500000;
    while (keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    bplustree->Insert(p1, predicate);
  }

  EXPECT_EQ(bplustree->StructuralIntegrityVerification(*keys.begin(), *keys.rbegin(), &keys, bplustree->GetRoot()),
            true);
  // All keys found in the tree
  EXPECT_EQ(keys.size(), 0);

  delete bplustree;
}

/**
 * Verifies structural integrity while inserting and splitting nodes.
 */
void StructuralIntegrityTestOnInsert() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  bplustree->SetInnerNodeSizeUpperThreshold(10);
  bplustree->SetLeafNodeSizeUpperThreshold(10);
  bplustree->SetInnerNodeSizeLowerThreshold(4);
  bplustree->SetLeafNodeSizeLowerThreshold(4);
  std::set<int> keys;
  for (unsigned i = 0; i < 100; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand_r(&globalseed) % 500;
    while (keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    bplustree->Insert(p1, predicate);
  }

  EXPECT_EQ(bplustree->StructuralIntegrityVerification(*keys.begin(), *keys.rbegin(), &keys, bplustree->GetRoot()),
            true);
  // All keys found in the tree
  EXPECT_EQ(keys.size(), 0);

  delete bplustree;
}

/**
 * Verifies structural integrity while inserting and splitting with very low thresholds
 */
void StructuralIntegrityTestOnLowThreshInsert() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  bplustree->SetInnerNodeSizeUpperThreshold(6);
  bplustree->SetLeafNodeSizeUpperThreshold(6);
  bplustree->SetInnerNodeSizeLowerThreshold(2);
  bplustree->SetLeafNodeSizeLowerThreshold(2);
  std::set<int> keys;
  unsigned key_num = 100;
  for (unsigned i = 0; i < key_num; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand_r(&globalseed) % 500;
    while (keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    bplustree->Insert(p1, predicate);
  }

  EXPECT_EQ(bplustree->StructuralIntegrityVerification(*keys.begin(), *keys.rbegin(), &keys, bplustree->GetRoot()),
            true);
  // All keys found in the tree
  EXPECT_EQ(keys.size(), 0);

  delete bplustree;
}

/**
 * Verify structural integrity with random inserts and deletes involving re-balancing.
 */
void StructuralIntegrityTestWithRandomInsertAndDelete() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  bplustree->SetInnerNodeSizeUpperThreshold(8);
  bplustree->SetLeafNodeSizeUpperThreshold(8);
  bplustree->SetInnerNodeSizeLowerThreshold(3);
  bplustree->SetLeafNodeSizeLowerThreshold(3);
  std::set<int> keys;
  for (unsigned i = 0; i < 15; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand_r(&globalseed) % 500;
    while (keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    bplustree->Insert(p1, predicate);
  }

  for (unsigned i = 0; i < 10; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *keys.begin();
    keys.erase(keys.begin());
    EXPECT_EQ(bplustree->DeleteElement(p1), true);
    std::set<int> newkeys = keys;

    // Structural Integrity Test Everytime
    EXPECT_EQ(
        bplustree->StructuralIntegrityVerification(*newkeys.begin(), *newkeys.rbegin(), &newkeys, bplustree->GetRoot()),
        true);
    EXPECT_EQ(newkeys.size(), 0);
  }

  auto iter = keys.begin();
  for (unsigned i = 0; i < 5; i++) {
    EXPECT_EQ(bplustree->IsPresent(*iter), true);
    iter++;
  }

  // All keys found in the tree
  EXPECT_EQ(keys.size(), 5);

  delete bplustree;
}

/**
 * Verify structural integrity with atleast 2-3 full levels along with inserts and deletes.
 */
void LargeStructuralIntegrityVerificationTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  bplustree->SetInnerNodeSizeUpperThreshold(16);
  bplustree->SetLeafNodeSizeUpperThreshold(16);
  bplustree->SetInnerNodeSizeLowerThreshold(6);
  bplustree->SetLeafNodeSizeLowerThreshold(6);
  std::set<int> keys;
  for (unsigned i = 0; i < 1000; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand_r(&globalseed) % 5000;
    while (keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    bplustree->Insert(p1, predicate);

    auto keys_copy = keys;

    // Structural Integrity Verification Everytime
    EXPECT_EQ(bplustree->StructuralIntegrityVerification(*keys_copy.begin(), *keys_copy.rbegin(), &keys_copy,
                                                         bplustree->GetRoot()),
              true);
    EXPECT_EQ(keys_copy.size(), 0);
  }

  // Delete All keys except one
  for (int i = 0; i < 999; i++) {
    auto iter = keys.begin();
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *iter;
    keys.erase(iter);
    EXPECT_EQ(bplustree->DeleteElement(p1), true);
    std::set<int> newkeys = keys;

    // Structural Integrity Test Everytime
    EXPECT_EQ(
        bplustree->StructuralIntegrityVerification(*newkeys.begin(), *newkeys.rbegin(), &newkeys, bplustree->GetRoot()),
        true);
    EXPECT_EQ(newkeys.size(), 0);
  }

  // Insert Again
  for (unsigned i = 0; i < 1000; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand_r(&globalseed) % 5000;
    while (keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    bplustree->Insert(p1, predicate);
    auto keys_copy = keys;

    // Structural Integrity Verification Everytime
    EXPECT_EQ(bplustree->StructuralIntegrityVerification(*keys_copy.begin(), *keys_copy.rbegin(), &keys_copy,
                                                         bplustree->GetRoot()),
              true);
    EXPECT_EQ(keys_copy.size(), 0);
  }

  // Delete Again now two keys remaining
  for (int i = 0; i < 999; i++) {
    auto iter = keys.begin();
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *iter;
    keys.erase(iter);
    EXPECT_EQ(bplustree->DeleteElement(p1), true);
    std::set<int> newkeys = keys;

    // Structural Integrity Test Everytime
    EXPECT_EQ(
        bplustree->StructuralIntegrityVerification(*newkeys.begin(), *newkeys.rbegin(), &newkeys, bplustree->GetRoot()),
        true);
    EXPECT_EQ(newkeys.size(), 0);
  }

  // Check Both still present
  auto iter = keys.begin();
  for (unsigned i = 0; i < 2; i++) {
    EXPECT_EQ(bplustree->IsPresent(*iter), true);
    iter++;
  }

  // Free Everything
  delete bplustree;
}

/**
 * Similar to the above test, but use reverse iterators
 */
void LargeStructuralIntegrityVerificationTestReverse() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  bplustree->SetInnerNodeSizeUpperThreshold(16);
  bplustree->SetLeafNodeSizeUpperThreshold(16);
  bplustree->SetInnerNodeSizeLowerThreshold(6);
  bplustree->SetLeafNodeSizeLowerThreshold(6);
  std::set<int> keys;
  unsigned key_num = 1000;
  for (unsigned i = 0; i < key_num; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand_r(&globalseed) % 5000;
    while (keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    bplustree->Insert(p1, predicate);

    auto keys_copy = keys;

    // Structural Integrity Verification Everytime
    EXPECT_EQ(bplustree->StructuralIntegrityVerification(*keys_copy.begin(), *keys_copy.rbegin(), &keys_copy,
                                                         bplustree->GetRoot()),
              true);
    EXPECT_EQ(keys_copy.size(), 0);
  }

  // Delete All keys except one - As root empty is not handled by delete yet
  for (unsigned i = 0; i < (key_num - 1); i++) {
    auto iter = keys.rbegin();
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *iter;
    keys.erase(*iter);
    EXPECT_EQ(bplustree->DeleteElement(p1), true);
    std::set<int> newkeys = keys;

    // Structural Integrity Test Everytime
    EXPECT_EQ(
        bplustree->StructuralIntegrityVerification(*newkeys.begin(), *newkeys.rbegin(), &newkeys, bplustree->GetRoot()),
        true);
    EXPECT_EQ(newkeys.size(), 0);
  }

  // Insert Again
  for (unsigned i = 0; i < key_num; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand_r(&globalseed) % 5000;
    while (keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    bplustree->Insert(p1, predicate);

    auto keys_copy = keys;

    // Structural Integrity Verification Everytime
    EXPECT_EQ(bplustree->StructuralIntegrityVerification(*keys_copy.begin(), *keys_copy.rbegin(), &keys_copy,
                                                         bplustree->GetRoot()),
              true);
    EXPECT_EQ(keys_copy.size(), 0);
  }

  // Delete Again now two keys remaining
  for (unsigned i = 0; i < (key_num - 1); i++) {
    auto iter = keys.rbegin();
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *iter;
    keys.erase(*iter);
    EXPECT_EQ(bplustree->DeleteElement(p1), true);
    std::set<int> newkeys = keys;

    // Structural Integrity Test Everytime
    EXPECT_EQ(
        bplustree->StructuralIntegrityVerification(*newkeys.begin(), *newkeys.rbegin(), &newkeys, bplustree->GetRoot()),
        true);
    EXPECT_EQ(newkeys.size(), 0);
  }

  // Check Both still present
  auto iter = keys.begin();
  for (unsigned i = 0; i < 2; i++) {
    EXPECT_EQ(bplustree->IsPresent(*iter), true);
    iter++;
  }

  // Free Everything

  delete bplustree;
}

/**
 * Test insert delete and re-insert in B+ Tree
 */
void BPlusTreeCompleteDeleteAndReinsertTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  bplustree->SetInnerNodeSizeUpperThreshold(8);
  bplustree->SetLeafNodeSizeUpperThreshold(8);
  bplustree->SetInnerNodeSizeLowerThreshold(3);
  bplustree->SetLeafNodeSizeLowerThreshold(3);
  std::set<int> keys;
  unsigned key_num = 30;
  for (unsigned i = 0; i < key_num; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    keys.insert(i);
    p1.first = i;
    bplustree->Insert(p1, predicate);
  }

  for (unsigned i = 0; i < key_num; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *keys.begin();
    keys.erase(keys.begin());
    bplustree->DeleteElement(p1);
    std::set<int> newkeys = keys;
    if (bplustree->GetRoot() != nullptr) {
      EXPECT_EQ(bplustree->StructuralIntegrityVerification(*newkeys.begin(), *newkeys.rbegin(), &newkeys,
                                                           bplustree->GetRoot()),
                true);
    } else {
      EXPECT_EQ(bplustree->GetRoot() == nullptr, true);
    }
  }

  for (unsigned i = 0; i < key_num; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    keys.insert(i);
    p1.first = i;
    bplustree->Insert(p1, predicate);

    std::set<int> newkeys = keys;
    if (bplustree->GetRoot() != nullptr) {
      EXPECT_EQ(bplustree->StructuralIntegrityVerification(*newkeys.begin(), *newkeys.rbegin(), &newkeys,
                                                           bplustree->GetRoot()),
                true);
    } else {
      EXPECT_EQ(bplustree->GetRoot() == nullptr, true);
    }
  }

  for (unsigned i = 0; i < key_num; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *keys.begin();
    keys.erase(keys.begin());
    bplustree->DeleteElement(p1);
    std::set<int> newkeys = keys;
    if (bplustree->GetRoot() != nullptr) {
      EXPECT_EQ(bplustree->StructuralIntegrityVerification(*newkeys.begin(), *newkeys.rbegin(), &newkeys,
                                                           bplustree->GetRoot()),
                true);
    } else {
      EXPECT_EQ(bplustree->GetRoot() == nullptr, true);
    }
  }

  // All keys found in the tree
  EXPECT_EQ(keys.size(), 0);

  delete bplustree;
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, InsertTests) {
  BasicBPlusTreeInsertTestNoSplit();
  BasicBPlusTreeInsertTestRootSplitOnce();
  LargeKeyRandomInsertScanTest();
  DuplicateKeyValueInsertTest();
  ScanKeyTest();
  LargeKeySequentialInsertAndRetrievalTest();
  LargeKeyRandomInsertAndRetrievalTest();
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, DeleteTests) {
  BasicBPlusTreeDeleteNoSplitTest();
  LargeKeySequentialInsertAndDeleteTest();
  LargeKeyRandomInsertAndDeleteTest();
  DuplicateKeyDeleteTest();
  RandomInsertAndDeleteScanTest();
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, StructuralIntegrityTests) {
  StructuralIntegrityTestWithRandomInsert();
  StructuralIntegrityTestOnInsert();
  StructuralIntegrityTestOnLowThreshInsert();
  StructuralIntegrityTestWithRandomInsertAndDelete();
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, LargeTests) {
  LargeStructuralIntegrityVerificationTest();
  LargeStructuralIntegrityVerificationTestReverse();
  BPlusTreeCompleteDeleteAndReinsertTest();
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, MultiThreadedInsertTest) {
  /**
   * Tests multi-threaded insert on the B+ Tree
   */
  std::function<bool(const int64_t)> predicate = [](const int64_t slot) -> bool { return false; };
  const int key_num = 1000 * 1000;

  auto *const tree = new BPlusTree<int64_t, int64_t>;
  std::vector<int64_t> keys;
  keys.reserve(key_num);
  int64_t work_per_thread = key_num / num_threads_;
  for (int64_t i = 0; i < key_num; ++i) {
    keys.emplace_back(i);
  }
  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT

  auto workload = [&](uint32_t worker_id) {
    int64_t start = work_per_thread * worker_id;
    int64_t end = work_per_thread * (worker_id + 1);

    // Inserts the keys
    for (int i = start; i < end; i++) {
      BPlusTree<int64_t, int64_t>::KeyElementPair p1;
      p1.first = keys[i];
      p1.second = keys[i];
      tree->Insert(p1, predicate);
    }
  };

  // Run the workload
  for (uint32_t i = 0; i < num_threads_; i++) {
    thread_pool_.SubmitTask([i, &workload] { workload(i); });
  }
  thread_pool_.WaitUntilAllFinished();

  EXPECT_EQ(tree->GetSize(), key_num);

  // Ensure all values are present
  for (int i = 0; i < key_num; i++) {
    std::vector<int64_t> results;
    tree->FindValueOfKey(keys[i], &results);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], keys[i]);
  }

  // Verify Structural Integrity
  std::set<int64_t> keys_present(keys.begin(), keys.end());
  auto low_key = *(std::min_element(keys_present.begin(), keys_present.end()));
  auto high_key = *(std::max_element(keys_present.begin(), keys_present.end()));
  EXPECT_EQ(tree->StructuralIntegrityVerification(low_key, high_key, &keys_present, tree->GetRoot()), true);

  delete tree;
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, MultiThreadedDeleteTest) {
  /**
   * Tests multi-threaded delete on the B+ Tree
   */
  auto predicate = [](const int64_t slot) -> bool { return false; };
  const int key_num = 1000 * 1000;

  auto *const tree = new BPlusTree<int64_t, int64_t>;
  std::vector<int64_t> keys;
  keys.reserve(key_num);

  for (int64_t i = 0; i < key_num; ++i) {
    keys.emplace_back(i);
  }
  std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});  // NOLINT
  for (int i = 0; i < key_num; i++) {
    BPlusTree<int64_t, int64_t>::KeyElementPair p1;
    p1.first = keys[i];
    p1.second = keys[i];
    tree->Insert(p1, predicate);
  }

  const int deleted_keys = key_num / 2;
  int64_t work_per_thread = deleted_keys / num_threads_;

  auto workload = [&](uint32_t worker_id) {
    int64_t start = work_per_thread * worker_id;
    int64_t end = work_per_thread * (worker_id + 1);

    // Delete the keys
    for (int i = start; i < end; i++) {
      BPlusTree<int64_t, int64_t>::KeyElementPair p1;
      p1.first = keys[i];
      p1.second = keys[i];
      tree->DeleteElement(p1);
    }
  };

  // Run the workload
  for (uint32_t i = 0; i < num_threads_; i++) {
    thread_pool_.SubmitTask([i, &workload] { workload(i); });
  }
  thread_pool_.WaitUntilAllFinished();

  EXPECT_EQ(tree->GetSize(), deleted_keys);

  // Ensure all values are present
  for (int i = deleted_keys; i < key_num; i++) {
    std::vector<int64_t> results;
    tree->FindValueOfKey(keys[i], &results);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], keys[i]);
  }

  // Verify Structural Integrity
  std::set<int64_t> rem_keys(keys.begin() + deleted_keys, keys.end());
  auto low_key = *(std::min_element(rem_keys.begin(), rem_keys.end()));
  auto high_key = *(std::max_element(rem_keys.begin(), rem_keys.end()));
  EXPECT_EQ(tree->StructuralIntegrityVerification(low_key, high_key, &rem_keys, tree->GetRoot()), true);

  delete tree;
}

}  // namespace noisepage::storage::index
