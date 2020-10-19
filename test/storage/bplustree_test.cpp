#include <cstdlib>
#include <set>
#include <unordered_map>

#include "storage/index/bplustree.h"
#include "storage/storage_defs.h"
#include "test_util/test_harness.h"

namespace terrier::storage::index {

unsigned int globalseed = 9;

struct BPlusTreeTests : public TerrierTest {};

void BasicNodeInitializationInsertReadAndFreeTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);

  // To check if we can read what we inserted
  std::vector<BPlusTree<int, TupleSlot>::KeyNodePointerPair> values;
  for (unsigned i = 0; i < 10; i++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = i;
    values.push_back(p1);
    node->PushBack(p1);
    EXPECT_EQ(node->GetSize(), i + 1);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;

  unsigned i = 0;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, i);
    i++;
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

void InsertElementInNodeTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);

  for (unsigned i = 0; i < 10; i++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = i;
    EXPECT_EQ(node->InsertElementIfPossible(p1, node->Begin()), true);
    EXPECT_EQ(node->GetSize(), i + 1);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;

  unsigned i = 9;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, i);
    i--;
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

void InsertElementInNodeRandomTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);

  std::map<int, int> positions;
  for (unsigned i = 0; i < 10; i++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = i;
    int k;
    k = rand_r(&globalseed) % (node->GetSize() + 1);
    while (positions.find(k) != positions.end()) k = (k + 1) % (node->GetSize() + 1);
    EXPECT_EQ(node->InsertElementIfPossible(p1, node->Begin() + k), true);
    positions[k] = i;
    EXPECT_EQ(node->GetSize(), i + 1);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;

  unsigned i = 0;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, positions[i]);
    i++;
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

void SplitNodeTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);

  for (unsigned i = 0; i < 10; i++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = i;
    EXPECT_EQ(node->InsertElementIfPossible(p1, node->End()), true);
    EXPECT_EQ(node->GetSize(), i + 1);
  }

  auto newnode = node->SplitNode();

  using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;

  unsigned i = 0;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, i);
    i++;
  }

  EXPECT_EQ(i, 5);

  for (ElementType *element_p = newnode->Begin(); element_p != newnode->End(); element_p++) {
    EXPECT_EQ(element_p->first, i);
    i++;
  }

  EXPECT_EQ(i, 10);

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

void FindLocationTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);

  std::set<unsigned> s;
  while (node->GetSize() < node->GetItemCount()) {
    int k = rand_r(&globalseed);
    while (s.find(k) != s.end()) k++;
    s.insert(k);
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p;
    p.first = k;
    EXPECT_EQ(node->InsertElementIfPossible(p, node->FindLocation(k, bplustree)), true);
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

void PopBeginTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);

  // To check if we can read what we inserted
  std::vector<BPlusTree<int, TupleSlot>::KeyNodePointerPair> values;
  for (unsigned i = 0; i < 10; i++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = i;
    node->PushBack(p1);
    EXPECT_EQ(node->GetSize(), i + 1);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;
  unsigned i = 0;
  while (node->PopBegin()) {
    i++;
    unsigned j = i;
    for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
      EXPECT_EQ(element_p->first, j);
      j++;
    }
    EXPECT_EQ(j, 10);
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

void PopEndTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);

  // To check if we can read what we inserted
  for (unsigned i = 0; i < 10; i++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = i;
    node->PushBack(p1);
    EXPECT_EQ(node->GetSize(), i + 1);
  }

  // using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;
  unsigned i = 9;
  while (node->PopEnd()) {
    if (node->GetSize() <= 0) break;
    i--;
    auto last = node->RBegin();
    EXPECT_EQ(last->first, i);
  }

  EXPECT_EQ(i, 0);

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

void NodeElementEraseTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);

  // To check if we can read what we inserted
  for (unsigned i = 0; i < 10; i++) {
    BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
    p1.first = i;
    node->PushBack(p1);
    EXPECT_EQ(node->GetSize(), i + 1);
  }

  // using ElementType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;
  int i = 9;
  while (node->Erase(i)) {
    if (node->GetSize() <= 0) break;
    i--;
    auto last = node->RBegin();
    EXPECT_EQ(last->first, i);
  }

  EXPECT_EQ(i, 0);

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

void NodeMergeTest() {
  auto bplustree = new BPlusTree<int, TupleSlot>;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p1;
  BPlusTree<int, TupleSlot>::KeyNodePointerPair p2;

  // Get inner Node
  auto node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);
  auto next_node = BPlusTree<int, TupleSlot>::ElasticNode<BPlusTree<int, TupleSlot>::KeyNodePointerPair>::Get(
      10, BPlusTree<int, TupleSlot>::NodeType::LeafType, 0, 10, p1, p2);

  for (unsigned i = 0; i < 5; i++) {
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

  unsigned i = 0;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, i);
    i++;
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
  next_node->FreeElasticNode();
  delete bplustree;
}

// NOLINTNEXTLINE
TEST_F(BPlusTreeTests, NodeStructuralTests) {
BasicNodeInitializationInsertReadAndFreeTest();
InsertElementInNodeTest();
InsertElementInNodeRandomTest();
SplitNodeTest();
FindLocationTest();
PopBeginTest();
PopEndTest();
NodeElementEraseTest();
NodeMergeTest();
}

void BasicBPlusTreeInsertTestNoSplittingOfRoot() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  for (unsigned i = 0; i < 100; i++) {
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
  EXPECT_EQ(i, 100);

  delete bplustree;
}

void BasicBPlusTreeInsertTestSplittingOfRootOnce() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  for (unsigned i = 0; i < 129; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = i;
    bplustree->Insert(p1, predicate);
  }

  using ElementType = BPlusTree<int, TupleSlot>::KeyElementPair;
  using KeyPointerType = BPlusTree<int, TupleSlot>::KeyNodePointerPair;

  auto node = reinterpret_cast<BPlusTree<int, TupleSlot>::ElasticNode<ElementType> *>(
      bplustree->GetRoot()->GetLowKeyPair().second);
  auto noderoot = reinterpret_cast<BPlusTree<int, TupleSlot>::ElasticNode<KeyPointerType> *>(bplustree->GetRoot());
  auto node2 = reinterpret_cast<BPlusTree<int, TupleSlot>::ElasticNode<ElementType> *>(noderoot->Begin()->second);
  unsigned i = 0;
  for (ElementType *element_p = node->Begin(); element_p != node->End(); element_p++) {
    EXPECT_EQ(element_p->first, i);
    i++;
  }
  EXPECT_EQ(i, 64);
  for (ElementType *element_p = node2->Begin(); element_p != node2->End(); element_p++) {
    EXPECT_EQ(element_p->first, i);
    i++;
  }
  EXPECT_EQ(i, 129);

  // Count no of elements in root node - should be 1
  i = 0;
  for (KeyPointerType *element_p = noderoot->Begin(); element_p != noderoot->End(); element_p++) i++;

  EXPECT_EQ(i, 1);

  // Only freeing these should free us of any ASAN

  delete bplustree;
}

void LargeKeyRandomInsertSiblingSequenceTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  // Insert keys
  auto bplustree = new BPlusTree<int, TupleSlot>;
  bplustree->SetInnerNodeSizeUpperThreshold(5);
  bplustree->SetLeafNodeSizeUpperThreshold(5);
  std::set<int> keys;
  for (unsigned i = 0; i < 100000; i++) {
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

void KeyRandomInsertAndDeleteSiblingSequenceTest() {
  auto predicate = [](const int slot) -> bool { return false; };

  // Insert keys
  auto bplustree = new BPlusTree<int, int>;
  bplustree->SetInnerNodeSizeUpperThreshold(10);
  bplustree->SetLeafNodeSizeUpperThreshold(10);
  bplustree->SetInnerNodeSizeLowerThreshold(4);
  bplustree->SetLeafNodeSizeLowerThreshold(4);
  std::set<int> keys;
  for (unsigned i = 0; i < 1000; i++) {
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

  for (unsigned i = 0; i < 500; i++) {
    BPlusTree<int, int>::KeyElementPair p1;
    unsigned key_index = rand_r(&globalseed) % keys.size();
    auto it = keys.begin();
    std::advance(it, key_index);
    int k = *it;
    keys.erase(k);
    p1.first = k;
    p1.second = k;
    bplustree->DeleteWithLock(p1);
  }

  EXPECT_EQ(bplustree->SiblingForwardCheck(&keys), true);
  EXPECT_EQ(bplustree->SiblingBackwardCheck(&keys), true);

  delete bplustree;
}

void DuplicateKeyValueInsertTest() {
  auto predicate = [](const int slot) -> bool { return false; };

  // Insert Keys
  auto bplustree = new BPlusTree<int, int>;
  bplustree->SetInnerNodeSizeUpperThreshold(5);
  bplustree->SetLeafNodeSizeUpperThreshold(5);
  std::unordered_map<int, std::set<int>> keys_values;
  for (int i = 0; i < 100000; i++) {
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
void ScanKeyTest() {
  auto predicate = [](const int slot) -> bool { return false; };

  // Insert Keys
  auto bplustree = new BPlusTree<int, int>;
  bplustree->SetInnerNodeSizeUpperThreshold(5);
  bplustree->SetLeafNodeSizeUpperThreshold(5);
  std::unordered_map<int, std::set<int>> keys_values;
  for (unsigned i = 0; i < 100000; i++) {
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

void BasicBPlusTreeDeleteTestNoSplittingOfRoot() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  for (unsigned i = 0; i < 100; i++) {
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
  EXPECT_EQ(i, 100);

  // Delete all values
  for (i = 0; i < 100; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = i;
    bplustree->DeleteWithLock(p1);
    EXPECT_EQ(bplustree->IsPresent(i), false);
  }
  EXPECT_EQ(bplustree->GetRoot() == nullptr, true);
  /*We should not call free node here*/

  delete bplustree;
}

void LargeKeySequentialInsertAndRetrievalTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  for (unsigned i = 0; i < 100000; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = i;
    bplustree->Insert(p1, predicate);
  }

  for (int i = 0; i < 100000; i++) {
    EXPECT_EQ(bplustree->IsPresent(i), true);
  }

  for (int i = 100000; i < 200000; i++) {
    EXPECT_EQ(bplustree->IsPresent(i), false);
  }

  delete bplustree;
}

void LargeKeySequentialInsertAndDeleteTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  for (unsigned i = 0; i < 100000; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = i;
    bplustree->Insert(p1, predicate);
  }

  for (int i = 0; i < 100000; i++) {
    EXPECT_EQ(bplustree->IsPresent(i), true);
  }

  for (int i = 100000; i < 200000; i++) {
    EXPECT_EQ(bplustree->IsPresent(i), false);
  }

  // delete certain elements
  for (unsigned i = 0; i < 100000; i += 2) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = i;
    bplustree->DeleteWithLock(p1);
  }

  for (int i = 0; i < 100000; i += 2) {
    EXPECT_EQ(bplustree->IsPresent(i), false);
    EXPECT_EQ(bplustree->IsPresent(i + 1), true);
  }

  delete bplustree;
}

void LargeKeyRandomInsertAndRetrievalTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  bplustree->SetInnerNodeSizeUpperThreshold(5);
  bplustree->SetLeafNodeSizeUpperThreshold(5);
  std::set<int> keys;
  for (unsigned i = 0; i < 100000; i++) {
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
  // hardcoded - maybe wrong
  EXPECT_EQ(bplustree->GetRoot()->GetDepth(), 7);

  delete bplustree;
}

void LargeKeyRandomInsertAndDeleteTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  std::set<int> keys;
  for (unsigned i = 0; i < 100000; i++) {
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

  auto iter = keys.begin();
  for (unsigned i = 0; i < 50000; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *iter;
    bplustree->DeleteWithLock(p1);
    iter++;
  }

  iter = keys.begin();
  for (unsigned i = 0; i < 50000; i++) {
    EXPECT_EQ(bplustree->IsPresent(*iter), false);
    iter++;
  }
  for (unsigned i = 50000; i < 100000; i++) {
    EXPECT_EQ(bplustree->IsPresent(*iter), true);
    iter++;
  }

  delete bplustree;
}

void DuplicateKeyDeleteTest() {
  auto predicate = [](const int slot) -> bool { return false; };

  // Insert keys
  auto bplustree = new BPlusTree<int, int>;
  bplustree->SetInnerNodeSizeUpperThreshold(10);
  bplustree->SetLeafNodeSizeUpperThreshold(10);
  bplustree->SetInnerNodeSizeLowerThreshold(4);
  bplustree->SetLeafNodeSizeLowerThreshold(4);
  std::map<int, std::set<int>> key_vals;
  for (int i = 0; i < 10000; i++) {
    BPlusTree<int, int>::KeyElementPair p1;
    int k = i % 100;                     // 100 different keys
    int v = rand_r(&globalseed) % 5000;  // 100 values per key
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
      bplustree->DeleteWithLock(p1);
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

void StructuralIntegrityTestWithCornerCase() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  // The size is set to 2 more because of the following
  // When we split an inner node, we might end up deleting an element
  // from right side without putting anything in the right side
  // Hence the size may be 31 at some points if we used 64.
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

void StructuralIntegrityTestWithCornerCase2() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  // The size is set to 2 more because of the following
  // When we split an inner node, we might end up deleting an element
  // from right side without putting anything in the right side
  // Hence the size may be 31 at some points if we used 64.
  bplustree->SetInnerNodeSizeUpperThreshold(6);
  bplustree->SetLeafNodeSizeUpperThreshold(6);
  bplustree->SetInnerNodeSizeLowerThreshold(2);
  bplustree->SetLeafNodeSizeLowerThreshold(2);
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

void StructuralIntegrityTestWithRandomInsertAndDelete() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  // The size is set to 2 more because of the following
  // When we split an inner node, we might end up deleting an element
  // from right side without putting anything in the right side
  // Hence the size may be 31 at some points if we used 64.
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
    EXPECT_EQ(bplustree->DeleteWithLock(p1), true);
    std::set<int> newkeys = keys;

    // Structural Integrity Test Everytime
    EXPECT_EQ(
        bplustree->StructuralIntegrityVerification(*newkeys.begin(), *newkeys.rbegin(), &newkeys, bplustree->GetRoot()),
        true);
    EXPECT_EQ(newkeys.size(), 0);
  }

  auto iter = keys.begin();
  for (unsigned i = 0; i < 5; i++) {
    // std::cout<<"Checking"<<*iter<<std::endl;
    EXPECT_EQ(bplustree->IsPresent(*iter), true);
    iter++;
  }

  // All keys found in the tree
  EXPECT_EQ(keys.size(), 5);

  delete bplustree;
}

void LargeStructuralIntegrityVerificationTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  // The size is set to 2 more because of the following
  // When we split an inner node, we might end up deleting an element
  // from right side without putting anything in the right side
  // Hence the size may be 31 at some points if we used 64.
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

  // Delete All keys except one - As root empty is not handled by delete yet
  for (int i = 0; i < 999; i++) {
    auto iter = keys.begin();
    // int k = rand_r(&globalseed) % keys.size();
    // for(int j = 0; j < k; j++) iter++;
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *iter;
    keys.erase(iter);
    EXPECT_EQ(bplustree->DeleteWithLock(p1), true);
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
    // int k = rand_r(&globalseed) % keys.size();
    // for(int j = 0; j < k; j++) iter++;
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *iter;
    keys.erase(iter);
    EXPECT_EQ(bplustree->DeleteWithLock(p1), true);
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
    // std::cout<<"Checking"<<*iter<<std::endl;
    EXPECT_EQ(bplustree->IsPresent(*iter), true);
    iter++;
  }

  // Free Everything

  delete bplustree;
}

void LargeStructuralIntegrityVerificationTestReverse() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  // The size is set to 2 more because of the following
  // When we split an inner node, we might end up deleting an element
  // from right side without putting anything in the right side
  // Hence the size may be 31 at some points if we used 64.
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

  // Delete All keys except one - As root empty is not handled by delete yet
  for (int i = 0; i < 999; i++) {
    auto iter = keys.rbegin();
    // int k = rand_r(&globalseed) % keys.size();
    // for(int j = 0; j < k; j++) iter++;
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *iter;
    keys.erase(*iter);
    EXPECT_EQ(bplustree->DeleteWithLock(p1), true);
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
    auto iter = keys.rbegin();
    // int k = rand_r(&globalseed) % keys.size();
    // for(int j = 0; j < k; j++) iter++;
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *iter;
    keys.erase(*iter);
    EXPECT_EQ(bplustree->DeleteWithLock(p1), true);
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
    // std::cout<<"Checking"<<*iter<<std::endl;
    EXPECT_EQ(bplustree->IsPresent(*iter), true);
    iter++;
  }

  // Free Everything

  delete bplustree;
}

void StructuralIntegrityTestWithRandomInsertAndDelete2() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  // The size is set to 2 more because of the following
  // When we split an inner node, we might end up deleting an element
  // from right side without putting anything in the right side
  // Hence the size may be 31 at some points if we used 64.
  bplustree->SetInnerNodeSizeUpperThreshold(6);
  bplustree->SetLeafNodeSizeUpperThreshold(6);
  bplustree->SetInnerNodeSizeLowerThreshold(2);
  bplustree->SetLeafNodeSizeLowerThreshold(2);
  std::set<int> keys;

  // std::cout << "Inserting " << std::endl;
  for (unsigned i = 0; i < 37; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand_r(&globalseed) % 60;
    while (keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    bplustree->Insert(p1, predicate);
  }

  // bplustree->PrintTree();

  // std::cout << "Deleting " << std::endl;
  auto it = keys.begin();
  for (unsigned i = 0; i < keys.size() / 2; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *(it);
    it++;
    // keys.erase(keys.begin());
    EXPECT_EQ(bplustree->DeleteWithLock(p1), true);
    // std::cout << "-----------------Deleted " << p1.first << std::endl;
    // bplustree->PrintTree();
  }

  // bplustree->PrintTree();

  for (unsigned i = keys.size() / 2; i < keys.size(); i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *(it);
    it++;
    // std::cout << "Finding " << p1.first << std::endl;
    EXPECT_EQ(bplustree->IsPresent(p1.first), true);
  }

  // EXPECT_EQ(bplustree->StructuralIntegrityVerification(*keys.begin(), *keys.rbegin(),
  //   keys, bplustree->GetRoot()), true);
  // All keys found in the tree
  // EXPECT_EQ(keys.size(), 0);

  delete bplustree;
}

void StructuralIntegrityTestWithRandomInsertAndDelete2Reverse() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  // The size is set to 2 more because of the following
  // When we split an inner node, we might end up deleting an element
  // from right side without putting anything in the right side
  // Hence the size may be 31 at some points if we used 64.
  bplustree->SetInnerNodeSizeUpperThreshold(6);
  bplustree->SetLeafNodeSizeUpperThreshold(6);
  bplustree->SetInnerNodeSizeLowerThreshold(2);
  bplustree->SetLeafNodeSizeLowerThreshold(2);
  std::set<int> keys;

  // std::cout << "Inserting " << std::endl;
  for (unsigned i = 0; i < 37; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    int k = rand_r(&globalseed) % 60;
    while (keys.find(k) != keys.end()) k++;
    keys.insert(k);
    p1.first = k;
    bplustree->Insert(p1, predicate);
  }

  // bplustree->PrintTree();

  // std::cout << "Deleting " << std::endl;
  auto it = keys.rbegin();
  for (unsigned i = 0; i < keys.size() / 2; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *(it);
    it++;
    // keys.erase(keys.begin());
    EXPECT_EQ(bplustree->DeleteWithLock(p1), true);
    // std::cout << "-----------------Deleted " << p1.first << std::endl;
    // bplustree->PrintTree();
  }

  // bplustree->PrintTree();

  for (unsigned i = keys.size() / 2; i < keys.size(); i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *(it);
    it++;
    // std::cout << "Finding " << p1.first << std::endl;
    EXPECT_EQ(bplustree->IsPresent(p1.first), true);
  }

  // EXPECT_EQ(bplustree->StructuralIntegrityVerification(*keys.begin(), *keys.rbegin(),
  //   keys, bplustree->GetRoot()), true);
  // All keys found in the tree
  // EXPECT_EQ(keys.size(), 0);

  delete bplustree;
}

void BPlusTreeCompleteDeleteAndReinsertTest() {
  auto predicate = [](const TupleSlot slot) -> bool { return false; };

  auto bplustree = new BPlusTree<int, TupleSlot>;
  // The size is set to 2 more because of the following
  // When we split an inner node, we might end up deleting an element
  // from right side without putting anything in the right side
  // Hence the size may be 31 at some points if we used 64.
  bplustree->SetInnerNodeSizeUpperThreshold(8);
  bplustree->SetLeafNodeSizeUpperThreshold(8);
  bplustree->SetInnerNodeSizeLowerThreshold(3);
  bplustree->SetLeafNodeSizeLowerThreshold(3);
  std::set<int> keys;
  for (unsigned i = 0; i < 30; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    keys.insert(i);
    p1.first = i;
    bplustree->Insert(p1, predicate);
  }

  for (unsigned i = 0; i < 30; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *keys.begin();
    keys.erase(keys.begin());
    bplustree->DeleteWithLock(p1);
    std::set<int> newkeys = keys;
    if (bplustree->GetRoot() != nullptr) {
      EXPECT_EQ(bplustree->StructuralIntegrityVerification(*newkeys.begin(), *newkeys.rbegin(), &newkeys,
                                                           bplustree->GetRoot()),
                true);
    } else {
      EXPECT_EQ(bplustree->GetRoot() == nullptr, true);
    }
  }

  for (unsigned i = 0; i < 30; i++) {
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

  for (unsigned i = 0; i < 30; i++) {
    BPlusTree<int, TupleSlot>::KeyElementPair p1;
    p1.first = *keys.begin();
    keys.erase(keys.begin());
    bplustree->DeleteWithLock(p1);
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
BasicBPlusTreeInsertTestNoSplittingOfRoot();
BasicBPlusTreeInsertTestSplittingOfRootOnce();
LargeKeyRandomInsertSiblingSequenceTest();
KeyRandomInsertAndDeleteSiblingSequenceTest();
DuplicateKeyValueInsertTest();
ScanKeyTest();
LargeKeySequentialInsertAndRetrievalTest();
LargeKeyRandomInsertAndRetrievalTest();
StructuralIntegrityTestWithRandomInsert();
StructuralIntegrityTestWithCornerCase();
StructuralIntegrityTestWithCornerCase2();
BasicBPlusTreeDeleteTestNoSplittingOfRoot();
LargeKeySequentialInsertAndDeleteTest();
LargeKeyRandomInsertAndDeleteTest();
DuplicateKeyDeleteTest();
StructuralIntegrityTestWithRandomInsertAndDelete();
StructuralIntegrityTestWithRandomInsertAndDelete2();
LargeStructuralIntegrityVerificationTest();
StructuralIntegrityTestWithRandomInsertAndDelete2Reverse();
LargeStructuralIntegrityVerificationTestReverse();
BPlusTreeCompleteDeleteAndReinsertTest();
}

}  // namespace terrier::storage::index