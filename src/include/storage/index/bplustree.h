#pragma once

#include <cstring>
#include <functional>
#include <iostream>
#include <list>
#include <queue>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/shared_latch.h"
#include "storage/index/index.h"
#include "storage/index/index_defs.h"

#include "loggers/index_logger.h"

namespace noisepage::storage::index {

// If node size goes above this then we split it
// Thresholds for Inner Node
#define INNER_NODE_SIZE_UPPER_THRESHOLD ((int)128)
#define INNER_NODE_SIZE_LOWER_THRESHOLD ((int)64)

// Thresholds for Leaf Node
#define LEAF_NODE_SIZE_UPPER_THRESHOLD ((int)128)
#define LEAF_NODE_SIZE_LOWER_THRESHOLD ((int)64)

/**
 *  Base class for BPlusTree that stores common data, inherited by the BPlusTree class. This
 *  class stores the threshold parameters specific to the B+ Tree.
 */
class BPlusTreeBase {
 public:
  /**
   * @return inner_node_size_upper_threshold
   */
  int GetInnerNodeSizeUpperThreshold() const { return inner_node_size_upper_threshold_; }
  /**
   * @return inner_node_size_lower_threshold
   */
  int GetInnerNodeSizeLowerThreshold() const { return inner_node_size_lower_threshold_; }
  /**
   * @return leaf_node_size_upper_threshold
   */
  int GetLeafNodeSizeUpperThreshold() const { return leaf_node_size_upper_threshold_; }
  /**
   * @return leaf_node_size_lower_threshold
   */
  int GetLeafNodeSizeLowerThreshold() const { return leaf_node_size_lower_threshold_; }

 public:
  /**
   * @param inner_node_size_upper_threshold upper size threshold for inner node split to be assigned to this tree
   */
  void SetInnerNodeSizeUpperThreshold(int inner_node_size_upper_threshold) {
    inner_node_size_upper_threshold_ = inner_node_size_upper_threshold;
  }

  /**
   * @param inner_node_size_lower_threshold lower size threshold for inner node removal to be assigned to this tree
   */
  void SetInnerNodeSizeLowerThreshold(int inner_node_size_lower_threshold) {
    inner_node_size_lower_threshold_ = inner_node_size_lower_threshold;
  }

  /**
   * @param leaf_node_size_upper_threshold upper size threshold for leaf node split to be assigned to this tree
   */
  void SetLeafNodeSizeUpperThreshold(int leaf_node_size_upper_threshold) {
    leaf_node_size_upper_threshold_ = leaf_node_size_upper_threshold;
  }

  /**
   * @param leaf_node_size_lower_threshold lower size threshold for leaf node removal to be assigned to this tree
   */
  void SetLeafNodeSizeLowerThreshold(int leaf_node_size_lower_threshold) {
    leaf_node_size_lower_threshold_ = leaf_node_size_lower_threshold;
  }

 protected:
  /** upper size threshold for inner node split */
  int inner_node_size_upper_threshold_ = INNER_NODE_SIZE_UPPER_THRESHOLD;
  /** lower size threshold for inner node removal */
  int inner_node_size_lower_threshold_ = INNER_NODE_SIZE_LOWER_THRESHOLD;

  /** upper size threshold for leaf node split */
  int leaf_node_size_upper_threshold_ = LEAF_NODE_SIZE_UPPER_THRESHOLD;
  /** lower size threshold for leaf node removal */
  int leaf_node_size_lower_threshold_ = LEAF_NODE_SIZE_LOWER_THRESHOLD;

 public:
  /**
   * Constructor
   */
  BPlusTreeBase() = default;

  /**
   * Destructor
   */
  ~BPlusTreeBase() = default;
};

/**
 * Implementation of a B+ Tree index using latch crabbing.
 *
 * Concurrency:
 *  Each node (leaf and inner) contains one shared latch to gain access to the node.
 *  Writers are prioritized in this lock. Additionally, to guarantee safety of the root pointer of the
 *  tree there is an additional latch for the root (root has 2 latches now). The root pointer requires the
 *  additional latch because the root note itself can be modified and changed, and at this point, other
 *  threads should not be allowed to enter/modify the tree. This latch can be thought of as a global
 *  latch protecting the entire B+ Tree data structure. Latches are acquired from
 *  top to bottom. Thus, before accessing a child node pointer, it is ensured that the pointer cannot be
 *  deleted no matter what (the parent latch is also being held at that point).
 *
 * Optimistic latch crabbing is used to acquire node latches for reads and writes.
 *  Read:
 *    Acquire latches starting from root, in the path to find the key. Release shared latch of parent
 *    once the latch on current node is obtained and so on...
 *
 *  Write:
 *    Happens in 2 phases:
 *    1) Shared latches acquired throughout the path expect in leaf node (exclusive lock needed here). If the leaf node
 *    is safe for insertion/deletion (no overflow/underflow), perform the operation else move to step 2.
 *
 *    2) Exclusive latches are acquired from root to the corresponding leaf. A queue of locks acquired is maintained.
 *    If the current node is safe (no overflow/underflow), release all parent locks are released.
 *
 * @tparam KeyType Key type of the map
 * @tparam ValueType Value type of the map. Note that it is possible that a single key is mapped to multiple values
 * @tparam KeyComparator "less than" relation comparator for KeyType
 *                   Returns true if "less than" relation holds
 * @tparam KeyEqualityChecker Equality checker for KeyType
 *                        Returns true if two keys are equal
 * @tparam ValueEqualityChecker Equality checker for value type
 *                          Returns true for ValueTypes that are equal
 */
template <typename KeyType, typename ValueType, typename KeyComparator = std::less<KeyType>,
          typename KeyEqualityChecker = std::equal_to<KeyType>,
          typename ValueEqualityChecker = std::equal_to<ValueType>>
class BPlusTree : public BPlusTreeBase {
 public:
  class BaseNode;

  /** <KeyType, BaseNode *> pair - represents an element in the inner node */
  using KeyNodePointerPair = std::pair<KeyType, BaseNode *>;
  /** <KeyType, List of ValueType> pair - represents an element in the leaf node */
  using KeyValuePair = std::pair<KeyType, std::list<ValueType> *>;
  /** <KeyType, ValueType> pair - used for inserts and deletes which operates using a key-value pair */
  using KeyElementPair = std::pair<KeyType, ValueType>;

  /**
   * enum class NodeType - B+ Tree node type
   */
  enum class NodeType : int { InnerType = 0, LeafType = 1 };

  ///////////////////////////////////////////////////////////////////
  // Key Comparison Member Functions
  ///////////////////////////////////////////////////////////////////

  /**
   * KeyCmpLess() - Compare two keys for "less than" relation
   *
   * If key1 < key2 return true
   * If not return false
   */
  bool KeyCmpLess(const KeyType &key1, const KeyType &key2) const { return key_cmp_obj_(key1, key2); }

  /**
   * KeyCmpEqual() - Compare a pair of keys for equality
   *
   * This functions compares keys for equality relation
   */
  bool KeyCmpEqual(const KeyType &key1, const KeyType &key2) const { return key_eq_obj_(key1, key2); }

  /**
   * KeyCmpGreaterEqual() - Compare a pair of keys for >= relation
   *
   * It negates result of keyCmpLess()
   */
  bool KeyCmpGreaterEqual(const KeyType &key1, const KeyType &key2) const { return !KeyCmpLess(key1, key2); }

  /**
   * KeyCmpGreater() - Compare a pair of keys for > relation
   *
   * It flips input for keyCmpLess()
   */
  bool KeyCmpGreater(const KeyType &key1, const KeyType &key2) const { return KeyCmpLess(key2, key1); }

  /**
   * KeyCmpLessEqual() - Compare a pair of keys for <= relation
   */
  bool KeyCmpLessEqual(const KeyType &key1, const KeyType &key2) const { return !KeyCmpGreater(key1, key2); }

  ///////////////////////////////////////////////////////////////////
  // Value Comparison Member
  ///////////////////////////////////////////////////////////////////

  /**
   * ValueCmpEqual() - Compares whether two values are equal
   */
  bool ValueCmpEqual(const ValueType &v1, const ValueType &v2) { return value_eq_obj_(v1, v2); }

  /**
   * class NodeMetaData - Holds node metadata in an object
   *
   * NOTE: We do not count node type as node metadata
   */
  class NodeMetaData {
   public:
    /** Low and High keys in a leaf or inner node. These are explicitly stored so
     * that the iterator begin and end can fetch the values easily.
     */
    const KeyNodePointerPair *low_key_p_;  /*!< Low Key */
    const KeyNodePointerPair *high_key_p_; /*!< High Key */

    /** The type of the node */
    NodeType type_;

    /** This is the depth of the node within the tree */
    int depth_;

    /** This counts the total number of items in the node */
    int item_count_;

    /** Latch for each node */
    common::SharedLatch node_latch_;

    /**
     * Constructor
     */
    NodeMetaData(const KeyNodePointerPair *p_low_key_p, const KeyNodePointerPair *p_high_key_p, NodeType p_type,
                 int p_depth, int p_item_count)
        : low_key_p_{p_low_key_p},
          high_key_p_{p_high_key_p},
          type_{p_type},
          depth_{p_depth},
          item_count_{p_item_count} {}
  };

  /**
   * class BaseNode - Generic node class; inherited by leaf, inner
   *                  and delta node
   */
  class BaseNode {
    // We hold its data structure as private to force using member functions
    // for member access
   private:
    // This holds low key, high key, type, depth and item count
    NodeMetaData metadata_;

   public:
    /**
     * Constructor - Initialize type and metadata
     */
    BaseNode(NodeType p_type, const KeyNodePointerPair *p_low_key_p, const KeyNodePointerPair *p_high_key_p,
             int p_depth, int p_item_count)
        : metadata_{p_low_key_p, p_high_key_p, p_type, p_depth, p_item_count} {}

    /**
     * GetType() - Return the type of node
     *
     */
    NodeType GetType() const { return metadata_.type_; }

    /**
     * GetNodeMetaData() - Returns a const reference to node metadata
     *
     */
    const NodeMetaData &GetNodeMetaData() const { return metadata_; }

    /**
     * IsInnerNode() - Returns true if the node is an inner node
     *
     */
    bool IsInnerNode() const { return GetType() == NodeType::InnerType; }

    /**
     * GetLowKey() - Returns the low key of the current base node
     *
     * NOTE: Since it is defined that for LeafNode the low key is undefined
     * and pointers should be set to nullptr, accessing the low key of
     * a leaf node would result in Segmentation Fault
     */
    const KeyType &GetLowKey() const { return metadata_.low_key_p->first; }

    /**
     * GetHighKey() - Returns a reference to the high key of current node
     *
     * This function could be called for all node types including leaf nodes
     * and inner nodes.
     */
    const KeyType &GetHighKey() const { return metadata_.high_key_p_->first; }

    /**
     * GetHighKeyPair() - Returns the pointer to high key node ptr pair
     */
    const KeyNodePointerPair &GetHighKeyPair() const { return *metadata_.high_key_p_; }

    /**
     * GetLowKeyPair() - Returns the pointer to low key node ptr pair
     */
    const KeyNodePointerPair &GetLowKeyPair() const { return *metadata_.low_key_p_; }

    /**
     * GetDepth() - Returns the Height of the Node
     */
    int GetDepth() const { return metadata_.depth_; }

    /**
     * GetItemCount() - Returns the item count of the current node
     */
    int GetItemCount() const { return metadata_.item_count_; }

    /**
     * GetNodeExclusiveLatch() - Obtain the exclusive lock to the current node
     */
    void GetNodeExclusiveLatch() { metadata_.node_latch_.LockExclusive(); }

    /**
     * GetNodeSharedLatch() - Obtain the Shared lock to the current node
     */
    void GetNodeSharedLatch() { metadata_.node_latch_.LockShared(); }

    /**
     * ReleaseNodeLatch() - Release the lock to the current node
     */
    void ReleaseNodeLatch() { metadata_.node_latch_.UnlockExclusive(); }

    /**
     * ReleaseNodeSharedLatch() - Release shared access to the current node
     */
    void ReleaseNodeSharedLatch() { metadata_.node_latch_.UnlockShared(); }

    /**
     * GetLatchPointer() - Get the Latch Pointer of current node's latch
     */
    common::SharedLatch *GetLatchPointer() { return &(metadata_.node_latch_); }

    /**
     * TryExclusiveLock() - Try to get the exclusive lock
     */
    bool TryExclusiveLock() { return metadata_.node_latch_.TryExclusiveLock(); }

    /**
     * TrySharedLock() - Try to get the shared read lock
     */
    bool TrySharedLock() { return metadata_.node_latch_.TryLockShared(); }

    /**
     * SetLowKeyPair() - Sets the low key pair of metadata
     */
    void SetLowKeyPair(const KeyNodePointerPair *p_low_key_p) { metadata_.low_key_p_ = p_low_key_p; }

    /**
     * SetHighKeyPair() - Sets the high key pair of metdtata
     */
    void SetHighKeyPair(const KeyNodePointerPair *p_high_key_p) { metadata_.high_key_p_ = p_high_key_p; }

    /**
     * SetType() - Sets the type of metadata
     */
    void SetType(const NodeType input_type) { metadata_.type_ = input_type; }

    /**
     * SetDepth() - Sets the depth of metadata
     */
    void SetDepth(const int input_depth) { metadata_.depth_ = input_depth; }

    /**
     * SetItemCount() - Sets the item_count of metadata
     */
    void SetItemCount(const int input_item_count) { metadata_.item_count_ = input_item_count; }
  };

  /**
   * class ElasticNode - The base class for elastic node types, i.e. InnerNode
   *                     and LeafNode
   */
  template <typename ElementType>
  class ElasticNode : public BaseNode {
   private:
    // These two are the low key and high key of the node respectively
    // since we could not add it in the inherited class (will clash with
    // the array which is invisible to the compiler) so they must be added here
    KeyNodePointerPair low_key_;
    KeyNodePointerPair high_key_;

    // This is the end of the elastic array
    // We explicitly store it here to avoid calculating the end of the array
    // everytime
    ElementType *end_;

    // This is the starting point
    ElementType start_[0];

   public:
    /**
     * Constructor
     *
     * Note that this constructor uses the low key and high key stored as
     * members to initialize the NodeMetadata object in class BaseNode
     */
    ElasticNode(NodeType p_type, int p_depth, int p_item_count, const KeyNodePointerPair *p_low_key,
                const KeyNodePointerPair *p_high_key)
        : BaseNode{p_type, &low_key_, &high_key_, p_depth, p_item_count},
          low_key_{*p_low_key},
          high_key_{*p_high_key},
          end_{start_} {}

    /**
     * Copy() - Copy constructs another instance
     */
    static ElasticNode *Copy(const ElasticNode &other) {
      ElasticNode *node_p = ElasticNode::Get(other.GetItemCount(), other.GetType(), other.GetDepth(),
                                             other.GetItemCount(), other.GetLowKeyPair(), other.GetHighKeyPair());

      node_p->PushBack(other.Begin(), other.End());

      return node_p;
    }

    /**
     * Free elastic node
     */
    void FreeElasticNode() {
      ElasticNode *allocation_start = this;
      delete[] reinterpret_cast<char *>(allocation_start);
    }

    /**
     * SetEnd() - Make end = start + offset
     */
    void SetEnd(int offset) { end_ = start_ + offset; }

    /**
     * Begin() - Returns a begin iterator to its internal array
     */
    ElementType *Begin() { return start_; }

    /**
     * Const function for the Begin() function above
     */
    const ElementType *Begin() const { return start_; }

    /**
     * RBegin() - Returns a reverse begin iterator to its internal array
     */
    ElementType *RBegin() {
      auto return_value = (end_ - 1);
      if (this->GetSize() == 0) {
        return_value = nullptr;
      }
      return return_value;
    }

    /**
     * End() - Returns an end iterator that is similar to the one for vector
     */
    ElementType *End() { return end_; }

    /**
     * Const function for the End() function above
     */
    const ElementType *End() const { return end_; }

    /**
     * REnd() - Returns the element before the first element
     *
     * Note that since we returned an invalid pointer into the array, the
     * return value should not be modified and is therefore of const type
     */
    const ElementType *REnd() { return start_ - 1; }

    /**
     * Const function for the REnd() function above
     */
    const ElementType *REnd() const { return start_ - 1; }

    /**
     * PushBack() - Push back an element
     *
     * This function takes an element type and copy-construct it on the array
     * which is invisible to the compiler. Therefore we must call placement
     * operator new to do the job
     */
    void PushBack(const ElementType &element) {
      // Placement new + copy constructor using end pointer
      new (end_) ElementType{element};

      // Move it pointing to the next available slot, if not reached the end
      end_++;
    }

    /**
     * PushBack() - Push back a series of elements
     *
     * The overloaded PushBack() could also push an array of elements
     */
    void PushBack(const ElementType *copy_start_p, const ElementType *copy_end_p) {
      // Make sure the loop will come to an end
      NOISEPAGE_ASSERT(copy_start_p <= copy_end_p, "Loop will not come to an end.");

      while (copy_start_p != copy_end_p) {
        PushBack(*copy_start_p);
        copy_start_p++;
      }
    }

   public:
    /**
     * InsertElementIfPossible - Returns true if inserted and false if node full
     * Inserts at location provided.
     */
    bool InsertElementIfPossible(const ElementType &element, ElementType *location) {
      if (this->GetSize() >= this->GetItemCount()) return false;
      if (end_ - location > 0)
        std::memmove(reinterpret_cast<void *>(location + 1), reinterpret_cast<void *>(location),
                     (end_ - location) * sizeof(ElementType));
      new (location) ElementType{element};
      end_ = end_ + 1;
      return true;
    }

    /**
     * SplitNode - Splits the current node and returns a pointer to the new node.
     * Returns nullptr if node is not already full
     */
    ElasticNode *SplitNode() {
      if (this->GetSize() < this->GetItemCount()) return nullptr;
      ElasticNode *new_node = this->Get(this->GetSize(), this->GetType(), this->GetDepth(), this->GetItemCount(),
                                        *this->GetElasticLowKeyPair(), *this->GetElasticHighKeyPair());
      ElementType *copy_from_location = Begin() + ((this->GetSize()) / 2);

      std::memcpy(reinterpret_cast<void *>(new_node->Begin()), reinterpret_cast<void *>(copy_from_location),
                  (end_ - copy_from_location) * sizeof(ElementType));
      new_node->SetEnd((end_ - copy_from_location));
      end_ = copy_from_location;
      return new_node;
    }

    /**
     * MergeNode - Merge a given node's entries to the current node.
     * Returns true if merge successful, false otherwise
     */
    bool MergeNode(ElasticNode *next_node) {
      // Merge the right type
      if (this->GetType() != next_node->GetType()) {
        return false;
      }

      // Is Merging possible
      if ((this->GetItemCount() - this->GetSize()) < next_node->GetSize()) {
        return false;
      }

      std::memmove(reinterpret_cast<void *>(this->End()), reinterpret_cast<void *>(next_node->Begin()),
                   (next_node->GetSize()) * sizeof(ElementType));
      SetEnd(this->GetSize() + next_node->GetSize());
      return true;
    }

    /**
     * Pop Begin - Pops the first element from the list
     * Returns False if empty
     */
    bool PopBegin() {
      if (this->GetSize() == 0) return false;
      if (this->GetSize() == 1) {
        SetEnd(0);
        return true;
      }
      std::memmove(reinterpret_cast<void *>(start_), reinterpret_cast<void *>(start_ + 1),
                   (this->GetSize() - 1) * sizeof(ElementType));
      SetEnd(this->GetSize() - 1);
      return true;
    }

    /**
     * Pop Back - Pops the last element from the list
     * Returns False if empty
     */
    bool PopEnd() {
      if (this->GetSize() == 0) return false;
      if (this->GetSize() == 1) {
        SetEnd(0);
        return true;
      }
      SetEnd(this->GetSize() - 1);
      return true;
    }

    /**
     * Erase - remove the element at index 'i' from the list
     * Returns False if empty
     */
    bool Erase(int i) {
      if (this->GetSize() <= i) return false;

      if (this->GetSize() == 1) {
        SetEnd(0);
        return true;
      }
      std::memmove(reinterpret_cast<void *>(start_ + i), reinterpret_cast<void *>(start_ + i + 1),
                   (this->GetSize() - i - 1) * sizeof(ElementType));
      SetEnd(this->GetSize() - 1);
      return true;
    }

    /**
     * GetSize() - Returns the size of the embedded list
     *
     * Note that the return type is integer since we use integer to represent
     * the size of a node
     */
    int GetSize() const { return static_cast<int>(End() - Begin()); }

    /**
     * SetElasticLowKeyPair() - Sets the low key of Elastic Node
     */
    void SetElasticLowKeyPair(const KeyNodePointerPair &p_low_key) { low_key_ = p_low_key; }

    /**
     * SetElasticHighKeyPair() - Sets the high key of Elastic Node
     */
    void SetElasticHighKeyPair(const KeyNodePointerPair &p_high_key) { high_key_ = p_high_key; }

    /**
     * GetElasticLowKeyPair() - Returns a pointer to the low key current Elastic node
     */
    KeyNodePointerPair *GetElasticLowKeyPair() { return &low_key_; }

    /**
     * GetElasticHighKeyPair() - Returns a pointer to the high key of current Elastic node
     */
    KeyNodePointerPair *GetElasticHighKeyPair() { return &high_key_; }

    /**
     * Get() - Static helper function that constructs a elastic node of
     *         a certain size
     */
    static ElasticNode *Get(int size,  // Number of elements
                            NodeType p_type, int p_depth,
                            int p_item_count,  // Usually equal to size
                            const KeyNodePointerPair &p_low_key, const KeyNodePointerPair &p_high_key) {
      // Allocate space for a new elastic node with size number of elements
      auto *alloc_base = new char[sizeof(ElasticNode) + size * sizeof(ElementType)];

      auto elastic_node = reinterpret_cast<ElasticNode *>(alloc_base);
      new (elastic_node) ElasticNode{p_type, p_depth, p_item_count, &p_low_key, &p_high_key};

      return elastic_node;
    }

    /**
     * Function that returns the element at location pointed to by index
     * @param index Index of the element being looked up
     * @return Element at index
     */
    ElementType &At(const int index) {
      // The index must be inside the valid range
      NOISEPAGE_ASSERT(index < GetSize(), "Index out of range.");

      return *(Begin() + index);
    }

    /**
     * At function that returns a const element
     */
    const ElementType &At(const int index) const {
      // The index must be inside the valid range
      NOISEPAGE_ASSERT(index < GetSize(), "Index out of range.");

      return *(Begin() + index);
    }
  };

  /**
   * class InnerNode - Inner node that holds pointers to the lower levels
   */
  class InnerNode : public ElasticNode<KeyNodePointerPair> {
   public:
    // Construction of InnerNode should be through the ElasticNode interface
    InnerNode() = delete;
    InnerNode(const InnerNode &) = delete;
    InnerNode(InnerNode &&) = delete;
    InnerNode &operator=(const InnerNode &) = delete;
    InnerNode &operator=(InnerNode &&) = delete;

    /**
     * Destructor - Calls destructor of ElasticNode
     */
    ~InnerNode() { this->~ElasticNode<KeyNodePointerPair>(); }

    /**
     * FindLocation - Returns the start of the first element that compares
     * greater to the key provided
     */
    KeyNodePointerPair *FindLocation(const KeyType &element, BPlusTree *tree) {
      BaseNode *dummy_ptr = nullptr;
      KeyNodePointerPair dummy_element = std::make_pair(element, dummy_ptr);
      KeyNodePointerPair *iter =
          std::lower_bound(this->Begin(), this->End(), dummy_element,
                           [tree](const auto &a, const auto &b) { return !tree->KeyCmpGreater(a.first, b.first); });
      return iter;
    }
  };

  /**
   * class LeafNode - Leaf node that holds data
   *
   * LeafNode contains the values. The values are stored as a list.
   */
  class LeafNode : public ElasticNode<KeyValuePair> {
   public:
    // Construction of LeafNode should happen through ElasticNode function
    LeafNode() = delete;
    LeafNode(const LeafNode &) = delete;
    LeafNode(LeafNode &&) = delete;
    LeafNode &operator=(const LeafNode &) = delete;
    LeafNode &operator=(LeafNode &&) = delete;

    /**
     * Destructor - Calls underlying ElasticNode destructor
     */
    ~LeafNode() { this->~ElasticNode<KeyValuePair>(); }

    /**
     * FindLocation - Returns the start of the first element that compares
     * greater to the key provided
     */
    KeyValuePair *FindLocation(const KeyType &element, BPlusTree *tree) {
      std::list<ValueType> *dummy_ptr = nullptr;
      KeyValuePair dummy_element = std::make_pair(element, dummy_ptr);
      KeyValuePair *iter =
          std::lower_bound(this->Begin(), this->End(), dummy_element,
                           [tree](const auto &a, const auto &b) { return !tree->KeyCmpGreater(a.first, b.first); });
      return iter;
    }
  };

  /** Key comparator */
  const KeyComparator key_cmp_obj_;

  /** Raw key eq checker */
  const KeyEqualityChecker key_eq_obj_;

  /** Check whether values are equivalent */
  const ValueEqualityChecker value_eq_obj_;

 private:
  BaseNode *root_;
  common::SharedLatch root_latch_;
  std::atomic_uint64_t num_keys_;
  std::atomic_uint64_t num_values_;

 public:
  /**
   * Get Root - Returns the current root
   */
  BaseNode *GetRoot() { return root_; }

  /**
   * Get Element Returns a constructed KeyElementPair
   */
  KeyElementPair GetElement(KeyType key, ValueType value) {
    KeyElementPair p1;
    p1.first = key;
    p1.second = value;
    return p1;
  }

  /**
   * Tries to find key by Traversing down the B+ Tree
   * Returns true if found
   * Returns false if not found
   */
  bool IsPresent(KeyType key) {
    if (root_ == nullptr) {
      return false;
    }

    BaseNode *current_node = root_;

    // Traversing Down to the right leaf node
    while (current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = static_cast<InnerNode *>(node)->FindLocation(key, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      if (index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      } else {
        current_node = node->GetLowKeyPair().second;
      }
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    for (KeyValuePair *element_p = node->Begin(); element_p != node->End(); element_p++) {
      if (KeyCmpEqual(element_p->first, key)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Tries to find key by Traversing down the BplusTree
   * Returns the list of values of the key from leaf if found in result vector
   * Returns null if not found
   */
  void FindValueOfKey(KeyType key, std::vector<ValueType> *result) {
    root_latch_.LockShared();

    if (root_ == nullptr) {
      root_latch_.UnlockShared();
      return;
    }

    BaseNode *current_node = root_;
    BaseNode *parent = nullptr;

    // Get the shared latch of next node, release the root_latch
    current_node->GetNodeSharedLatch();
    root_latch_.UnlockShared();

    // Traversing Down to the right leaf node
    while (current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = static_cast<InnerNode *>(node)->FindLocation(key, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      parent = current_node;
      if (index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      } else {
        current_node = node->GetLowKeyPair().second;
      }

      // Get the shared latch for next node and release the parent
      current_node->GetNodeSharedLatch();
      parent->ReleaseNodeSharedLatch();
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    for (KeyValuePair *element_p = node->Begin(); element_p != node->End(); element_p++) {
      if (KeyCmpEqual(element_p->first, key)) {
        auto itr_list = element_p->second->begin();
        while (itr_list != element_p->second->end()) {
          (*result).push_back(*itr_list);
          itr_list++;
        }

        // Release the Leaf shared latch
        current_node->ReleaseNodeSharedLatch();
        return;
      }
    }

    // Release the Leaf shared latch
    current_node->ReleaseNodeSharedLatch();
  }

  /**
   * Traverses Down the root in a BFS manner and frees all the nodes. Used in
   * the B+ Tree destructor.
   * NOTE: This function does not acquire any latches, should be called only when safe
   * (normally it should be safe since the Tree is being deleted)
   */
  void FreeTree() {
    if (root_ == nullptr) return;
    std::queue<BaseNode *> bfs_queue;
    std::queue<BaseNode *> all_nodes;
    bfs_queue.push(root_);

    while (!bfs_queue.empty()) {
      BaseNode *node = bfs_queue.front();
      bfs_queue.pop();
      all_nodes.push(node);
      if (node->GetType() != NodeType::LeafType) {
        bfs_queue.push(node->GetLowKeyPair().second);
        auto current_node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
        for (KeyNodePointerPair *element_p = current_node->Begin(); element_p != current_node->End(); element_p++) {
          bfs_queue.push(element_p->second);
        }
      }
    }

    while (!all_nodes.empty()) {
      BaseNode *node = all_nodes.front();
      all_nodes.pop();
      if (node->GetType() != NodeType::LeafType) {
        auto current_node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
        current_node->FreeElasticNode();
      } else {
        auto current_node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node);
        for (KeyValuePair *element_p = current_node->Begin(); element_p != current_node->End(); element_p++) {
          // destroy the list of values in a key
          delete element_p->second;
        }
        current_node->FreeElasticNode();
      }
    }
  }

  /**
   * StructuralIntegrityVerification - Recursively Tests the structural integrity of the data
   * structure. Verifies whether all the keys in the set are present in the tree. Verifies
   * all keys lie between the given range(This gets pruned as move down the tree). Also verifies
   * the size of each node.
   * Also erases all keys that are found in the tree from the input set
   *
   * NOTE: This function does not acquire any latches, used by tests
   */
  bool StructuralIntegrityVerification(KeyType low_key, KeyType high_key, std::set<KeyType> *keys,
                                       BaseNode *current_node) {
    bool return_answer = true;
    if (current_node->GetType() == NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
      for (KeyValuePair *element_p = node->Begin(); element_p != node->End(); element_p++) {
        /* All Keys within Range*/
        if (this->KeyCmpGreater(low_key, element_p->first) || this->KeyCmpGreater(element_p->first, high_key)) {
          return false;
        }
        /* Size of Node is correct */
        if (current_node != root_) {
          if (node->GetSize() < leaf_node_size_lower_threshold_ || node->GetSize() > leaf_node_size_upper_threshold_) {
            return false;
          }
        }
        (*keys).erase(element_p->first);
      }
    } else {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      /* Size of Node is correct */
      if (current_node != root_) {
        if (node->GetSize() < inner_node_size_lower_threshold_ || node->GetSize() > inner_node_size_upper_threshold_) {
          return false;
        }
      }
      return_answer &= this->StructuralIntegrityVerification(low_key, node->Begin()->first, keys,
                                                             current_node->GetLowKeyPair().second);

      KeyNodePointerPair *iter = node->Begin();
      while ((iter + 1) != node->End()) {
        low_key = iter->first;
        return_answer &= this->StructuralIntegrityVerification(low_key, (iter + 1)->first, keys, iter->second);
        iter++;
      }

      return_answer &= this->StructuralIntegrityVerification(iter->first, high_key, keys, iter->second);
    }
    return return_answer;
  }

  /**
   * SiblingForwardCheck - Expects a sorted set of the keys and then finds the
   * the least element and the leaf it is stored in. Traverses the right sibling
   * of the leaf to cover all the elements in the B+ tree and checks with the
   * sorted key set for their order. Verifies if a full scan (ascending) using the siblings
   * is correct or not.
   *
   * NOTE: This function does not acquire any latches, used by tests
   */
  bool SiblingForwardCheck(std::set<KeyType> *keys) {
    KeyType key = *(*keys).begin();
    if (root_ == nullptr) {
      return false;
    }

    BaseNode *current_node = root_;

    // Traversing Down to the right leaf node
    while (current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = static_cast<InnerNode *>(node)->FindLocation(key, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      if (index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      } else {
        current_node = node->GetLowKeyPair().second;
      }
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    auto itr = (*keys).begin();
    while (node != nullptr) {
      // iterate over all the values in the leaf node checking with the key set
      for (KeyValuePair *element_p = node->Begin(); element_p != node->End(); element_p++) {
        if (element_p->first != *itr) {
          return false;
        }
        itr++;
      }
      // current node checked, move to the next one
      node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetHighKeyPair().second);
    }
    return true;
  }

  /**
   * SiblingBackwardCheck - Expects a sorted set of the keys and then finds the
   * the highest element and the leaf it is stored in. Traverses the left sibling
   * of the leaf to cover all the elements in the b+ tree and checks with the
   * sorted key set for their order. Verifies if a full scan (descending) using the siblings
   * is correct or not.
   *
   * NOTE: This function does not acquire any latches, used by tests
   */
  bool SiblingBackwardCheck(std::set<KeyType> *keys) {
    KeyType key = *(*keys).rbegin();
    if (root_ == nullptr) {
      return false;
    }

    BaseNode *current_node = root_;

    // Traversing Down to the right leaf node
    while (current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = static_cast<InnerNode *>(node)->FindLocation(key, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      if (index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      } else {
        current_node = node->GetLowKeyPair().second;
      }
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    auto itr = (*keys).rbegin();
    while (node != nullptr) {
      // iterate over all the values in the leaf node checking with the key set
      for (KeyValuePair *element_p = node->End() - 1; element_p != node->REnd(); element_p--) {
        if (element_p->first != *itr) {
          return false;
        }
        itr++;
      }
      // current node checked, move to the next one
      node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetLowKeyPair().second);
    }
    return true;
  }

  /**
   * This test checks if duplicate keys are correctly handled by the insert wrapper.
   * For a B+ tree, with each key having multiple values, stored in a map keys_values
   * check if the values_list in the tree have the same values as the map given
   * in the function.
   *
   * NOTE: This function does not acquire any latches, used by tests
   */
  bool DuplicateKeyValuesCheck(std::unordered_map<KeyType, std::set<ValueType>> *keys_values) {
    for (auto itr = (*keys_values).begin(); itr != (*keys_values).end(); itr++) {
      KeyType k = itr->first;
      std::set<ValueType> values = (*keys_values)[k];
      std::vector<ValueType> result;
      FindValueOfKey(k, &result);
      if (result.empty()) {
        return false;
      }
      bool return_value = true;
      for (auto it = result.begin(); it != result.end(); it++) {
        if (!(return_value)) {
          break;
        }
        if (values.count(*(it)) == 0) {
          return_value = false;
        } else {
          values.erase(*(it));
        }
      }
      if (!(return_value)) {
        return return_value;
      }
      if (!values.empty()) {
        return false;
      }
    }
    return true;
  }

  /**
   * The check gets a map of keys to vector of values that are whetted and
   * removes any duplicates that might have been tried to be added by a
   * transaction in insert unique. The check ensures that the values list
   * in the B+ tree also have the same keys and the same order.
   *
   * NOTE: This function does not acquire any latches, used by tests
   */
  bool DuplicateKeyValueUniqueInsertCheck(std::unordered_map<KeyType, std::vector<ValueType>> *keys_values) {
    for (auto itr = (*keys_values).begin(); itr != (*keys_values).end(); itr++) {
      KeyType k = itr->first;
      std::vector<ValueType> values = (*keys_values)[k];
      std::list<ValueType> *values_list_p = FindValueOfKey(k);
      if (values_list_p == nullptr) {
        return false;
      }
      auto it_list = values_list_p->begin();
      for (unsigned j = 0; j != values.size(); j++) {
        if (it_list == values_list_p->end()) {
          return false;
        }
        if (values[j] != *(it_list)) {
          return false;
        }
        it_list++;
      }
    }
    return true;
  }

  /**
   * Returns current heap usage (tries to return the exact heap usage by traversing
   * the tree).
   *
   * NOTE: This function acquires a coarse lock, on the root, and prevents access to
   * any other thread. Not ideal behavior, but this function is not commonly used. To
   * get an approximate heap usage, EstimateHeapUsage() should be used.
   */
  size_t GetHeapUsage() {
    root_latch_.LockExclusive();
    if (root_ == nullptr) {
      root_latch_.UnlockExclusive();
      return 0;
    }

    std::queue<BaseNode *> bfs_queue;
    std::queue<BaseNode *> all_nodes;
    bfs_queue.push(root_);

    while (!bfs_queue.empty()) {
      BaseNode *node = bfs_queue.front();
      bfs_queue.pop();
      all_nodes.push(node);
      if (node->GetType() != NodeType::LeafType) {
        bfs_queue.push(node->GetLowKeyPair().second);
        auto current_node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
        for (KeyNodePointerPair *element_p = current_node->Begin(); element_p != current_node->End(); element_p++) {
          bfs_queue.push(element_p->second);
        }
      }
    }

    size_t heap_size = 0;
    while (!all_nodes.empty()) {
      BaseNode *node = all_nodes.front();
      all_nodes.pop();
      if (node->GetType() != NodeType::LeafType) {
        auto current_node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node);
        heap_size += sizeof(BaseNode) + current_node->GetSize() * sizeof(KeyNodePointerPair);
      } else {
        auto current_node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node);
        heap_size += sizeof(BaseNode) + current_node->GetSize() * sizeof(KeyValuePair);
        for (KeyValuePair *element_p = current_node->Begin(); element_p != current_node->End(); element_p++) {
          if (element_p->second != nullptr) {
            // Add the size of values to be counted as heap usage
            heap_size += element_p->second->size() * sizeof(ValueType);
          }
        }
      }
    }

    // Unlock the tree
    root_latch_.UnlockExclusive();
    return heap_size;
  }

  /**
   * Scan Ascending - Scans keys starting at low key and moves till high key or limit, and populates
   * the value_list vector with the values found, if they are visible to the transaction.
   *
   * @param index_low_key Key to start at
   * @param index_high_key Key to end at
   * @param low_key_exists Whether low_key exists in the scan operation
   * @param num_attrs Number of attributes to compare
   * @param high_key_exists Whether high_key exists in the scan operation
   * @param limit Upper bound for the number of elements
   * @param value_list List of values scanned
   * @param metadata Index metadata
   * @param predicate Predicate to be satisfied to add a value to the result
   */
  bool ScanAscending(KeyType index_low_key, KeyType index_high_key, bool low_key_exists, uint32_t num_attrs,
                     bool high_key_exists, uint32_t limit, std::vector<TupleSlot> *value_list,
                     const IndexMetadata *metadata, std::function<bool(const ValueType)> predicate) {
    root_latch_.LockShared();

    if (root_ == nullptr) {
      root_latch_.UnlockShared();
      return true;
    }

    BaseNode *current_node = root_;
    BaseNode *parent = nullptr;

    // Acquire latch on the root, release root_latch
    current_node->GetNodeSharedLatch();
    root_latch_.UnlockShared();

    // Traversing Down to the correct leaf node
    while (current_node->GetType() != NodeType::LeafType) {
      // Set parent for releasing the lock
      parent = current_node;

      if (low_key_exists) {
        auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
        // Note that Find Location returns the location of first element
        // that compare greater than
        auto index_pointer = static_cast<InnerNode *>(node)->FindLocation(index_low_key, this);
        // Thus we have to go in the left side of location which will be the
        // pointer of the previous location.

        if (index_pointer != node->Begin()) {
          index_pointer -= 1;
          current_node = index_pointer->second;
        } else {
          current_node = node->GetLowKeyPair().second;
        }
      } else {
        current_node = current_node->GetLowKeyPair().second;
      }

      // Get shared latch on current node, and release the parent.
      current_node->GetNodeSharedLatch();
      parent->ReleaseNodeSharedLatch();
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    KeyValuePair *element_p;
    if (low_key_exists) {
      element_p = static_cast<LeafNode *>(node)->FindLocation(index_low_key, this);
      if (element_p != node->Begin()) {
        if (KeyCmpEqual((element_p - 1)->first, index_low_key)) {
          element_p--;
        }
        if (element_p == node->End()) {
          if (node->GetHighKeyPair().second == nullptr) {
            current_node->ReleaseNodeSharedLatch();
            return true;
          }
          parent = node;
          node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetHighKeyPair().second);
          current_node = node;

          // Get shared latch on current node, and release the parent (here, parent is the previous
          // node while iterating at the leaf level).
          if (!(current_node->TrySharedLock())) {
            parent->ReleaseNodeSharedLatch();
            return false;
          }
          parent->ReleaseNodeSharedLatch();

          element_p = node->Begin();
        }
      }
    } else {
      element_p = node->Begin();
    }

    while ((limit == 0 || value_list->size() < limit) &&
           (!high_key_exists || element_p->first.PartialLessThan(index_high_key, metadata, num_attrs))) {
      auto itr_list = element_p->second->begin();
      while (itr_list != element_p->second->end()) {
        if (!predicate(*itr_list)) {
          itr_list++;
          continue;
        }
        value_list->push_back(*itr_list);
        if (!(limit == 0 || value_list->size() < limit)) break;
        itr_list++;
      }

      element_p++;
      if (element_p == node->End()) {
        if (node->GetHighKeyPair().second == nullptr) break;
        parent = node;
        node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetHighKeyPair().second);
        current_node = node;

        // Get shared latch on current node, and release the parent (here, parent is the previous
        // node while iterating at the leaf level).
        if (!(current_node->TrySharedLock())) {
          parent->ReleaseNodeSharedLatch();
          return false;
        }
        parent->ReleaseNodeSharedLatch();

        element_p = node->Begin();
      }
    }

    // Release the latch on the leaf node
    current_node->ReleaseNodeSharedLatch();
    return true;
  }

  /**
   * Scan Descending - Scan keys starting from high key and moves till low key, and populates a vector
   * with the values found, if they are visible to the transaction. Since there is a possibility of
   * deadlock with a ScanAscending and concurrent delete/insert, if shared latch on a node is not
   * available, the entire operation is forfeited, and false is returned. The caller is expected to
   * retry the operation till it succeeds, returning true.
   *
   * @param index_low_key Key to end at
   * @param index_high_key Key to start at
   * @param value_list List to be populated with results
   * @return true on success, false on failure
   */
  bool ScanDescending(KeyType index_low_key, KeyType index_high_key, std::vector<TupleSlot> *value_list) {
    root_latch_.LockShared();

    if (root_ == nullptr) {
      root_latch_.UnlockShared();
      return true;
    }

    BaseNode *current_node = root_;
    BaseNode *parent = nullptr;

    // Unlock root latch and get shared latch on root node
    current_node->GetNodeSharedLatch();
    root_latch_.UnlockShared();

    // Traversing Down to the right leaf node
    while (current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = static_cast<InnerNode *>(node)->FindLocation(index_high_key, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.

      // set parent for crabbing
      parent = node;

      if (index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      } else {
        current_node = node->GetLowKeyPair().second;
      }

      // Unlock parent and get shared latch on current node
      current_node->GetNodeSharedLatch();
      parent->ReleaseNodeSharedLatch();
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    KeyValuePair *element_p;
    element_p = static_cast<LeafNode *>(node)->FindLocation(index_high_key, this);

    bool reach_end = false;
    if (element_p != node->Begin()) {
      element_p--;
    } else {
      if (node->GetLowKeyPair().second == nullptr) {
        current_node->ReleaseNodeSharedLatch();
        reach_end = true;
      } else {
        parent = node;
        node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetLowKeyPair().second);
        current_node = node;
        if (!(current_node->TrySharedLock())) {
          // If the shared latch on the current node is node available, abort operation to prevent
          // deadlock scenarios.
          parent->ReleaseNodeSharedLatch();
          return false;
        }
        parent->ReleaseNodeSharedLatch();
        element_p = node->End() - 1;
      }
    }

    if (reach_end) {
      return true;
    }

    while (KeyCmpGreaterEqual(element_p->first, index_low_key)) {
      auto itr_list = element_p->second->begin();
      while (itr_list != element_p->second->end()) {
        value_list->push_back(*itr_list);
        itr_list++;
      }

      element_p--;
      if (element_p == node->Begin() - 1) {
        parent = node;
        if (node->GetLowKeyPair().second == nullptr) break;
        node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetLowKeyPair().second);
        current_node = node;
        if (!(current_node->TrySharedLock())) {
          // If shared latch on current node is not available, abort operation to prevent
          // deadlock scenarios.
          parent->ReleaseNodeSharedLatch();
          return false;
        }
        parent->ReleaseNodeSharedLatch();
        element_p = node->End() - 1;
      }
    }

    current_node->ReleaseNodeSharedLatch();
    return true;
  }

  /**
   * Scan keys starting from high key and moves till low key or till limit, and populates a vector
   * with the values found, if they are visible to the transaction. Since there is a possibility of
   * deadlock with a ScanAscending and concurrent delete/insert, if shared latch on a node is not
   * available, the entire operation is forfeited, and false is returned. The caller is expected to
   * retry the operation till it succeeds, returning true.
   *
   * @param index_low_key Key to end at
   * @param index_high_key Key to start at
   * @param value_list List to be populated with results
   * @param limit Upper bound of number of values to return
   * @param predicate Predicate to be satisfied to add a value to the result
   * @return true on success, false on failure
   */
  bool ScanLimitDescending(KeyType index_low_key, KeyType index_high_key, std::vector<TupleSlot> *value_list,
                           uint32_t limit, std::function<bool(const ValueType)> predicate) {
    root_latch_.LockShared();

    if (root_ == nullptr) {
      root_latch_.UnlockShared();
      return true;
    }

    BaseNode *current_node = root_;
    BaseNode *parent = nullptr;

    // Release root latch and get shared latch on the root node
    current_node->GetNodeSharedLatch();
    root_latch_.UnlockShared();

    // Traversing Down to the right leaf node
    while (current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = static_cast<InnerNode *>(node)->FindLocation(index_high_key, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.

      // set parent for crabbing
      parent = node;
      if (index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      } else {
        current_node = node->GetLowKeyPair().second;
      }

      // Unlock parent and get shared latch on current node
      current_node->GetNodeSharedLatch();
      parent->ReleaseNodeSharedLatch();
    }

    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    KeyValuePair *element_p;
    element_p = static_cast<LeafNode *>(node)->FindLocation(index_high_key, this);

    bool reach_end = false;
    if (element_p != node->Begin()) {
      element_p--;
    } else {
      if (node->GetLowKeyPair().second == nullptr) {
        current_node->ReleaseNodeSharedLatch();
        reach_end = true;
      } else {
        parent = node;
        node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetLowKeyPair().second);
        current_node = node;
        if (!(current_node->TrySharedLock())) {
          // If shared latch on current node is not available, abort operation to prevent
          // deadlock scenarios.
          parent->ReleaseNodeSharedLatch();
          return false;
        }
        parent->ReleaseNodeSharedLatch();
        element_p = node->End() - 1;
      }
    }

    if (reach_end) {
      return true;
    }

    while ((value_list->size() < limit) && KeyCmpGreaterEqual(element_p->first, index_low_key)) {
      auto itr_list = element_p->second->begin();
      while (itr_list != element_p->second->end()) {
        if (!predicate(*itr_list)) {
          itr_list++;
          continue;
        }
        value_list->push_back(*itr_list);
        if (value_list->size() >= limit) break;
        itr_list++;
      }

      element_p--;
      if (element_p == node->Begin() - 1) {
        parent = node;
        if (node->GetLowKeyPair().second == nullptr) break;
        node = reinterpret_cast<ElasticNode<KeyValuePair> *>(node->GetLowKeyPair().second);
        current_node = node;
        if (!(current_node->TrySharedLock())) {
          // If shared latch on current node is not available, abort operation to prevent
          // deadlock scenarios.
          parent->ReleaseNodeSharedLatch();
          return false;
        }
        parent->ReleaseNodeSharedLatch();
        element_p = node->End() - 1;
      }
    }

    current_node->ReleaseNodeSharedLatch();
    return true;
  }

  /**
   * ReleaseAllLocks - This function releases all locks currently held, according to the list
   * passed.
   *
   * NOTE: The function assumes that all the locks being held are exclusive locks.
   */
  bool ReleaseAllLocks(std::vector<BaseNode *> *node_list, bool got_root_latch) {
    while (!node_list->empty()) {
      (*node_list->rbegin())->ReleaseNodeLatch();
      node_list->pop_back();
    }
    if (got_root_latch) {
      got_root_latch = false;
      root_latch_.UnlockExclusive();
    }
    return got_root_latch;
  }

  /**
   * This function adds an element in the tree
   * The structure followed in the code is the lowKeyPointerPair's pointer represents
   * the leftmost pointer. While for all other nodes their pointer go to a node on their
   * right, ie containing values with keys greater than them.
   * @param element The element to be inserted
   * @param predicate The predicate function that should be satisfied while insertion
   * @return true on successful insertion, false otherwise
   */
  bool Insert(const KeyElementPair element, std::function<bool(const ValueType)> predicate) {
    /*
     * Try Optimistic Insert
     * Assuming insert will not cause any overflows, get shared latch for all nodes except the
     * leaf node. For the leaf node where insert occurs, get exclusive access.
     */

    // Get access to the Tree
    root_latch_.LockExclusive();

    if (root_ == nullptr) {
      // If root is nullptr then we make a Leaf Node.
      KeyNodePointerPair p1, p2;
      p1.first = element.first;
      p2.first = element.first;
      p1.second = nullptr;
      p2.second = nullptr;

      root_ = ElasticNode<KeyValuePair>::Get(leaf_node_size_upper_threshold_, NodeType::LeafType, 0,
                                             leaf_node_size_upper_threshold_, p1, p2);
    }

    // NOTE: At this point, exclusive lock to tree (root_latch_) is held.

    BaseNode *current_node = root_;
    BaseNode *parent_node = nullptr;

    // Get shared latch on root node
    current_node->GetNodeSharedLatch();

    // Traversing Down and maintaining a stack of pointers
    while (current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);

      // Release parent's shared lock
      if (parent_node != nullptr) {
        parent_node->ReleaseNodeSharedLatch();
      } else {
        // Release the exclusive lock on the tree
        root_latch_.UnlockExclusive();
      }

      // NOTE: FindLocation returns the location of first element that compares greater than
      auto index_pointer = static_cast<InnerNode *>(node)->FindLocation(element.first, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      if (index_pointer != node->Begin()) {
        index_pointer -= 1;
        parent_node = current_node;
        current_node = index_pointer->second;
      } else {
        parent_node = current_node;
        current_node = node->GetLowKeyPair().second;
      }

      // Get current node's shared lock
      current_node->GetNodeSharedLatch();
    }

    // Try optimistic insertion into the leaf node, if insertion without splitting is possible.

    // Get current node's exclusive lock and release lock on the parent
    current_node->ReleaseNodeSharedLatch();
    current_node->GetNodeExclusiveLatch();
    if (parent_node != nullptr) {
      parent_node->ReleaseNodeSharedLatch();
    } else {
      root_latch_.UnlockExclusive();
    }

    // Beyond this we only have exclusive latch on the current_node

    bool finished_insertion = false;
    // We maintain the element that we have to recursively insert up.
    // This is the element that has to be inserted into the inner nodes.
    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);

    auto location_greater_key_leaf = static_cast<LeafNode *>(node)->FindLocation(element.first, this);
    if (location_greater_key_leaf != node->Begin()) {
      if (KeyCmpEqual((location_greater_key_leaf - 1)->first, element.first)) {
        // Key present in tree => insert into value list
        auto itr_list = (location_greater_key_leaf - 1)->second->begin();
        while (itr_list != (location_greater_key_leaf - 1)->second->end()) {
          if (ValueCmpEqual(*itr_list, element.second) || predicate(*itr_list)) {
            // Release all locks if the value is already present
            current_node->ReleaseNodeLatch();
            return false;
          }
          itr_list++;
        }
        (location_greater_key_leaf - 1)->second->push_back(element.second);

        // Release the latch, insertion is complete
        current_node->ReleaseNodeLatch();

        num_values_++;
        finished_insertion = true;
      }
    }

    // Insertion not done yet as key is not present
    if (!finished_insertion) {
      auto value_list = new std::list<ValueType>();
      value_list->push_back(element.second);
      KeyValuePair key_list_value;
      key_list_value.first = element.first;
      key_list_value.second = value_list;
      if (node->InsertElementIfPossible(key_list_value,
                                        static_cast<LeafNode *>(node)->FindLocation(element.first, this))) {
        // If you can directly insert in the leaf - Insertion is over
        finished_insertion = true;
        // Insertion is done, release the latch
        current_node->ReleaseNodeLatch();
        num_keys_++;
        num_values_++;
      } else {
        delete value_list;
      }
      // Otherwise, split the node
      // Optimistic approach failed
    }

    if (!finished_insertion) {
      // Optimistic approach failed. Release the latch, and start grabbing
      // exclusive latches.
      current_node->ReleaseNodeLatch();
    } else {
      return true;
    }

    /*
     ****************************************
      if not successful -> pessimistic insert
     ****************************************
    */

    /*
     * If root is nullptr then make a Leaf Node.
     */
    root_latch_.LockExclusive();
    bool got_root_latch = true;

    if (root_ == nullptr) {
      KeyNodePointerPair p1, p2;
      p1.first = element.first;
      p2.first = element.first;
      p1.second = nullptr;
      p2.second = nullptr;
      root_ = ElasticNode<KeyValuePair>::Get(leaf_node_size_upper_threshold_, NodeType::LeafType, 0,
                                             leaf_node_size_upper_threshold_, p1, p2);
    }

    current_node = root_;

    // Get exclusive access on root node
    current_node->GetNodeExclusiveLatch();

    // Stack of pointers
    std::vector<BaseNode *> node_list;

    // Traversing Down and maintaining a stack of pointers
    while (current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);

      // If the current node is safe, release all latches currently held
      if (node->GetSize() < node->GetItemCount()) {
        got_root_latch = ReleaseAllLocks(&node_list, got_root_latch);
      }

      node_list.push_back(current_node);
      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = static_cast<InnerNode *>(node)->FindLocation(element.first, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      if (index_pointer != node->Begin()) {
        index_pointer -= 1;
        current_node = index_pointer->second;
      } else {
        current_node = node->GetLowKeyPair().second;
      }

      // Acquire exclusive access on current node
      current_node->GetNodeExclusiveLatch();
    }

    finished_insertion = false;
    // We maintain the element that we have to recursively insert up.
    // This is the element that has to be inserted into the inner nodes.
    KeyNodePointerPair inner_node_element;
    node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);

    location_greater_key_leaf = static_cast<LeafNode *>(node)->FindLocation(element.first, this);
    if (location_greater_key_leaf != node->Begin()) {
      if (KeyCmpEqual((location_greater_key_leaf - 1)->first, element.first)) {
        auto itr_list = (location_greater_key_leaf - 1)->second->begin();
        while (itr_list != (location_greater_key_leaf - 1)->second->end()) {
          if (ValueCmpEqual(*itr_list, element.second) || predicate(*itr_list)) {
            // Release all locks if element is already present
            current_node->ReleaseNodeLatch();
            got_root_latch = ReleaseAllLocks(&node_list, got_root_latch);

            return false;
          }
          itr_list++;
        }
        (location_greater_key_leaf - 1)->second->push_back(element.second);

        // Insertion is complete, release all locks
        current_node->ReleaseNodeLatch();
        got_root_latch = ReleaseAllLocks(&node_list, got_root_latch);

        num_values_++;
        finished_insertion = true;
      }
    }
    if (!finished_insertion) {
      auto value_list = new std::list<ValueType>();
      value_list->push_back(element.second);
      KeyValuePair key_list_value;
      key_list_value.first = element.first;
      key_list_value.second = value_list;
      if (node->InsertElementIfPossible(key_list_value,
                                        static_cast<LeafNode *>(node)->FindLocation(element.first, this))) {
        // If you can directly insert in the leaf - Insertion is over
        finished_insertion = true;
        got_root_latch = ReleaseAllLocks(&node_list, got_root_latch);
      } else {
        // Otherwise you split the node
        // Now pay attention to the fact that when we copy up the right pointer
        // remains same and the new node after splitting that gets returned to us
        // becomes the right pointer.
        // Thus our inner node element will contain the first key of the splitted
        // Node and a pointer to the splitted node.
        auto splitted_node = node->SplitNode();
        auto splitted_node_begin = splitted_node->Begin();
        // To decide which leaf to put in the element
        if (KeyCmpGreater(splitted_node_begin->first, element.first)) {
          node->InsertElementIfPossible(key_list_value,
                                        static_cast<LeafNode *>(node)->FindLocation(element.first, this));
        } else {
          splitted_node->InsertElementIfPossible(
              key_list_value, static_cast<LeafNode *>(splitted_node)->FindLocation(element.first, this));
        }

        // Set the siblings correctly
        // node_next = node_right
        // node_next_left = splitted
        // splitted_right = node_right
        // node_right = splitted
        // splitted_left = node
        if (node->GetElasticHighKeyPair()->second != nullptr) {
          // Get exclusive access on right node to set its links
          node->GetElasticHighKeyPair()->second->GetNodeExclusiveLatch();

          auto node_next = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(node->GetElasticHighKeyPair()->second);
          node_next->GetElasticLowKeyPair()->second = splitted_node;

          // Release exclusive access on right node
          node->GetElasticHighKeyPair()->second->ReleaseNodeLatch();
        }
        splitted_node->GetElasticHighKeyPair()->second = node->GetElasticHighKeyPair()->second;
        node->GetElasticHighKeyPair()->second = splitted_node;
        splitted_node->GetElasticLowKeyPair()->second = node;

        // Set the inner node that needs to be passed to the parents
        inner_node_element.first = splitted_node->Begin()->first;
        inner_node_element.second = splitted_node;
      }

      current_node->ReleaseNodeLatch();
      num_keys_++;
      num_values_++;
    }

    while (!finished_insertion && !node_list.empty()) {
      auto inner_node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(*node_list.rbegin());
      node_list.pop_back();

      // If we can insert element now without splitting then we are done
      if (inner_node->InsertElementIfPossible(
              inner_node_element, static_cast<InnerNode *>(inner_node)->FindLocation(inner_node_element.first, this))) {
        finished_insertion = true;
      } else {
        // otherwise we have to recursively split again

        auto splitted_node = inner_node->SplitNode();
        auto splitted_node_begin = splitted_node->Begin();
        if (KeyCmpGreater(splitted_node_begin->first, inner_node_element.first)) {
          inner_node->InsertElementIfPossible(
              inner_node_element, static_cast<InnerNode *>(inner_node)->FindLocation(inner_node_element.first, this));
        } else {
          splitted_node->InsertElementIfPossible(
              inner_node_element,
              static_cast<InnerNode *>(splitted_node)->FindLocation(inner_node_element.first, this));
        }

        splitted_node->GetElasticLowKeyPair()->second = splitted_node->Begin()->second;
        inner_node_element.first = splitted_node->Begin()->first;
        inner_node_element.second = splitted_node;
        splitted_node->PopBegin();
      }
      inner_node->ReleaseNodeLatch();
    }

    // If still insertion is not finished we have to split the root node.
    // Remember the root must have been split by now.
    if (!finished_insertion) {
      NOISEPAGE_ASSERT(got_root_latch, "Root Latch should be held here");
      auto old_root = root_;
      KeyNodePointerPair p1, p2;
      p1.first = inner_node_element.first; /* This is a dummy initialization */
      p2.first = inner_node_element.first; /* This is a dummy initialization */
      p1.second = old_root;                /* This initialization matters */
      p2.second = nullptr;                 /* This is a dummy initialization */
      root_ = ElasticNode<KeyNodePointerPair>::Get(inner_node_size_upper_threshold_, NodeType::InnerType,
                                                   root_->GetDepth() + 1, inner_node_size_upper_threshold_, p1, p2);
      auto new_root_node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(root_);
      new_root_node->InsertElementIfPossible(
          inner_node_element, static_cast<InnerNode *>(new_root_node)->FindLocation(inner_node_element.first, this));
    }

    if (got_root_latch) {
      got_root_latch = false;
      root_latch_.UnlockExclusive();
    }

    return true;
  }

  /**
   * DeleteRebalance - Function that deletes and rebalances the tree by borrowing from siblings or by
   * merging nodes.
   */
  template <typename ElementType>
  void DeleteRebalance(ElasticNode<KeyNodePointerPair> *parent, BaseNode *input_child_pointer, int index,
                       int node_lower_threshold) {
    // We assume that we have exclusive lock for parent and only try to get lock for child nodes
    // Get child lock before proceeding
    input_child_pointer->GetNodeExclusiveLatch();

    auto child = reinterpret_cast<ElasticNode<ElementType> *>(input_child_pointer);

    if (child->GetSize() >= node_lower_threshold) {
      // Rebalancing not required
      input_child_pointer->ReleaseNodeLatch();
      return;
    }

    // Need to rebalance
    if (index > -1) {
      BaseNode *left_sibling_base_node;
      ElasticNode<ElementType> *left_sibling;
      if (index == 0) {
        left_sibling_base_node = parent->GetLowKeyPair().second;
        left_sibling = reinterpret_cast<ElasticNode<ElementType> *>(parent->GetLowKeyPair().second);
      } else {
        left_sibling_base_node = (parent->Begin() + index - 1)->second;
        left_sibling = reinterpret_cast<ElasticNode<ElementType> *>((parent->Begin() + index - 1)->second);
      }

      // Get left sibling lock before checking for borrowing
      left_sibling_base_node->GetNodeExclusiveLatch();

      if (left_sibling->GetSize() > node_lower_threshold) {
        /*
        This is for Inner Node only
        Remember for leaf node - You cannot bring down A.
        Capital chars represent Keys. Small chars represent nodes.
                            A                                               C
                          /   \                                          /     \
                       [ B ]     [ C  D ]           ==                [B  A]    [D]
                        /  \      /  \  \                             /  \  \   /  \
                       a    b    c   d   e                           a   b   c  d   e

            Initially
           C - (parent->Begin() + index)-key
           A - left sibling->Rbegin()->first
           D - child->Begin()->first
           d - child->lowkeypair().second
           c - left sibling->Rbegin()->second

            Finally
           A - (parent->Begin() + index)-key
           C - child->Begin()->first
           c - child->lowkeypair().second
           d - child->Begin()->second
           D - (child->Begin() + 1)->first - Happens automatically
        */

        // Borrow one

        if (child->GetType() == NodeType::LeafType) {
          (parent->Begin() + index)->first = left_sibling->RBegin()->first;
          child->InsertElementIfPossible(*(left_sibling->RBegin()), child->Begin());
          left_sibling->PopEnd();
        } else {
          auto inner_child = reinterpret_cast<InnerNode *>(input_child_pointer);
          auto inner_left_sibling = reinterpret_cast<InnerNode *>(left_sibling);
          // Make C->d to insert in child
          auto parent_key = (parent->Begin() + index)->first;             /* C */
          auto current_low_pointer = inner_child->GetLowKeyPair().second; /* d */
          KeyNodePointerPair to_insert;
          to_insert.first = parent_key;
          to_insert.second = current_low_pointer;
          inner_child->InsertElementIfPossible(to_insert, inner_child->Begin());
          // Make low key pointer c
          inner_child->GetElasticLowKeyPair()->second = inner_left_sibling->RBegin()->second;

          // Update parent key to A
          (parent->Begin() + index)->first = inner_left_sibling->RBegin()->first;

          // Delete A->c
          left_sibling->PopEnd();
        }

        // Borrow successful, release child and left sibling locks
        input_child_pointer->ReleaseNodeLatch();
        left_sibling_base_node->ReleaseNodeLatch();

        return;
      }

      // Borrow unsuccessful, release left sibling locks
      left_sibling_base_node->ReleaseNodeLatch();
    }

    if (index < parent->GetSize() - 1) {
      BaseNode *right_sibling_base_node = (parent->Begin() + index + 1)->second;
      auto *right_sibling = reinterpret_cast<ElasticNode<ElementType> *>((parent->Begin() + index + 1)->second);

      // Get right sibling lock before checking for borrowing
      right_sibling_base_node->GetNodeExclusiveLatch();

      if (right_sibling->GetSize() > node_lower_threshold) {
        /*
        This is for Inner Node only
        Remember for leaf node - You cannot bring down A.
        Capital chars represent Keys. Small chars represent nodes.
                        X   A                                          X    C
                          \   \                                          /     \
                       [ B ]     [ C  D ]           ==                [B  A]    [D]
                        /  \      /  \  \                             /  \  \   /  \
                       a    b    c   d   e                           a   b   c  d   e

            Initially
           X - (parent->Begin() + index)-key
           A - (parent->Begin() + index + 1)-key
           C - right_sibling->Begin()->first
           c - right_sibling->lowkeypair().second
           d - right_sibling->Begin()->second
           D - (right_sibling->Begin() + 1)->first - Happens automatically

            Finally
           X - (parent->Begin() + index)-key
           C - (parent->Begin() + index + 1)-key
           A - child->Rbegin()->first
           D - right_sibling->Begin()->first
           d - right_sibling->lowkeypair().second
           c - child->Rbegin()->second
        */

        // Borrow one

        if (child->GetType() == NodeType::InnerType) {
          auto inner_child = reinterpret_cast<InnerNode *>(input_child_pointer);
          auto inner_right_sibling = reinterpret_cast<InnerNode *>(right_sibling);
          // Make A->c to insert in child
          auto parent_key = (parent->Begin() + index + 1)->first;                 /* A */
          auto current_low_pointer = inner_right_sibling->GetLowKeyPair().second; /* c */
          KeyNodePointerPair to_insert;
          to_insert.first = parent_key;
          to_insert.second = current_low_pointer;
          inner_child->InsertElementIfPossible(to_insert, inner_child->End());
          // Make low key pointer d
          right_sibling->GetElasticLowKeyPair()->second = inner_right_sibling->Begin()->second;

          // Update A to C
          (parent->Begin() + index + 1)->first = inner_right_sibling->Begin()->first;

          // Delete C-d
          right_sibling->PopBegin();
        } else {
          child->InsertElementIfPossible(*(right_sibling->Begin()), child->End());
          right_sibling->PopBegin();
          (parent->Begin() + index + 1)->first = right_sibling->Begin()->first;
        }

        // Borrow successful, release child and left sibling locks
        input_child_pointer->ReleaseNodeLatch();
        right_sibling_base_node->ReleaseNodeLatch();

        return;
      }

      // Borrow unsuccessful, release right sibling locks
      right_sibling_base_node->ReleaseNodeLatch();
    }

    // Cannot redistribute, so we perform merge
    // We try to merge with left sibling first, if not possible
    // merge with right sibling
    if (index > -1) {
      BaseNode *left_sibling_base_node;
      ElasticNode<ElementType> *left_sibling;
      if (index == 0) {
        left_sibling_base_node = parent->GetLowKeyPair().second;
        left_sibling = reinterpret_cast<ElasticNode<ElementType> *>(parent->GetLowKeyPair().second);
      } else {
        left_sibling_base_node = (parent->Begin() + index - 1)->second;
        left_sibling = reinterpret_cast<ElasticNode<ElementType> *>((parent->Begin() + index - 1)->second);
      }

      // Get left sibling lock before checking for borrowing
      left_sibling_base_node->GetNodeExclusiveLatch();

      /*
                           A
                         /   \                                      B  A  C  D
                        B    [C  D]              ==                /  \  \  \  \
                       / \   /  \  \                              a    b  c  d  e
                      a   b  c   d  e
      */

      if (left_sibling->GetType() == NodeType::InnerType) {
        auto parent_key = (parent->Begin() + index)->first;
        auto current_low_pointer = child->GetLowKeyPair().second;
        auto inner_left_sibling = reinterpret_cast<InnerNode *>(left_sibling);

        KeyNodePointerPair to_insert;
        to_insert.first = parent_key;
        to_insert.second = current_low_pointer;
        inner_left_sibling->InsertElementIfPossible(to_insert, inner_left_sibling->End());
      }

      // Fixing sibling pointers code
      if (left_sibling->GetType() == NodeType::LeafType) {
        if (child->GetHighKeyPair().second != nullptr) {
          BaseNode *new_right_sibling = child->GetHighKeyPair().second;

          // Get right sibling lock before updating sibling pointers
          new_right_sibling->GetNodeExclusiveLatch();

          left_sibling->GetElasticHighKeyPair()->second = child->GetHighKeyPair().second;
          reinterpret_cast<ElasticNode<KeyValuePair> *>(child->GetHighKeyPair().second)
              ->GetElasticLowKeyPair()
              ->second = left_sibling;

          // updating sibling pointers complete, release lock
          new_right_sibling->ReleaseNodeLatch();

        } else {
          left_sibling->GetElasticHighKeyPair()->second = child->GetHighKeyPair().second;
        }
      }

      left_sibling->MergeNode(child);

      // Merge successful, release child and left sibling locks
      input_child_pointer->ReleaseNodeLatch();
      left_sibling_base_node->ReleaseNodeLatch();

      child->FreeElasticNode();
      parent->Erase(index);

    } else {
      BaseNode *right_sibling_base_node = (parent->Begin() + index + 1)->second;
      auto *right_sibling = reinterpret_cast<ElasticNode<ElementType> *>((parent->Begin() + index + 1)->second);

      // Get left sibling lock before checking for borrowing
      right_sibling_base_node->GetNodeExclusiveLatch();

      if (right_sibling->GetType() == NodeType::InnerType) {
        auto parent_key = (parent->Begin() + index + 1)->first;
        auto current_low_pointer = right_sibling->GetLowKeyPair().second;
        auto inner_child = reinterpret_cast<InnerNode *>(child);

        KeyNodePointerPair to_insert;
        to_insert.first = parent_key;
        to_insert.second = current_low_pointer;
        inner_child->InsertElementIfPossible(to_insert, inner_child->End());
      }

      // Fixing sibling pointers code
      if (child->GetType() == NodeType::LeafType) {
        if (right_sibling->GetHighKeyPair().second != nullptr) {
          BaseNode *new_right_sibling = right_sibling->GetHighKeyPair().second;

          // Get right sibling lock before updating sibling pointers
          new_right_sibling->GetNodeExclusiveLatch();

          child->GetElasticHighKeyPair()->second = right_sibling->GetHighKeyPair().second;
          reinterpret_cast<ElasticNode<KeyValuePair> *>(right_sibling->GetHighKeyPair().second)
              ->GetElasticLowKeyPair()
              ->second = child;

          // updating sibling pointers complete, release lock
          new_right_sibling->ReleaseNodeLatch();
        } else {
          child->GetElasticHighKeyPair()->second = right_sibling->GetHighKeyPair().second;
        }
      }

      child->MergeNode(right_sibling);

      // Merge successful, release child and right sibling locks
      input_child_pointer->ReleaseNodeLatch();
      right_sibling_base_node->ReleaseNodeLatch();

      right_sibling->FreeElasticNode();
      parent->Erase(index + 1);
    }
  }

  /**
   * RelaseLastLocksDelete - Releases the node's latch and pops it from the list
   */
  void RelaseLastLocksDelete(std::vector<common::SharedLatch *> *lock_list) {
    if (!lock_list->empty()) {
      (*lock_list->rbegin())->UnlockExclusive();
      lock_list->pop_back();
    }
  }

  /**
   * This function tries to perform optimistic deletion of an element, if not possible, performs
   * pessimistic deletion and rebalances the tree. If the element is not found, it returns false.
   * @param element Element to be deleted
   * @return true on success, false on failure
   */
  bool DeleteElement(const KeyElementPair &element) {
    /*
     ****************************
      Try optimistic delete
     ****************************
    */
    // If root is nullptr then we return false.
    root_latch_.LockExclusive();

    if (root_ == nullptr) {
      root_latch_.UnlockExclusive();
      return false;
    }
    // Beyond this point we'll have exclusive_lock on tree lock

    BaseNode *current_node = root_;
    BaseNode *parent_node = nullptr;

    // Get Shared Latch on the root
    current_node->GetNodeSharedLatch();

    // Traversing Down
    while (current_node->GetType() != NodeType::LeafType) {
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);

      // Release parent shared latch
      if (parent_node != nullptr) {
        parent_node->ReleaseNodeSharedLatch();
      } else {
        root_latch_.UnlockExclusive();
      }

      // Note that Find Location returns the location of first element
      // that compare greater than
      auto index_pointer = static_cast<InnerNode *>(node)->FindLocation(element.first, this);
      // Thus we have to go in the left side of location which will be the
      // pointer of the previous location.
      if (index_pointer != node->Begin()) {
        index_pointer -= 1;
        parent_node = current_node;
        current_node = index_pointer->second;
      } else {
        parent_node = current_node;
        current_node = node->GetLowKeyPair().second;
      }

      // Acquire shared latch on the current node
      current_node->GetNodeSharedLatch();
    }

    // Now we try deletion from the found leaf node
    // only if without sharing or merge is possible

    // Get exclusive lock on the leaf node for deletion
    current_node->ReleaseNodeSharedLatch();
    current_node->GetNodeExclusiveLatch();
    if (parent_node != nullptr) {
      parent_node->ReleaseNodeSharedLatch();
    } else {
      root_latch_.UnlockExclusive();
    }

    bool finished_deletion = false;
    auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
    auto location_greater_key_leaf = static_cast<LeafNode *>(node)->FindLocation(element.first, this);
    if (location_greater_key_leaf != node->Begin()) {
      if (KeyCmpEqual((location_greater_key_leaf - 1)->first, element.first)) {
        // Key present in tree => check if value present & delete from value list
        bool found_value = false;
        auto itr_list = (location_greater_key_leaf - 1)->second->begin();
        while (itr_list != (location_greater_key_leaf - 1)->second->end()) {
          if (ValueCmpEqual(*itr_list, element.second)) {
            // Value fround => Delete element from list if won't trigger rebalance
            found_value = true;
            if (((location_greater_key_leaf - 1)->second->size() > 1) ||
                (node->GetSize() > GetLeafNodeSizeLowerThreshold())) {
              (location_greater_key_leaf - 1)->second->erase(itr_list);
              finished_deletion = true;
              break;
            }
          }
          itr_list++;
        }

        if (!found_value) {
          // Value not in tree
          // Release the latch and return
          current_node->ReleaseNodeLatch();
          return false;
        }

        if (found_value) {
          if (finished_deletion) {
            // If now the list is empty delete key-emptylist from the tree
            bool key_deleted = false;
            if ((location_greater_key_leaf - 1)->second->empty()) {
              delete (location_greater_key_leaf - 1)->second;
              key_deleted = node->Erase((location_greater_key_leaf - 1) - node->Begin());
            }

            // Release the latch and return
            current_node->ReleaseNodeLatch();

            if (key_deleted) {
              num_keys_--;
            }

            num_values_--;
            return true;
          }

          if (!finished_deletion) {
            // Need to continue with pessimistic delete
            // Release the latch
            current_node->ReleaseNodeLatch();
          }
        }
      } else {
        // Deletion not done yet as key is not present
        // Release the lock and return
        current_node->ReleaseNodeLatch();

        return false;
      }
    } else {
      // Deletion not done yet as key is not present
      // Release the lock and return
      current_node->ReleaseNodeLatch();

      return false;
    }

    /*
     ****************************************
      if not successful -> pessimistic delete
     ****************************************
    */

    std::vector<common::SharedLatch *> lock_list;
    root_latch_.LockExclusive();
    lock_list.push_back(&root_latch_);
    bool is_deleted = Delete(root_, element, &lock_list);
    RelaseLastLocksDelete(&lock_list);
    return is_deleted;
  }

  /**
   * Delete() - Remove a key-value pair from the tree
   *
   * This function returns false if the key and value pair does not
   * exist. Return true if delete succeeds
   *
   */
  bool Delete(BaseNode *current_node, const KeyElementPair &element, std::vector<common::SharedLatch *> *lock_list) {
    // If tree is empty, return false
    if (current_node == nullptr) {
      return false;
    }

    // Get exclusive latch on current node. Crabbing is performed and all parent locks are released
    // whenever a safe node is encountered.
    current_node->GetNodeExclusiveLatch();

    bool condition_for_underflow = true;
    if (current_node == root_) {
      if (current_node->GetType() == NodeType::InnerType &&
          (reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node))->GetSize() > 1) {
        condition_for_underflow = false;
      } else if (current_node->GetType() == NodeType::LeafType &&
                 (reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node))->GetSize() > 1) {
        condition_for_underflow = false;
      }
    }
    if (current_node != root_ && current_node->GetType() == NodeType::InnerType &&
        (reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node))->GetSize() >
            GetInnerNodeSizeLowerThreshold()) {
      condition_for_underflow = false;
    }
    if (current_node != root_ && current_node->GetType() == NodeType::LeafType &&
        (reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node))->GetSize() > GetLeafNodeSizeLowerThreshold()) {
      condition_for_underflow = false;
    }

    if (!condition_for_underflow) {
      while (!lock_list->empty()) {
        (*lock_list->rbegin())->UnlockExclusive();
        lock_list->pop_back();
      }
    }

    lock_list->push_back(current_node->GetLatchPointer());

    // If delete called on leaf node, just perform deletion
    // Else, call delete on child and check if child underflows
    if (current_node->GetType() == NodeType::LeafType) {
      // Leaf Node case => delete element
      auto node = reinterpret_cast<ElasticNode<KeyValuePair> *>(current_node);
      auto leaf_position = static_cast<LeafNode *>(node)->FindLocation(element.first, this);
      if (leaf_position != node->Begin()) {
        leaf_position -= 1;
        if (KeyCmpEqual(leaf_position->first, element.first)) {
          bool element_present = false;
          auto itr_list = (leaf_position)->second->begin();
          while (itr_list != (leaf_position)->second->end()) {
            if (ValueCmpEqual(*itr_list, element.second)) {
              // Delete element from list
              (leaf_position)->second->erase(itr_list);
              element_present = true;
              break;
            }
            itr_list++;
          }

          // Not Found - Return false
          if (!element_present) {
            // Release the lock and return
            RelaseLastLocksDelete(lock_list);
            return false;
          }

          if (!leaf_position->second->empty()) {
            // Release the lock and return
            RelaseLastLocksDelete(lock_list);
            num_values_--;
            return true;
          }

          // If now the list is empty delete key-emptylist from the tree
          delete leaf_position->second;
          bool is_deleted = node->Erase(leaf_position - node->Begin());
          if (is_deleted && node->GetSize() == 0) {
            // All elements of tree are now deleted
            node->FreeElasticNode();  // Important - we need to free node
            root_ = nullptr;
          }

          // Release the lock
          RelaseLastLocksDelete(lock_list);
          num_values_--;
          num_keys_--;

          return is_deleted;
        }

        // Key not found
        RelaseLastLocksDelete(lock_list);
        return false;
      }

      if (leaf_position == node->Begin()) {
        // Key not found
        RelaseLastLocksDelete(lock_list);
        return false;
      }

    } else {
      // Inner Node case => call delete element on child and check if child underflows
      auto node = reinterpret_cast<ElasticNode<KeyNodePointerPair> *>(current_node);
      auto child_position = static_cast<InnerNode *>(node)->FindLocation(element.first, this);
      BaseNode *child_pointer;
      int index;
      if (child_position != node->Begin()) {
        child_pointer = (child_position - 1)->second;
        index = child_position - node->Begin() - 1;
      } else {
        child_pointer = node->GetLowKeyPair().second;
        index = -1;
      }
      bool is_deleted = Delete(child_pointer, element, lock_list);

      // Now perform any re-balancing or merge on child if it underflows
      if (is_deleted) {
        if (!lock_list->empty()) {
          if (child_pointer->GetType() == NodeType::LeafType) {
            DeleteRebalance<KeyValuePair>(node, child_pointer, index, GetLeafNodeSizeLowerThreshold());
          } else {
            DeleteRebalance<KeyNodePointerPair>(node, child_pointer, index, GetInnerNodeSizeLowerThreshold());
          }
        }

        // Check if this node is root and if its size becomes 0
        if (node->GetSize() == 0) {
          root_ = current_node->GetLowKeyPair().second;

          // Release the lock and free the node
          RelaseLastLocksDelete(lock_list);
          node->FreeElasticNode();
          return true;
        }

        // Release the lock
        RelaseLastLocksDelete(lock_list);

        return true;
      }

      // Reached here => not deleted
      RelaseLastLocksDelete(lock_list);
      return false;
    }

    return false;
  }

  /**
   * Returns the size of the B+ Tree (number of keys stored)
   * @return The size of the tree
   */
  uint64_t GetSize() { return num_keys_; }

  /**
   * Returns the estimated heap usage of the B+ Tree. This function returns an estimate. For the
   * actual heap usage, GetHeapUsage() can be used.
   * @return Estimated heap usage
   */
  size_t EstimateHeapUsage() {
    // To estimate the heap usage, on an average, assume that the B+ Tree is always half full.
    if (root_ == nullptr) {
      return 0;
    }

    auto depth = root_->GetDepth();
    size_t heap_usage = (depth * GetInnerNodeSizeLowerThreshold() *
                         sizeof(KeyNodePointerPair)) +  // InnerNode size (assuming half full)
                        (num_keys_ * sizeof(KeyType)) +
                        (num_values_ * sizeof(ValueType));  // LeafNode size

    return heap_usage;
  }

  /**
   * Constructor for the B+ Tree
   * @param p_key_cmp_obj Key comparator
   * @param p_key_eq_obj Key equality checker
   * @param p_value_eq_obj Value equality checker
   */
  explicit BPlusTree(KeyComparator p_key_cmp_obj = KeyComparator{},
                     KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
                     ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{})
      : BPlusTreeBase(),
        // Key comparator, equality checker
        key_cmp_obj_{p_key_cmp_obj},
        key_eq_obj_{p_key_eq_obj},

        // Value equality checker
        value_eq_obj_{p_value_eq_obj},

        // Root and key/value counters
        root_(nullptr),
        num_keys_(0),
        num_values_(0) {}

  /**
   * Destructor - Destroy BPlusTree instance
   */
  ~BPlusTree() { FreeTree(); }
};  // class BPlusTree

}  // namespace noisepage::storage::index
