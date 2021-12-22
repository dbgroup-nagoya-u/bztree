/*
 * Copyright 2021 Database Group, Nagoya University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef BZTREE_BZTREE_HPP
#define BZTREE_BZTREE_HPP

#include <array>
#include <atomic>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "component/node.hpp"
#include "memory/epoch_based_gc.hpp"
#include "utility.hpp"

namespace dbgroup::index::bztree
{
/**
 * @brief A class to represent BzTree.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 */
template <class Key, class Payload, class Compare = std::less<Key>>
class BzTree
{
  using Metadata = component::Metadata;
  using StatusWord = component::StatusWord;
  using Node_t = component::Node<Key, Compare>;
  using NodeRC = component::NodeRC;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<Node_t>;
  using NodeStack = std::vector<std::pair<Node_t *, size_t>>;
  using MwCASDescriptor = component::MwCASDescriptor;
  using Binary_t = std::remove_pointer_t<Payload>;
  using Binary_p = std::unique_ptr<Binary_t, component::PayloadDeleter<Binary_t>>;

 public:
  /*################################################################################################
   * Public classes
   *##############################################################################################*/

  /**
   * @brief A class to represent a iterator for scan results.
   *
   * @tparam Key a target key class
   * @tparam Payload a target payload class
   * @tparam Compare a key-comparator class
   */
  class RecordIterator
  {
    using BzTree_t = BzTree<Key, Payload, Compare>;

   public:
    /*##############################################################################################
     * Public constructors and assignment operators
     *############################################################################################*/

    RecordIterator(  //
        BzTree_t *bztree,
        Node_t *node,
        const size_t current_pos)
        : bztree_{bztree},
          node_{node},
          record_count_{node->GetSortedCount()},
          current_pos_{current_pos},
          current_meta_{node->GetMetadata(current_pos_)}
    {
    }

    RecordIterator(const RecordIterator &) = delete;
    RecordIterator &operator=(const RecordIterator &) = delete;
    constexpr RecordIterator(RecordIterator &&) noexcept = default;
    constexpr RecordIterator &operator=(RecordIterator &&) noexcept = default;

    /*##############################################################################################
     * Public destructors
     *############################################################################################*/

    ~RecordIterator() = default;

    /*##############################################################################################
     * Public operators for iterators
     *############################################################################################*/

    /**
     * @return std::pair<Key, Payload>: a current key and payload pair
     */
    constexpr auto
    operator*() const  //
        -> std::pair<Key, Payload>
    {
      return {GetKey(), GetPayload()};
    }

    /**
     * @brief Forward an iterator.
     *
     */
    constexpr void
    operator++()
    {
      ++current_pos_;
      current_meta_ = node_->GetMetadata(current_pos_);
    }

    /*##############################################################################################
     * Public getters/setters
     *############################################################################################*/

    /**
     * @brief Check if there are any records left.
     *
     * function may call a scan function internally to get a next leaf node.
     *
     * @retval true if there are any records or next node left.
     * @retval false if there are no records and node left.
     */
    bool
    HasNext()
    {
      if (current_pos_ < record_count_) return true;
      if (node_->IsRightEnd()) return false;

      // keep the end key to use as the next begin key
      current_meta_ = node_->GetMetadata(record_count_ - 1);
      const auto &begin_key = GetKey();

      // update this iterator with the next scan results
      *this = bztree_->Scan(begin_key, false, node_.release());
      return HasNext();
    }

    /**
     * @return Key: a key of a current record
     */
    [[nodiscard]] constexpr auto
    GetKey() const  //
        -> Key
    {
      return node_->GetKey(current_meta_);
    }

    /**
     * @return Payload: a payload of a current record
     */
    [[nodiscard]] constexpr auto
    GetPayload() const  //
        -> Payload
    {
      return node_->template GetPayload<Payload>(current_meta_);
    }

   private:
    /*##############################################################################################
     * Internal member variables
     *############################################################################################*/

    /// a pointer to BwTree to perform continuous scan
    BzTree_t *bztree_{nullptr};

    /// the pointer to a node that includes partial scan results
    std::unique_ptr<Node_t> node_{nullptr};

    /// the number of records in this node
    size_t record_count_{0};

    /// the position of a current record
    size_t current_pos_{0};

    /// the metadata of a current record
    Metadata current_meta_{};
  };

  /*################################################################################################
   * Public constructors and assignment operators
   *##############################################################################################*/

  /**
   * @brief Construct a new BzTree object.
   *
   * @param gc_interval_microsec GC internal [us]
   */
  explicit BzTree(  //
      const size_t gc_interval_microsec = 100000,
      const size_t gc_thread_num = 1)
  {
    // create a GC instance for deleted nodes
    gc_ = std::make_unique<NodeGC_t>(gc_interval_microsec, gc_thread_num, true);

    // create an initial root node
    Node_t *leaf = CreateNewNode<Payload>();
    root_.store(leaf, std::memory_order_release);
  }

  BzTree(const BzTree &) = delete;
  BzTree &operator=(const BzTree &) = delete;
  BzTree(BzTree &&) = delete;
  BzTree &operator=(BzTree &&) = delete;

  /*################################################################################################
   * Public destructors
   *##############################################################################################*/

  /**
   * @brief Destroy the BzTree object.
   *
   */
  ~BzTree()
  {
    gc_->StopGC();
    DeleteChildren(GetRoot());
  }

  /*################################################################################################
   * Public read APIs
   *##############################################################################################*/

  /**
   * @brief Read a payload of a specified key if it exists.
   *
   * This function returns two return codes: kSuccess and kKeyNotExist. If a return code
   * is kSuccess, a returned pair contains a target payload. If a return code is
   * kKeyNotExist, the value of a returned payload is undefined.
   *
   * @param key a target key.
   * @return std::pair<ReturnCode, Payload>: a return code and payload pair.
   */
  auto
  Read(const Key &key)
  {
    [[maybe_unused]] const auto &guard = gc_->CreateEpochGuard();

    const Node_t *node = SearchLeafNode(key, true);

    Payload payload{};
    const auto &rc = node->Read(key, payload);
    if constexpr (IsVariableLengthData<Payload>()) {
      if (rc == NodeRC::kSuccess) {
        return std::make_pair(ReturnCode::kSuccess, Binary_p{payload});
      }
      return std::make_pair(ReturnCode::kKeyNotExist, Binary_p{});
    } else {
      if (rc == NodeRC::kSuccess) {
        return std::make_pair(ReturnCode::kSuccess, std::move(payload));
      }
      return std::make_pair(ReturnCode::kKeyNotExist, Payload{});
    }
  }

  /**
   * @brief Perform a full scan.
   *
   * @return an iterator to access target records.
   */
  auto
  Begin()  //
      -> RecordIterator
  {
    [[maybe_unused]] const auto &guard = gc_->CreateEpochGuard();

    const Node_t *node = SearchLeftEdgeLeaf();
    auto *page = CreateNewNode<Payload>();
    page->template Consolidate<Payload>(node);

    return RecordIterator{this, page, 0};
  }

  /**
   * @brief Perform a range scan with specified keys.
   *
   * @param begin_key a begin key of a range scan.
   * @param begin_closed a flag to indicate whether the begin side of a range is closed.
   * @param page a page that contains old keys/payloads. This argument is used internally.
   * @return an iterator to access target records.
   */
  auto
  Scan(  //
      const Key &begin_key,
      const bool begin_closed = true,
      Node_t *page = nullptr)  //
      -> RecordIterator
  {
    [[maybe_unused]] const auto &guard = gc_->CreateEpochGuard();

    if (page != nullptr) {
      gc_->AddGarbage(page);
    }

    const Node_t *node = SearchLeafNode(begin_key, begin_closed);
    page = CreateNewNode<Payload>();
    page->template Consolidate<Payload>(node);

    return RecordIterator{this, page, page->Search(begin_key, begin_closed)};
  }

  /*################################################################################################
   * Public write APIs
   *##############################################################################################*/

  /**
   * @brief Write (i.e., upsert) a specified kay/payload pair.
   *
   * If a specified key does not exist in the index, this function performs an insert
   * operation. If a specified key has been already inserted, this function perfroms an
   * update operation. Thus, this function always returns kSuccess as a return code.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_length the length of a target key.
   * @param payload_length the length of a target payload.
   * @return ReturnCode: kSuccess.
   */
  auto
  Write(  //
      const Key &key,
      const Payload &payload,
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_->CreateEpochGuard();

    while (true) {
      Node_t *node = SearchLeafNode(key, true);
      const auto &rc = node->Write(key, key_length, payload, payload_length);

      switch (rc) {
        case NodeRC::kSuccess:
          return ReturnCode::kSuccess;
        case NodeRC::kNeedConsolidation:
          Consolidate(node, key);
        default:
          break;
      }
    }
  }

  /**
   * @brief Insert a specified kay/payload pair.
   *
   * This function performs a uniqueness check in its processing. If a specified key
   * does not exist, this function insert a target payload into the index. If a
   * specified key exists in the index, this function does nothing and returns kKeyExist
   * as a return code.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_length the length of a target key.
   * @param payload_length the length of a target payload.
   * @retval.kSuccess if inserted.
   * @retval kKeyExist if a specified key exists.
   */
  auto
  Insert(  //
      const Key &key,
      const Payload &payload,
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_->CreateEpochGuard();

    while (true) {
      Node_t *node = SearchLeafNode(key, true);
      const auto &rc = node->Insert(key, key_length, payload, payload_length);

      switch (rc) {
        case NodeRC::kSuccess:
          return ReturnCode::kSuccess;
        case NodeRC::kKeyExist:
          return ReturnCode::kKeyExist;
        case NodeRC::kNeedConsolidation:
          Consolidate(node, key);
        default:
          break;
      }
    }
  }

  /**
   * @brief Update a target kay with a specified payload.
   *
   * This function performs a uniqueness check in its processing. If a specified key
   * exist, this function update a target payload. If a specified key does not exist in
   * the index, this function does nothing and returns kKeyNotExist as a return code.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_length the length of a target key.
   * @param payload_length the length of a target payload.
   * @retval kSuccess if updated.
   * @retval kKeyNotExist if a specified key does not exist.
   */
  auto
  Update(  //
      const Key &key,
      const Payload &payload,
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_->CreateEpochGuard();

    while (true) {
      Node_t *node = SearchLeafNode(key, true);
      const auto &rc = node->Update(key, key_length, payload, payload_length);

      switch (rc) {
        case NodeRC::kSuccess:
          return ReturnCode::kSuccess;
        case NodeRC::kKeyNotExist:
          return ReturnCode::kKeyNotExist;
        case NodeRC::kNeedConsolidation:
          Consolidate(node, key);
        default:
          break;
      }
    }
  }

  /**
   * @brief Delete a target kay from the index.
   *
   * This function performs a uniqueness check in its processing. If a specified key
   * exist, this function deletes it. If a specified key does not exist in the index,
   * this function does nothing and returns kKeyNotExist as a return code.
   *
   * Note that if a target key is binary data, it is required to specify its length in
   * bytes.
   *
   * @param key a target key to be written.
   * @param key_length the length of a target key.
   * @retval kSuccess if deleted.
   * @retval kKeyNotExist if a specified key does not exist.
   */
  auto
  Delete(  //
      const Key &key,
      const size_t key_length = sizeof(Key))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_->CreateEpochGuard();

    while (true) {
      Node_t *node = SearchLeafNode(key, true);
      const auto &rc = node->template Delete<Payload>(key, key_length);

      switch (rc) {
        case NodeRC::kSuccess:
          return ReturnCode::kSuccess;
        case NodeRC::kKeyNotExist:
          return ReturnCode::kKeyNotExist;
        case NodeRC::kNeedConsolidation:
          Consolidate(node, key);
        default:
          break;
      }
    }
  }

 private:
  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  template <class T>
  [[nodiscard]] auto
  CreateNewNode()  //
      -> Node_t *
  {
    constexpr size_t kBlockSize = kPageSize - component::GetInitialOffset<Key, T>();
    constexpr bool kIsLeaf = std::is_same_v<T, Payload>;

    auto *page = gc_->template GetPageIfPossible<Node_t>();
    return (page == nullptr) ? new Node_t{kIsLeaf, kBlockSize}
                             : new (page) Node_t{kIsLeaf, kBlockSize};
  }

  /**
   * @return Node_t*: a current root node.
   */
  [[nodiscard]] auto
  GetRoot() const  //
      -> Node_t *
  {
    auto &&root = MwCASDescriptor::Read<Node_t *>(&root_);
    std::atomic_thread_fence(std::memory_order_acquire);

    return root;
  }

  /**
   * @brief Search a leaf node with a specified key.
   *
   * @param key a target key.
   * @param range_is_closed a flag to indicate whether a key is included.
   * @return Node_t*: a leaf node that may contain a target key.
   */
  [[nodiscard]] auto
  SearchLeafNode(  //
      const Key &key,
      const bool range_is_closed) const  //
      -> Node_t *
  {
    Node_t *current_node = GetRoot();
    while (!current_node->IsLeaf()) {
      const auto &pos = current_node->Search(key, range_is_closed);
      current_node = current_node->GetChild(pos);
    }

    return current_node;
  }

  /**
   * @return Node_t*: a leaf node on the far left.
   */
  [[nodiscard]] auto
  SearchLeftEdgeLeaf() const  //
      -> Node_t *
  {
    Node_t *current_node = GetRoot();
    while (!current_node->IsLeaf()) {
      current_node = current_node->GetChild(0);
    }

    return current_node;
  }

  /**
   * @brief Trace a target node and extract intermidiate nodes.
   *
   * Note that traced nodes may not have a target node because concurrent SMOs may remove it.
   *
   * @param key a target key.
   * @param target_node a target node.
   * @return NodeStack: a stack of nodes.
   */
  auto
  TraceTargetNode(  //
      const Key &key,
      const Node_t *target_node) const  //
      -> NodeStack
  {
    // trace nodes to a target internal node
    NodeStack trace;
    size_t index = 0;
    Node_t *current_node = GetRoot();
    while (current_node != target_node && !current_node->IsLeaf()) {
      trace.emplace_back(current_node, index);
      index = current_node->Search(key, true);
      current_node = current_node->GetChild(index);
    }
    trace.emplace_back(current_node, index);

    return trace;
  }

  /*################################################################################################
   * Internal structure modification functoins
   *##############################################################################################*/

  /**
   * @brief Consolidate a target leaf node.
   *
   * Note that this function may call split/merge functions if needed.
   *
   * @param node a target leaf node.
   * @param key a target key.
   */
  void
  Consolidate(  //
      Node_t *node,
      const Key &key)
  {
    // freeze a target node and perform consolidation
    if (node->Freeze() != NodeRC::kSuccess) return;

    // create a consolidated node
    Node_t *consol_node = CreateNewNode<Payload>();
    consol_node->template Consolidate<Payload>(node);

    // check whether splitting/merging is needed
    const auto &stat = consol_node->GetStatusWord();
    if (stat.NeedSplit()) {
      // invoke splitting
      Split<Payload>(consol_node, key);
    } else if (!stat.NeedMerge() || !Merge<Payload>(consol_node, key)) {  // try merging
      // install the consolidated node
      auto &&trace = TraceTargetNode(key, node);
      InstallNewNode(trace, consol_node, key, node);
    }

    gc_->AddGarbage(node);
  }

  /**
   * @brief Split a target node.
   *
   * Note that this function may call a split function for internal nodes if needed.
   *
   * @param node a target node.
   * @param key a target key.
   */
  template <class T>
  void
  Split(  //
      Node_t *node,
      const Key &key)
  {
    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    NodeStack trace;
    Node_t *old_parent = nullptr;
    size_t target_pos{};
    bool root_split = false;
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(key, node);
      if (trace.size() <= 1) {
        root_split = true;
        break;
      }
      target_pos = trace.back().second;
      trace.pop_back();

      // pre-freezing of SMO targets
      old_parent = trace.back().first;
      if (old_parent->Freeze() == NodeRC::kSuccess) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    // create split nodes and its parent node
    Node_t *l_node = CreateNewNode<T>();
    Node_t *r_node = CreateNewNode<T>();
    node->template Split<T>(l_node, r_node);

    // create a new root/parent node
    bool recurse_split = false;
    Node_t *new_parent = CreateNewNode<Node_t *>();
    if (root_split) {
      new_parent->InitAsRoot(l_node, r_node);
    } else {
      recurse_split = new_parent->InitAsSplitParent(old_parent, l_node, r_node, target_pos);
    }

    // install new nodes to the index and register garbages
    InstallNewNode(trace, new_parent, key, old_parent);
    gc_->AddGarbage(node);
    if (!root_split) {
      gc_->AddGarbage(old_parent);
    }

    // split the new parent node if needed
    if (recurse_split) {
      Split<Node_t *>(new_parent, key);
    }
  }

  /**
   * @brief Perform left-merge for a target node.
   *
   * Note that this function may call itself recursively if needed.
   *
   * @param right_node a target node.
   * @param key a target key.
   * @retval true if merging succeeds
   * @retval false otherwise
   */
  template <class T>
  auto
  Merge(  //
      Node_t *right_node,
      const Key &key)  //
      -> bool
  {
    const auto &r_stat = right_node->GetStatusWord();

    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    NodeStack trace;
    Node_t *old_parent{};
    Node_t *left_node{};
    size_t target_pos{};
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(key, right_node);
      target_pos = trace.back().second;
      if (target_pos <= 0) return false;  // there is no mergeable node

      // check a parent node is live
      trace.pop_back();
      old_parent = trace.back().first;
      const auto &p_status = old_parent->GetStatusWord();
      if (p_status.IsFrozen()) continue;

      // check a right sibling node is live and has sufficent capacity
      left_node = old_parent->GetChild(target_pos - 1);
      const auto &l_stat = left_node->GetStatusWord();
      if (!l_stat.CanMergeWith(r_stat)) return false;
      if (l_stat.IsFrozen()) continue;

      // pre-freezing of SMO targets
      MwCASDescriptor desc{};
      old_parent->SetStatusForMwCAS(desc, p_status, p_status.Freeze());
      left_node->SetStatusForMwCAS(desc, l_stat, l_stat.Freeze());
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    // create new nodes
    Node_t *merged_node = CreateNewNode<T>();
    merged_node->template Merge<T>(left_node, right_node);
    Node_t *new_parent = CreateNewNode<Node_t *>();
    bool recurse_merge = new_parent->InitAsMergeParent(old_parent, merged_node, target_pos - 1);
    if (trace.size() <= 1 && new_parent->GetSortedCount() == 1) {
      // the new root node has only one child, use the merged child as a new root
      gc_->AddGarbage(new_parent);
      new_parent = merged_node;
    }

    // install new nodes to the index and register garbages
    InstallNewNode(trace, new_parent, key, old_parent);
    gc_->AddGarbage(old_parent);
    gc_->AddGarbage(left_node);
    gc_->AddGarbage(right_node);

    // merge the new parent node if needed
    if (recurse_merge && !Merge<Node_t *>(new_parent, key)) {
      // if the parent node cannot be merged, unfreeze it
      new_parent->Unfreeze();
    }

    return true;
  }

  /**
   * @brief Install a new node by using MwCAS.
   *
   * Note that this function may do nothing if a target node has been already modified.
   *
   * @param trace the stack of nodes up to a target node.
   * @param new_node a new node to be installed.
   * @param key a target key.
   * @param target_node an old node to be swapped.
   */
  void
  InstallNewNode(  //
      NodeStack &trace,
      Node_t *new_node,
      const Key &key,
      const Node_t *target_node)
  {
    if (trace.size() <= 1) {
      // root swapping
      root_.store(new_node, std::memory_order_release);
      return;
    }

    std::atomic_thread_fence(std::memory_order_release);
    while (true) {
      // prepare installing nodes
      auto &&[old_node, target_pos] = trace.back();
      trace.pop_back();
      Node_t *parent = trace.back().first;

      // check wether related nodes are frozen
      const auto &parent_status = parent->GetStatusWordProtected();
      if (!parent_status.IsFrozen()) {
        // install a new internal node by MwCAS
        MwCASDescriptor desc{};
        parent->SetStatusForMwCAS(desc, parent_status, parent_status);
        parent->SetChildForMwCAS(desc, target_pos, old_node, new_node);
        if (desc.MwCAS()) return;
      }

      // traverse again to get a modified parent
      trace = TraceTargetNode(key, target_node);
    }
  }

  /**
   * @brief Delete children nodes recursively.
   *
   * Note that this function assumes that there are no other threads in operation.
   *
   * @param node a target node.
   */
  static void
  DeleteChildren(Node_t *node)
  {
    if (!node->IsLeaf()) {
      // delete children nodes recursively
      for (size_t i = 0; i < node->GetSortedCount(); ++i) {
        Node_t *child_node = node->GetChild(i);
        DeleteChildren(child_node);
      }
    }

    delete node;
  }

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a root node of BzTree
  std::atomic<Node_t *> root_{nullptr};

  /// garbage collector
  std::unique_ptr<NodeGC_t> gc_{nullptr};
};

}  // namespace dbgroup::index::bztree

#endif  // BZTREE_BZTREE_HPP
