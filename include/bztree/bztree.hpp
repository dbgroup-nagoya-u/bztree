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

#pragma once

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
  using Node_t = component::Node<Key, Payload, Compare>;
  using MetaArray = std::array<Metadata, Node_t::kMaxRecordNum>;
  using NodeReturnCode = component::NodeReturnCode;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<Node_t>;
  using NodeStack = std::vector<std::pair<Node_t *, size_t>>;
  using MwCASDescriptor = component::MwCASDescriptor;
  using Binary_t = std::remove_pointer_t<Payload>;
  using Binary_p = std::unique_ptr<Binary_t, component::PayloadDeleter<Binary_t>>;

 private:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr bool kLeafFlag = true;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// an epoch to count the number of failure
  const size_t index_epoch_ = 1;

  /// a root node of BzTree
  std::atomic<Node_t *> root_ = nullptr;

  /// garbage collector
  NodeGC_t gc_ = 100000;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  auto
  CreateNewNode(const bool is_leaf)  //
      -> Node_t *
  {
    auto *page = gc_.template GetPageIfPossible<Node_t>();
    return (page == nullptr) ? new Node_t{is_leaf} : new (page) Node_t{is_leaf};
  }

  /**
   * @return Node_t*: a current root node.
   */
  Node_t *
  GetRoot()
  {
    return MwCASDescriptor::Read<Node_t *>(&root_);
  }

  /**
   * @brief Search a leaf node with a specified key.
   *
   * @param key a target key.
   * @param range_is_closed a flag to indicate whether a key is included.
   * @return Node_t*: a leaf node that may contain a target key.
   */
  Node_t *
  SearchLeafNode(  //
      const Key &key,
      const bool range_is_closed)
  {
    Node_t *current_node = GetRoot();
    while (!current_node->IsLeaf()) {
      const auto index = current_node->Search(key, range_is_closed);
      current_node = current_node->GetChild(index);
    }

    return current_node;
  }

  /**
   * @return Node_t*: a leaf node on the far left.
   */
  Node_t *
  SearchLeftEdgeLeaf()
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
  NodeStack
  TraceTargetNode(  //
      const Key &key,
      const Node_t *target_node)
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
   * @param key_length the length of a target key.
   */
  void
  Consolidate(  //
      Node_t *node,
      const Key &key)
  {
    // freeze a target node and perform consolidation
    if (node->Freeze() != NodeReturnCode::kSuccess) return;

    // create a consolidated node
    Node_t *consolidated_node = CreateNewNode(kLeafFlag);
    consolidated_node->Consolidate(node);
    gc_.AddGarbage(node);

    // check whether splitting/merging is needed
    const auto stat = consolidated_node->GetStatusWordProtected();
    if (stat.NeedSplit()) {
      Split(consolidated_node, key);
      return;
    } else if (stat.NeedMerge()) {
      if (Merge(consolidated_node, key)) return;
    }

    // install a new node
    auto trace = TraceTargetNode(key, node);
    InstallNewNode(trace, consolidated_node, key, node);
  }

  /**
   * @brief Split a target leaf node.
   *
   * Note that this function may call a split function for internal nodes if needed.
   *
   * @param node a target leaf node.
   * @param key a target key.
   * @param metadata an array of consolidated metadata.
   * @param rec_count the number of metadata.
   */
  void
  Split(  //
      Node_t *node,
      const Key &key)
  {
    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    NodeStack trace;
    Node_t *old_parent{};
    size_t target_pos{};
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(key, node);
      target_pos = trace.back().second;
      trace.pop_back();

      if (trace.empty()) break;

      // pre-freezing of SMO targets
      old_parent = trace.back().first;
      if (old_parent->Freeze() == NodeReturnCode::kSuccess) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    // create split nodes and its parent node
    const auto is_leaf = node->IsLeaf();
    Node_t *left_node = CreateNewNode(is_leaf);
    Node_t *right_node = CreateNewNode(is_leaf);
    if (is_leaf) {
      Node_t::template Split<Payload>(node, left_node, right_node);
    } else {
      Node_t::template Split<Node_t *>(node, left_node, right_node);
    }
    Node_t *new_parent = CreateNewNode(!kLeafFlag);
    if (trace.empty()) {
      // create a new root node
      new_parent->InitAsRoot(left_node, right_node);
      root_.store(new_parent, std::memory_order_relaxed);
    } else {
      // create a new parent node
      new_parent->InitAsSplitParent(old_parent, left_node, right_node, target_pos);

      // check the parent node has sufficent capacity
      const auto p_stat = new_parent->GetStatusWordProtected();
      const auto parent_need_split = p_stat.NeedSplit();
      if (parent_need_split) {
        new_parent->Freeze();  // pre-freeze for recursive splitting
      }

      // install new nodes to the index
      InstallNewNode(trace, new_parent, key, old_parent);

      // split the new parent node if needed
      if (parent_need_split) {
        Split(new_parent, key);
      }

      // register frozen nodes with garbage collection
      gc_.AddGarbage(old_parent);
    }
    gc_.AddGarbage(node);
  }

  /**
   * @brief Merge a target internal node.
   *
   * Note that this function may call itself recursively if needed.
   *
   * @param node a target internal node.
   * @param key a target key.
   * @param key_length the length of a target key.
   */
  auto
  Merge(  //
      Node_t *right_node,
      const Key &key)  //
      -> bool
  {
    const auto r_stat = right_node->GetStatusWordProtected();

    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    NodeStack trace;
    Node_t *old_parent;
    Node_t *left_node;
    size_t target_pos;
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(key, right_node);
      target_pos = trace.back().second;
      if (target_pos <= 0) return false;  // there is no mergeable node

      // check a parent node is live
      trace.pop_back();
      old_parent = trace.back().first;
      const auto p_status = old_parent->GetStatusWordProtected();
      if (p_status.IsFrozen()) continue;

      // check a right sibling node is live and has sufficent capacity
      left_node = old_parent->GetChild(target_pos - 1);
      const auto l_stat = left_node->GetStatusWordProtected();
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
    const auto is_leaf = right_node->IsLeaf();
    Node_t *merged_node = CreateNewNode(is_leaf);
    if (is_leaf) {
      merged_node->Consolidate(left_node);
      Node_t::template Merge<Payload>(merged_node, right_node, merged_node);
    } else {
      Node_t::template Merge<Node_t *>(left_node, right_node, merged_node);
    }
    Node_t *new_parent = CreateNewNode(!kLeafFlag);
    new_parent->InitAsMergeParent(old_parent, merged_node, target_pos - 1);

    // check the parent node should be merged
    const auto parent_need_merge = new_parent->GetStatusWordProtected().NeedMerge();
    if (parent_need_merge) {
      if (trace.size() > 1 || new_parent->GetSortedCount() > 1) {
        new_parent->Freeze();  // pre-freeze for recursive merging
      } else {
        // the new root node has only one child, use the merged child as a new root
        gc_.AddGarbage(new_parent);
        new_parent = merged_node;
      }
    }

    // install new nodes
    InstallNewNode(trace, new_parent, key, old_parent);

    // register frozen nodes with garbage collection
    gc_.AddGarbage(old_parent);
    gc_.AddGarbage(left_node);
    gc_.AddGarbage(right_node);

    // merge the new parent node if needed
    if (parent_need_merge && !Merge(new_parent, key)) {
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
    while (true) {
      MwCASDescriptor desc;
      if (trace.size() > 1) {
        /*------------------------------------------------------------------------------------------
         * Swapping a new internal node
         *----------------------------------------------------------------------------------------*/

        // prepare installing nodes
        auto [old_node, target_pos] = trace.back();
        if (old_node != target_node) return;
        trace.pop_back();
        Node_t *parent = trace.back().first;

        // check wether related nodes are frozen
        const auto parent_status = parent->GetStatusWordProtected();
        if (parent_status.IsFrozen()) {
          trace = TraceTargetNode(key, target_node);
          continue;
        }

        // install a new internal node by PMwCAS
        parent->SetStatusForMwCAS(desc, parent_status, parent_status);
        parent->SetChildForMwCAS(desc, target_pos, old_node, new_node);
      } else {
        /*------------------------------------------------------------------------------------------
         * Swapping a new root node
         *----------------------------------------------------------------------------------------*/

        Node_t *old_node = trace.back().first;
        if (old_node != target_node) return;
        trace.pop_back();
        desc.AddMwCASTarget(&root_, old_node, new_node);
      }

      if (desc.MwCAS()) return;

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
     * Public constructors/destructors
     *############################################################################################*/

    constexpr RecordIterator(  //
        BzTree_t *bztree,
        Node_t *node,
        size_t current_pos)
        : bztree_{bztree},
          node_{node},
          record_count_{node->GetSortedCount()},
          current_pos_{current_pos}
    {
    }

    ~RecordIterator() = default;

    RecordIterator(const RecordIterator &) = delete;
    RecordIterator &operator=(const RecordIterator &) = delete;
    constexpr RecordIterator(RecordIterator &&) = default;
    constexpr RecordIterator &operator=(RecordIterator &&) = default;

    /*##############################################################################################
     * Public operators for iterators
     *############################################################################################*/

    /**
     * @return std::pair<Key, Payload>: a current key and payload pair
     */
    constexpr std::pair<Key, Payload>
    operator*() const
    {
      return {GetKey(), GetPayload()};
    }

    /**
     * @brief Forward an iterator.
     *
     */
    void
    operator++()
    {
      ++current_pos_;
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

      current_pos_ = record_count_ - 1;
      *this = bztree_->Scan(GetKey, false, node_);

      return HasNext();
    }

    /**
     * @return Key: a key of a current record
     */
    constexpr auto
    GetKey() const  //
        -> Key
    {
      return node_->GetKey(current_pos_);
    }

    /**
     * @return Payload: a payload of a current record
     */
    constexpr Payload
    GetPayload() const
    {
      return node_->GetPayload(current_pos_);
    }

   private:
    /*##############################################################################################
     * Internal member variables
     *############################################################################################*/

    /// a pointer to BwTree to perform continuous scan
    BzTree_t *bztree_;

    /// node
    Node_t *node_;

    /// the number of records in this node.
    size_t record_count_;

    /// an index of a current record
    size_t current_pos_;
  };

  /*################################################################################################
   * Public constructor/destructor
   *##############################################################################################*/

  /**
   * @brief Construct a new BzTree object.
   *
   * @param gc_interval_microsec GC internal [us]
   */
  explicit BzTree(const size_t gc_interval_microsec = 100000) : gc_{gc_interval_microsec}
  {
    // create an initial root node
    Node_t *leaf = CreateNewNode(kLeafFlag);
    root_.store(leaf, std::memory_order_relaxed);

    // start GC for nodes
    gc_.StartGC();
  }

  /**
   * @brief Destroy the BzTree object.
   *
   */
  ~BzTree()
  {
    gc_.StopGC();
    DeleteChildren(GetRoot());
  }

  BzTree(const BzTree &) = delete;
  BzTree &operator=(const BzTree &) = delete;
  BzTree(BzTree &&) = delete;
  BzTree &operator=(BzTree &&) = delete;

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
    const auto guard = gc_.CreateEpochGuard();

    const Node_t *node = SearchLeafNode(key, true);

    Payload payload{};
    const auto rc = node->Read(key, payload);
    if (rc == NodeReturnCode::kSuccess) {
      if constexpr (IsVariableLengthData<Payload>()) {
        return std::make_pair(ReturnCode::kSuccess, Binary_p{payload});
      } else {
        return std::make_pair(ReturnCode::kSuccess, std::move(payload));
      }
    }
    if constexpr (IsVariableLengthData<Payload>()) {
      return std::make_pair(ReturnCode::kKeyNotExist, Binary_p{});
    } else {
      return std::make_pair(ReturnCode::kKeyNotExist, Payload{});
    }
  }

  /**
   * @brief Perform a range scan with specified keys.
   *
   * If a begin/end key is nullptr, it is treated as negative or positive infinite.
   *
   * @param begin_key the pointer of a begin key of a range scan.
   * @param begin_closed a flag to indicate whether the begin side of a range is closed.
   * @param page a page to copy target keys/payloads. This argument is used internally.
   * @return an iterator to access target records.
   */
  auto
  Scan(  //
      const Key &begin_key,
      const bool begin_closed = false,
      Node_t *page = nullptr)  //
      -> RecordIterator
  {
    const auto guard = gc_.CreateEpochGuard();

    const Node_t *node = SearchLeafNode(begin_key, begin_closed);
    if (page == nullptr) {
      page = CreateNewNode(kLeafFlag);
    }
    page->Consolidate(node);

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
  ReturnCode
  Write(  //
      const Key &key,
      const Payload &payload,
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))
  {
    const auto guard = gc_.CreateEpochGuard();

    while (true) {
      Node_t *node = SearchLeafNode(key, true);
      const auto rc = node->Write(key, key_length, payload, payload_length);

      if (rc == NodeReturnCode::kSuccess) {
        break;
      } else if (rc == NodeReturnCode::kNeedConsolidation) {
        Consolidate(node, key);
      }
    }
    return ReturnCode::kSuccess;
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
  ReturnCode
  Insert(  //
      const Key &key,
      const Payload &payload,
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))
  {
    const auto guard = gc_.CreateEpochGuard();

    while (true) {
      Node_t *node = SearchLeafNode(key, true);
      const auto rc = node->Insert(key, key_length, payload, payload_length);

      if (rc == NodeReturnCode::kSuccess || rc == NodeReturnCode::kKeyExist) {
        if (rc == NodeReturnCode::kKeyExist) return ReturnCode::kKeyExist;
        break;
      } else if (rc == NodeReturnCode::kNeedConsolidation) {
        Consolidate(node, key);
      }
    }
    return ReturnCode::kSuccess;
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
  ReturnCode
  Update(  //
      const Key &key,
      const Payload &payload,
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))
  {
    const auto guard = gc_.CreateEpochGuard();

    while (true) {
      Node_t *node = SearchLeafNode(key, true);
      const auto rc = node->Update(key, key_length, payload, payload_length);

      if (rc == NodeReturnCode::kSuccess || rc == NodeReturnCode::kKeyNotExist) {
        if (rc == NodeReturnCode::kKeyNotExist) return ReturnCode::kKeyNotExist;
        break;
      } else if (rc == NodeReturnCode::kNeedConsolidation) {
        Consolidate(node, key);
      }
    }
    return ReturnCode::kSuccess;
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
  ReturnCode
  Delete(  //
      const Key &key,
      const size_t key_length = sizeof(Key))
  {
    const auto guard = gc_.CreateEpochGuard();

    while (true) {
      Node_t *node = SearchLeafNode(key, true);
      const auto rc = node->Delete(key, key_length);

      if (rc == NodeReturnCode::kSuccess || rc == NodeReturnCode::kKeyNotExist) {
        if (rc == NodeReturnCode::kKeyNotExist) return ReturnCode::kKeyNotExist;
        break;
      } else if (rc == NodeReturnCode::kNeedConsolidation) {
        Consolidate(node, key);
      }
    }
    return ReturnCode::kSuccess;
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @brief Count the total number of nodes in the index.
   *
   * Note that this function assumes that there are no other threads in operation.
   *
   * @param node a node to begin counting. If nullptr is set, a root node is used.
   * @param internal_count the number of internal nodes.
   * @param leaf_count the number of leaf nodes.
   * @return std::pair<size_t, size_t>: the number of internal/leaf nodes.
   */
  std::pair<size_t, size_t>
  CountNodes(  //
      Node_t *node = nullptr,
      size_t internal_count = 0,
      size_t leaf_count = 0)
  {
    if (node == nullptr) node = GetRoot();

    if (!node->IsLeaf()) {
      // delete children nodes recursively
      for (size_t i = 0; i < node->GetSortedCount(); ++i) {
        Node_t *child_node = node->GetChild(i);
        std::tie(internal_count, leaf_count) = CountNodes(child_node, internal_count, leaf_count);
      }
      return {internal_count + 1, leaf_count};
    }
    return {internal_count, leaf_count + 1};
  }
};

}  // namespace dbgroup::index::bztree
