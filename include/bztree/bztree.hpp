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

#include "component/internal_node_api.hpp"
#include "component/leaf_node_api.hpp"
#include "component/record_iterator.hpp"
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
  using RecordPage_t = component::RecordPage<Key, Payload>;
  using RecordIterator_t = component::RecordIterator<Key, Payload, Compare>;
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
  const size_t index_epoch_;

  /// a root node of BzTree
  Node_t *root_;

  /// garbage collector
  NodeGC_t gc_;

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

  auto
  GetChild(  //
      const Node_t *node,
      const size_t position)  //
      -> Node_t *
  {
    const auto meta = node->GetMetadata(position);
    return MwCASDescriptor::Read<Node_t *>(node->GetPayloadAddr(meta));
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
    auto current_node = GetRoot();
    do {
      const auto index = internal::SearchChildNode(current_node, key, range_is_closed);
      current_node = internal::GetChildNode(current_node, index);
    } while (!current_node->IsLeaf());

    return current_node;
  }

  /**
   * @return Node_t*: a leaf node on the far left.
   */
  Node_t *
  SearchLeftEdgeLeaf()
  {
    auto current_node = GetRoot();
    do {
      current_node = internal::GetChildNode(current_node, 0);
    } while (!current_node->IsLeaf());

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
    auto current_node = GetRoot();
    while (current_node != target_node && !current_node->IsLeaf()) {
      trace.emplace_back(current_node, index);
      index = internal::SearchChildNode(current_node, key, true);
      current_node = internal::GetChildNode(current_node, index);
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
  ConsolidateLeafNode(  //
      Node_t *node,
      const Key &key,
      const size_t key_length)
  {
    // freeze a target node and perform consolidation
    if (node->Freeze() != NodeReturnCode::kSuccess) return;

    // create a consolidated node
    auto *consolidated_node = CreateNewNode(kLeafFlag);
    leaf::Consolidate(consolidated_node, node);
    gc_.AddGarbage(node);

    // check whether splitting/merging is needed
    if (consolidated_node->GetFreeSpaceSize() < kMinFreeSpaceSize) {
      Split(consolidated_node, key);
      return;
    } else if (consolidated_node->GetSortedCount() < kMinSortedRecNum) {
      if (MergeLeafNodes(node, key, key_length, target_size, metadata, rec_count)) return;
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
    Node_t *old_parent;
    size_t target_pos;
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(key, node);
      target_pos = trace.back().second;
      trace.pop_back();

      if (trace.empty()) {
        // if the target node is a root, insert a new root node
        old_parent = node;
        break;
      }

      // pre-freezing of SMO targets
      old_parent = trace.back().first;
      if (old_parent->Freeze() == NodeReturnCode::kSuccess) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    // create split nodes and its parent node
    const auto is_leaf = node->IsLeaf();
    Node_t *left_node = (is_leaf) ? node : CreateNewNode(!kLeafFlag);
    Node_t *right_node = CreateNewNode(is_leaf);
    if (is_leaf) {
      InnerSplit<Payload>(node, left_node, right_node);
    } else {
      InnerSplit<Node_t *>(node, left_node, right_node);
    }
    Node_t *new_parent = CreateNewNode(!kLeafFlag);
    if (trace.empty()) {  // create a new root node
      new_parent->SetSortedCount(1);
      new_parent->SetRightEndFlag(true);
      CreateSplitParent(new_parent, new_parent, left_node, right_node, target_pos);
    } else {
      CreateSplitParent(old_parent, new_parent, left_node, right_node, target_pos);
    }

    // check the parent node has sufficent capacity
    const auto parent_need_split = new_parent->GetFreeSpaceSize() < kMinFreeSpaceSize;
    if (parent_need_split) {
      new_parent->Freeze();  // pre-freeze for recursive splitting
    }

    // install new nodes
    InstallNewNode(trace, new_parent, key, old_parent);

    // register frozen nodes with garbage collection
    gc_.AddGarbage(old_parent);
    if (!is_leaf) {
      gc_.AddGarbage(node);
    }

    // split the new parent node if needed
    if (parent_need_split) {
      Split(new_parent, key);
    }
  }

  template <class T>
  void
  InnerSplit(  //
      Node_t *node,
      Node_t *l_node,
      Node_t *r_node)
  {
    // set a right-end flag if needed
    if (!node->HasNext()) {
      l_node->SetRightEndFlag(false);
      r_node->SetRightEndFlag(true);
    }

    // copy records to a left node
    const auto rec_count = node->GetSortedCount();
    const size_t l_count = rec_count / 2;
    size_t l_offset;
    if constexpr (std::is_same_v<T, Node_t *>) {
      l_offset = l_node->CopyRecordsFrom<T>(node, 0, l_count, 0, kPageSize);
    } else {  // when splitting a leaf node, only update its offset
      l_offset = l_node->GetMetadata(l_count - 1).GetOffset();
    }
    l_node->SetSortedCount(l_count);
    l_node->SetStatus(StatusWord{l_count, kPageSize - l_offset});

    // copy records to a right node
    const auto r_offset = r_node->CopyRecordsFrom<T>(node, l_count, rec_count, 0, kPageSize);
    const auto r_count = rec_count - l_count;
    r_node->SetSortedCount(r_count);
    r_node->SetStatus(StatusWord{r_count, kPageSize - r_offset});
  }

  void
  CreateSplitParent(  //
      const Node_t *old_node,
      Node_t *new_node,
      const Node_t *l_child,
      const Node_t *r_child,
      const size_t l_pos)  //
  {
    // set a right-end flag if needed
    if (!old_node->HasNext()) {
      new_node->SetRightEndFlag(true);
    }

    // copy lower records
    auto offset = new_node->CopyRecordsFrom<Node_t *>(old_node, 0, l_pos, 0, kPageSize);

    // insert split nodes
    const auto l_meta = l_child->GetMetadata(l_child->GetSortedCount() - 1);
    offset = new_node->InsertChild(l_child, l_meta, l_child, l_pos, offset);
    const auto r_meta = old_node->GetMetadata(l_pos);
    const auto r_pos = l_pos + 1;
    offset = new_node->InsertChild(old_node, r_meta, r_child, r_pos, offset);

    // copy upper records
    auto rec_count = old_node->GetSortedCount();
    offset = new_node->CopyRecordsFrom<Node_t *>(old_node, r_pos, rec_count, r_pos + 1, offset);

    // set an updated header
    new_node->SetSortedCount(++rec_count);
    new_node->SetStatus(StatusWord{rec_count, kPageSize - offset});
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
      Node_t *node,
      const Key &key)  //
      -> bool
  {
    const auto l_size = node->GetUsedSize();

    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    NodeStack trace;
    Node_t *old_parent;
    Node_t *right_node;
    size_t target_pos;
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(key, node);
      target_pos = trace.back().second;
      trace.pop_back();

      // check a parent node is live and has a right child node
      old_parent = trace.back().first;
      const auto p_count = old_parent->GetSortedCount();
      if (target_pos >= p_count - 1) return false;  // there is no mergeable node
      const auto p_status = old_parent->GetStatusWordProtected();
      if (p_status.IsFrozen()) continue;

      // check a right sibling node is live and has sufficent capacity
      right_node = GetChild(old_parent, target_pos + 1);
      const auto r_size = right_node->GetUsedSize();
      if (l_size + r_size > kMaxMergedSize - component::kHeaderLength) return false;
      const auto r_status = right_node->GetStatusWordProtected();
      if (r_status.IsFrozen()) continue;

      // pre-freezing of SMO targets
      MwCASDescriptor desc{};
      old_parent->SetStatusForMwCAS(desc, p_status, p_status.Freeze());
      right_node->SetStatusForMwCAS(desc, r_status, r_status.Freeze());
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    // create new nodes
    const auto is_leaf = node->IsLeaf();
    Node_t *merged_node = (is_leaf) ? node : CreateNewNode(!kLeafFlag);
    if (is_leaf) {
      InnerMerge<Payload>(node, right_node, merged_node);
    } else {
      InnerMerge<Node_t *>(node, right_node, merged_node);
    }
    Node_t *new_parent = CreateMergeParent(old_parent, merged_node, target_pos);

    // check the parent node should be merged
    const auto parent_need_merge = new_parent->GetSortedCount() < kMinSortedRecNum;
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
    gc_.AddGarbage(right_node);
    if (!is_leaf) {
      gc_.AddGarbage(node);
    }

    // merge the new parent node if needed
    if (parent_need_merge && (trace.empty() || !Merge(new_parent, key))) {
      // if the parent node cannot be merged, unfreeze it
      new_parent->SetStatus(new_parent->GetStatusWord().Unfreeze());
    }

    return true;
  }

  template <class T>
  void
  InnerMerge(  //
      Node_t *l_node,
      Node_t *r_node,
      Node_t *merged_node)
  {
    // set a right-end flag if needed
    if (!r_node->HasNext()) {
      merged_node->SetRightEndFlag();
    }

    // copy records in left/right nodes
    const auto l_count = l_node->GetSortedCount();
    size_t offset;
    if constexpr (std::is_same_v<T, Node_t *>) {
      offset = merged_node->CopyRecordsFrom<T>(l_node, 0, l_count, 0, kPageSize);
    } else {  // when merging a leaf node, only update its offset
      offset = merged_node->GetMetadata(l_count - 1).GetOffset();
    }
    const auto r_count = r_node->GetSortedCount();
    offset = merged_node->CopyRecordsFrom<T>(r_node, 0, r_count, l_count, offset);

    // create a merged node
    const auto rec_count = l_count + r_count;
    merged_node->SetSortedCount(rec_count);
    merged_node->SetStatus(StatusWord{rec_count, kPageSize - offset});
  }

  auto
  CreateMergeParent(  //
      const Node_t *old_node,
      const Node_t *merged_child,
      const size_t position)  //
      -> Node_t *
  {
    Node_t *new_node = CreateNewNode(!kLeafFlag);

    // set a right-end flag if needed
    if (!old_node->HasNext()) {
      new_node->SetRightEndFlag(true);
    }

    // copy lower records
    auto offset = new_node->CopyRecordsFrom<Node_t *>(old_node, 0, position, 0, kPageSize);

    // insert a merged node
    const auto r_pos = position + 1;
    const auto meta = old_node->GetMetadata(r_pos);
    offset = new_node->InsertChild(old_node, meta, merged_child, position, offset);

    // copy upper records
    auto rec_count = old_node->GetSortedCount();
    offset = new_node->CopyRecordsFrom<Node_t *>(old_node, r_pos + 1, rec_count, r_pos, offset);

    // set an updated header
    new_node->SetSortedCount(--rec_count);
    new_node->SetStatus(StatusWord{rec_count, kPageSize - offset});

    return new_node;
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
        auto parent = trace.back().first;

        // check wether related nodes are frozen
        const auto parent_status = parent->GetStatusWordProtected();
        if (parent_status.IsFrozen()) {
          trace = TraceTargetNode(key, target_node);
          continue;
        }

        // install a new internal node by PMwCAS
        parent->SetStatusForMwCAS(desc, parent_status, parent_status);
        parent->SetPayloadForMwCAS(desc, parent->GetMetadata(target_pos), old_node, new_node);
      } else {
        /*------------------------------------------------------------------------------------------
         * Swapping a new root node
         *----------------------------------------------------------------------------------------*/

        const auto old_node = trace.back().first;
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
        auto child_node = internal::GetChildNode(node, i);
        DeleteChildren(child_node);
      }
    }

    delete node;
  }

 public:
  /*################################################################################################
   * Public constructor/destructor
   *##############################################################################################*/

  /**
   * @brief Construct a new BzTree object.
   *
   * @param gc_interval_microsec GC internal [us]
   */
  explicit BzTree(const size_t gc_interval_microsec = 100000)
      : index_epoch_{1},
        root_{internal::CreateInitialRoot<Key, Payload, Compare>()},
        gc_{gc_interval_microsec}
  {
    gc_.StartGC();
  }

  /**
   * @brief Destroy the BzTree object.
   *
   */
  ~BzTree() { DeleteChildren(GetRoot()); }

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

    const auto node = SearchLeafNode(key, true);

    Payload payload{};
    const auto rc = leaf::Read(node, key, payload);
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

  // /**
  //  * @brief Perform a range scan with specified keys.
  //  *
  //  * If a begin/end key is nullptr, it is treated as negative or positive infinite.
  //  *
  //  * @param begin_key the pointer of a begin key of a range scan.
  //  * @param begin_closed a flag to indicate whether the begin side of a range is closed.
  //  * @param end_key the pointer of an end key of a range scan.
  //  * @param end_closed a flag to indicate whether the end side of a range is closed.
  //  * @param page a page to copy target keys/payloads. This argument is used internally.
  //  * @return RecordIterator_t: an iterator to access target records.
  //  */
  // RecordIterator_t
  // Scan(  //
  //     const Key *begin_key = nullptr,
  //     const bool begin_closed = false,
  //     const Key *end_key = nullptr,
  //     const bool end_closed = false,
  //     RecordPage_t *page = nullptr)
  // {
  //   if (page == nullptr) {
  //     page = new RecordPage_t{};
  //   }

  //   const auto guard = gc_.CreateEpochGuard();

  //   const auto node =
  //       (begin_key == nullptr) ? SearchLeftEdgeLeaf() : SearchLeafNode(*begin_key, begin_closed);
  //   const auto scan_finished = leaf::Scan(node, begin_key, begin_closed, end_key, end_closed,
  //   page);

  //   return RecordIterator_t{this, end_key, end_closed, page, scan_finished};
  // }

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
      auto node = SearchLeafNode(key, true);
      const auto rc = leaf::Write(node, key, key_length, payload, payload_length, index_epoch_);

      if (rc == NodeReturnCode::kSuccess) {
        break;
      } else if (rc == NodeReturnCode::kNeedConsolidation) {
        ConsolidateLeafNode(node, key, key_length);
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
      auto node = SearchLeafNode(key, true);
      const auto rc = leaf::Insert(node, key, key_length, payload, payload_length, index_epoch_);

      if (rc == NodeReturnCode::kSuccess || rc == NodeReturnCode::kKeyExist) {
        if (rc == NodeReturnCode::kKeyExist) return ReturnCode::kKeyExist;
        break;
      } else if (rc == NodeReturnCode::kNeedConsolidation) {
        ConsolidateLeafNode(node, key, key_length);
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
      auto node = SearchLeafNode(key, true);
      const auto rc = leaf::Update(node, key, key_length, payload, payload_length, index_epoch_);

      if (rc == NodeReturnCode::kSuccess || rc == NodeReturnCode::kKeyNotExist) {
        if (rc == NodeReturnCode::kKeyNotExist) return ReturnCode::kKeyNotExist;
        break;
      } else if (rc == NodeReturnCode::kNeedConsolidation) {
        ConsolidateLeafNode(node, key, key_length);
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
      auto node = SearchLeafNode(key, true);
      const auto rc = leaf::Delete(node, key, key_length);

      if (rc == NodeReturnCode::kSuccess || rc == NodeReturnCode::kKeyNotExist) {
        if (rc == NodeReturnCode::kKeyNotExist) return ReturnCode::kKeyNotExist;
        break;
      } else if (rc == NodeReturnCode::kNeedConsolidation) {
        ConsolidateLeafNode(node, key, key_length);
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
        auto child_node = internal::GetChildNode(node, i);
        std::tie(internal_count, leaf_count) = CountNodes(child_node, internal_count, leaf_count);
      }
      return {internal_count + 1, leaf_count};
    }
    return {internal_count, leaf_count + 1};
  }
};

}  // namespace dbgroup::index::bztree
