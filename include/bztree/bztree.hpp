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

  /**
   * @brief Check whether a target node should be split.
   *
   * @param internal_node a target node.
   * @param key_length the length of a target key.
   * @retval true if a target node should be split.
   * @retval false if a target node do not need to be split.
   */
  static constexpr bool
  NeedSplit(  //
      const Node_t *internal_node,
      const size_t key_length)
  {
    return internal_node->GetStatusWordProtected().GetOccupiedSize() + key_length
           > kPageSize - 2 * kWordLength;
  }

  /**
   * @brief Compute the size of a data block of a consolidated node.
   *
   * @param metadata an array of metadata of a consolidated node.
   * @param rec_count the number of metadata.
   * @return constexpr size_t: the size of a data block.
   */
  static constexpr size_t
  ComputeOccupiedSize(  //
      const MetaArray &metadata,
      const size_t rec_count)
  {
    size_t block_size = 0;
    for (size_t i = 0; i < rec_count; ++i) {
      block_size += metadata[i].GetTotalLength();
    }
    block_size += component::kHeaderLength + (kWordLength * rec_count);

    return block_size;
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
        RootSplit(node);
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
    Node_t *new_parent = CreateParent(old_parent, left_node, right_node, target_pos);

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
  RootSplit(Node_t *node)
  {
    // create split nodes and its parent node
    const auto is_leaf = node->IsLeaf();
    Node_t *l_child = (is_leaf) ? node : CreateNewNode(!kLeafFlag);
    Node_t *r_child = CreateNewNode(is_leaf);
    if (is_leaf) {
      InnerSplit<Payload>(node, l_child, r_child);
    } else {
      InnerSplit<Node_t *>(node, l_child, r_child);
    }
    Node_t *new_root = CreateNewNode(!kLeafFlag);
    new_root->SetRightEndFlag(true);

    // insert children nodes
    const auto l_meta = l_child->GetMetadata(l_child->GetSortedCount() - 1);
    auto offset = new_root->InsertChild(l_child, l_meta, l_child, 0, kPageSize);
    const auto r_meta = r_child->GetMetadata(r_child->GetSortedCount() - 1);
    offset = new_root->InsertChild(r_child, r_meta, r_child, 1, offset);

    // set header information
    new_root->SetSortedCount(2);
    new_root->SetStatus(StatusWord{2, kPageSize - offset});

    // install the new root node
    reinterpret_cast<std::atomic<Node_t *> *>(root_)->store(new_root, std::memory_order_release);
  }

  auto
  CreateParent(  //
      const Node_t *old_node,
      const Node_t *l_child,
      const Node_t *r_child,
      const size_t l_pos)  //
      -> Node_t *
  {
    Node_t *new_node = CreateNewNode(!kLeafFlag);

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

    return new_node;
  }

  /**
   * @brief Merge a target leaf node.
   *
   * Note that this function may call a merge function for internal nodes if needed.
   *
   * @param node a target leaf node.
   * @param key a target key.
   * @param key_length the length of a target key.
   * @param target_size the size of a target leaf node.
   * @param metadata an array of consolidated metadata.
   * @param rec_count the number of metadata.
   * @retval true if a target leaf node is merged.
   * @retval false if a target leaf node is not merged.
   */
  bool
  MergeLeafNodes(  //
      const Node_t *node,
      const Key &key,
      const size_t key_length,
      const size_t target_size)
  {
    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    NodeStack trace;
    Node_t *parent = nullptr, *sib_node = nullptr;
    bool sib_is_left = true;
    size_t target_pos = 0;
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(key, node);
      target_pos = trace.back().second;
      trace.pop_back();

      // check a parent node is live
      parent = trace.back().first;
      const auto parent_status = parent->GetStatusWordProtected();
      if (parent_status.IsFrozen()) continue;

      // check a left/right sibling node is live
      std::tie(sib_node, sib_is_left) = GetSiblingNode(parent, target_pos, target_size);
      if (sib_node == nullptr) return false;  // there is no live sibling node
      const auto sibling_status = sib_node->GetStatusWordProtected();
      if (sibling_status.IsFrozen()) {
        if (sib_is_left) continue;
        return false;
      }

      // pre-freezing of SMO targets
      MwCASDescriptor desc;
      parent->SetStatusForMwCAS(desc, parent_status, parent_status.Freeze());
      sib_node->SetStatusForMwCAS(desc, sibling_status, sibling_status.Freeze());
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    // create new nodes
    const auto [sib_meta, sib_rec_count] = leaf::GatherSortedLiveMetadata(sib_node);
    auto *merged_node = CreateNewNode(kLeafFlag);
    size_t deleted_pos;
    if (sib_is_left) {
      leaf::Merge(merged_node, sib_node, sib_meta, sib_rec_count, node, metadata, rec_count);
      deleted_pos = target_pos - 1;
    } else {
      leaf::Merge(merged_node, node, metadata, rec_count, sib_node, sib_meta, sib_rec_count);
      deleted_pos = target_pos;
    }
    auto *new_parent = CreateNewNode(!kLeafFlag);
    internal::NewParentForMerge(new_parent, parent, merged_node, deleted_pos);

    // install new nodes
    InstallNewNode(trace, new_parent, key, parent);

    // register frozen nodes with garbage collection
    gc_.AddGarbage(node);
    gc_.AddGarbage(parent);
    gc_.AddGarbage(sib_node);

    // check whether it is required to merge a new parent node
    if (trace.size() != 0 && new_parent->GetSortedCount() < kMinSortedRecNum) {
      MergeInternalNodes(new_parent, key, key_length);
    }
    return true;
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
  void
  MergeInternalNodes(  //
      Node_t *node,
      const Key &key,
      const size_t key_length)
  {
    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    NodeStack trace;
    Node_t *parent = nullptr, *sib_node = nullptr;
    bool sib_is_left;
    size_t target_pos = 0;
    while (true) {
      // check a target node is not frozen and live
      const auto target_status = node->GetStatusWordProtected();
      if (target_status.IsFrozen()) return;

      // trace and get the embedded index of a target node
      trace = TraceTargetNode(key, node);
      target_pos = trace.back().second;
      trace.pop_back();

      // check a parent node is live
      parent = trace.back().first;
      const auto parent_status = parent->GetStatusWordProtected();
      if (parent_status.IsFrozen()) continue;

      // check a left/right sibling node is live
      const auto target_size = target_status.GetOccupiedSize();
      std::tie(sib_node, sib_is_left) = GetSiblingNode(parent, target_pos, target_size);
      if (sib_node == nullptr) return;
      const auto sibling_status = sib_node->GetStatusWordProtected();
      if (sibling_status.IsFrozen()) continue;

      // pre-freezing of SMO targets
      auto desc = MwCASDescriptor{};
      parent->SetStatusForMwCAS(desc, parent_status, parent_status.Freeze());
      node->SetStatusForMwCAS(desc, target_status, target_status.Freeze());
      sib_node->SetStatusForMwCAS(desc, sibling_status, sibling_status.Freeze());
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    // create new nodes
    auto *merged_node = CreateNewNode(!kLeafFlag);
    size_t deleted_pos;
    if (sib_is_left) {
      internal::Merge(merged_node, sib_node, node);
      deleted_pos = target_pos - 1;
    } else {
      internal::Merge(merged_node, node, sib_node);
      deleted_pos = target_pos;
    }
    auto *new_parent = CreateNewNode(!kLeafFlag);
    internal::NewParentForMerge(new_parent, parent, merged_node, deleted_pos);

    // install new nodes
    InstallNewNode(trace, new_parent, key, parent);

    // register frozen nodes with garbage collection
    gc_.AddGarbage(node);
    gc_.AddGarbage(parent);
    gc_.AddGarbage(sib_node);

    // check whether it is required to merge a parent node
    if (trace.size() != 0 && new_parent->GetSortedCount() < kMinSortedRecNum) {
      MergeInternalNodes(new_parent, key, key_length);
    }
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
