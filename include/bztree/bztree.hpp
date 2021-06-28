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
#include <map>
#include <memory>
#include <stack>
#include <tuple>
#include <utility>
#include <vector>

#include "components/internal_node.hpp"
#include "components/leaf_node.hpp"
#include "memory/manager/tls_based_memory_manager.hpp"

namespace dbgroup::index::bztree
{
using dbgroup::memory::manager::TLSBasedMemoryManager;

template <class Key, class Payload, class Compare = std::less<Key>>
class BzTree
{
 private:
  using BaseNode_t = BaseNode<Key, Payload, Compare>;
  using LeafNode_t = LeafNode<Key, Payload, Compare>;
  using InternalNode_t = InternalNode<Key, Payload, Compare>;
  using Record_t = Record<Key, Payload>;
  using NodeReturnCode = typename BaseNode<Key, Payload, Compare>::NodeReturnCode;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// an entire node size in bytes
  const size_t node_size_;

  /// if an occupied size of a consolidated node is less than this threshold, invoke merging
  const size_t min_node_size_;

  /// the minimum size of free space in bytes
  const size_t min_free_space_;

  /// an expected size of free space after SMOs in bytes
  const size_t expected_free_space_;

  /// if a deleted block size exceeds this threshold, invoke consolidation
  const size_t max_deleted_size_;

  /// if an occupied size of a merged node exceeds this threshold, cancel merging
  const size_t max_merged_size_;

  /// an epoch to count the number of failure
  const size_t index_epoch_;

  /// a root node of BzTree
  uintptr_t root_;

  /// garbage collector
  TLSBasedMemoryManager<BaseNode_t> gc_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  BaseNode_t *
  GetRoot()
  {
    return reinterpret_cast<BaseNode_t *>(ReadMwCASField<uintptr_t>(&root_));
  }

  std::pair<void *, LeafNode_t *>
  SearchLeafNode(  //
      const Key *key,
      const bool range_is_closed)
  {
    auto current_node = GetRoot();
    void *node_key = nullptr;
    do {
      const auto index =
          (key == nullptr) ? 0 : current_node->SearchSortedMetadata(*key, range_is_closed).second;
      const auto meta = current_node->GetMetadata(index);
      if (meta.GetKeyLength() == 0) {
        node_key = nullptr;
      } else {
        node_key = current_node->GetKeyAddr(meta);
      }
      current_node = CastAddress<InternalNode_t *>(current_node)->GetChildNode(index);
    } while (!current_node->IsLeaf());

    return {node_key, CastAddress<LeafNode_t *>(current_node)};
  }

  std::stack<std::pair<BaseNode_t *, size_t>>
  TraceTargetNode(  //
      const Key &key,
      const void *target_node)
  {
    // trace nodes to a target internal node
    std::stack<std::pair<BaseNode_t *, size_t>> trace;
    size_t index = 0;
    auto current_node = GetRoot();
    while (!HaveSameAddress(current_node, target_node) && !current_node->IsLeaf()) {
      trace.emplace(current_node, index);
      index = current_node->SearchSortedMetadata(key, true).second;
      current_node = CastAddress<InternalNode_t *>(current_node)->GetChildNode(index);
    }
    trace.emplace(current_node, index);

    return trace;
  }

  constexpr bool
  NeedConsolidation(const StatusWord status) const
  {
    return status.GetOccupiedSize() + min_free_space_ > node_size_
           || status.GetDeletedSize() > max_deleted_size_;
  }

  static constexpr size_t
  ComputeOccupiedSize(const std::vector<std::pair<Key, Metadata>> &live_meta)
  {
    size_t block_size = 0;
    for (auto &&[key, meta] : live_meta) {
      block_size += meta.GetTotalLength();
    }
    block_size += kHeaderLength + (kWordLength * live_meta.size());

    return block_size;
  }

  void
  SetRootForMwCAS(  //
      MwCASDescriptor &desc,
      const void *old_root_node,
      const void *new_root_node)
  {
    desc.AddMwCASTarget(&root_,  //
                        reinterpret_cast<uintptr_t>(old_root_node),
                        reinterpret_cast<uintptr_t>(new_root_node));
  }

  std::pair<ReturnCode, std::vector<std::unique_ptr<Record_t>>>
  ScanPerLeaf(  //
      const Key *begin_key,
      const bool begin_is_closed,
      const Key *end_key,
      const bool end_is_closed)
  {
    const auto guard = gc_.CreateEpochGuard();

    auto [node_key, leaf_node] = SearchLeafNode(begin_key, begin_is_closed);
    auto [rc, scan_results] = leaf_node->Scan(begin_key, begin_is_closed, end_key, end_is_closed);

    if (node_key == nullptr
        || (end_key != nullptr
            && (Compare{}(*end_key, CastKey<Key>(node_key))
                || IsEqual<Compare>(*end_key, CastKey<Key>(node_key))))) {
      return {ReturnCode::kSuccess, std::move(scan_results)};
    } else {
      return {ReturnCode::kScanInProgress, std::move(scan_results)};
    }
  }

  std::pair<BaseNode_t *, bool>
  GetMergeableSibling(  //
      InternalNode_t *parent,
      const size_t target_index,
      const size_t target_size,
      const bool is_leaf)
  {
    if (parent->CanMergeLeftSibling(target_index, target_size, max_merged_size_)) {
      const auto sibling_node = parent->GetChildNode(target_index - 1);
      if ((is_leaf && sibling_node->IsLeaf()) || (!is_leaf && !sibling_node->IsLeaf())) {
        return {sibling_node, true};
      }
    }

    if (parent->CanMergeRightSibling(target_index, target_size, max_merged_size_)) {
      const auto sibling_node = parent->GetChildNode(target_index + 1);
      if ((is_leaf && sibling_node->IsLeaf()) || (!is_leaf && !sibling_node->IsLeaf())) {
        return {sibling_node, false};
      }
    }

    return {nullptr, true};
  }

  /*################################################################################################
   * Internal structure modification functoins
   *##############################################################################################*/

  void
  ConsolidateLeafNode(  //
      LeafNode_t *target_node,
      const Key &target_key,
      const size_t target_key_length)
  {
    // freeze a target node and perform consolidation
    if (target_node->Freeze() != NodeReturnCode::kSuccess) {
      return;
    }

    // gather sorted live metadata of a targetnode, and check whether split/merge is required
    const auto live_meta = target_node->GatherSortedLiveMetadata();
    const auto occupied_size = ComputeOccupiedSize(live_meta);
    if (occupied_size + expected_free_space_ > node_size_) {
      SplitLeafNode(target_node, target_key, live_meta);
      return;
    } else if (occupied_size < min_node_size_) {
      if (MergeLeafNodes(target_node, target_key, target_key_length, occupied_size, live_meta)) {
        return;
      }
    }

    // install a new node
    const auto new_node = LeafNode_t::Consolidate(target_node, live_meta);
    auto trace = TraceTargetNode(target_key, target_node);
    InstallNewNode(trace, new_node, target_key, target_node);

    // Temporal implementation of garbage collection
    gc_.AddGarbage(target_node);
  }

  void
  SplitLeafNode(  //
      const LeafNode_t *target_node,
      const Key &target_key,
      const std::vector<std::pair<Key, Metadata>> &sorted_meta)
  {
    // get a separator key and its length
    const auto left_record_count = (sorted_meta.size() / 2);
    const auto [split_key, split_meta] = sorted_meta[left_record_count - 1];
    const auto split_key_length = split_meta.GetKeyLength();

    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    std::stack<std::pair<BaseNode_t *, size_t>> trace;
    InternalNode_t *parent = nullptr;
    size_t target_index = 0;
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(target_key, target_node);
      target_index = trace.top().second;
      trace.pop();

      // check whether it is required to split a parent node
      parent = CastAddress<InternalNode_t *>(trace.top().first);
      if (parent->NeedSplit(split_key_length, kWordLength)) {
        SplitInternalNode(parent, target_key);
        continue;
      }

      // pre-freezing of SMO targets
      if (parent->Freeze() == NodeReturnCode::kSuccess) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    // create new nodes
    const auto [left_node, right_node] =
        LeafNode_t::Split(target_node, sorted_meta, left_record_count);
    const auto new_parent = InternalNode_t::NewParentForSplit(parent, split_key, split_key_length,
                                                              left_node, right_node, target_index);

    // install new nodes
    InstallNewNode(trace, new_parent, target_key, parent);
    gc_.AddGarbage(target_node);
    gc_.AddGarbage(parent);
  }

  void
  SplitInternalNode(  //
      InternalNode_t *target_node,
      const Key &target_key)
  {
    // get a split index and a corresponding key length
    const auto left_record_count = (target_node->GetSortedCount() / 2);
    const auto [split_key, split_key_length] =
        target_node->GetKeyAndItsLength(left_record_count - 1);

    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    std::stack<std::pair<BaseNode_t *, size_t>> trace;
    InternalNode_t *parent = nullptr;
    size_t target_index = 0;
    while (true) {
      // check a target node is live
      const auto target_status = target_node->GetStatusWordProtected();
      if (target_status.IsFrozen()) {
        return;  // a target node is modified by concurrent SMOs
      }

      // trace and get the embedded index of a target node
      trace = TraceTargetNode(target_key, target_node);
      target_index = trace.top().second;

      // check whether it is required to split a parent node
      MwCASDescriptor desc;
      if (trace.size() > 1) {  // target is not a root node (i.e., there is a parent node)
        trace.pop();
        parent = CastAddress<InternalNode_t *>(trace.top().first);
        if (parent->NeedSplit(split_key_length, kWordLength)) {
          SplitInternalNode(parent, target_key);
          continue;
        }

        // check a parent node is live
        const auto parent_status = parent->GetStatusWordProtected();
        if (parent_status.IsFrozen()) {
          continue;
        }

        // pre-freezing of SMO targets
        parent->SetStatusForMwCAS(desc, parent_status, parent_status.Freeze());
      }

      // pre-freezing of SMO targets
      target_node->SetStatusForMwCAS(desc, target_status, target_status.Freeze());
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    // create new nodes
    const auto [left_node, right_node] = InternalNode_t::Split(target_node, left_record_count);
    InternalNode_t *new_parent;
    if (parent != nullptr) {
      // target is not a root node
      new_parent = InternalNode_t::NewParentForSplit(parent, split_key, split_key_length, left_node,
                                                     right_node, target_index);
    } else {
      // target is a root node
      new_parent = InternalNode_t::CreateNewRoot(left_node, right_node);
      parent = target_node;  // set parent as a target node for installation
    }

    // install new nodes
    InstallNewNode(trace, new_parent, target_key, parent);
    gc_.AddGarbage(target_node);
    if (parent != target_node) gc_.AddGarbage(parent);
  }

  bool
  MergeLeafNodes(  //
      const LeafNode_t *target_node,
      const Key &target_key,
      const size_t target_key_length,
      const size_t target_size,
      const std::vector<std::pair<Key, Metadata>> &sorted_meta)
  {
    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    std::stack<std::pair<BaseNode_t *, size_t>> trace;
    InternalNode_t *parent = nullptr;
    LeafNode_t *sibling_node = nullptr;
    bool sibling_is_left = true;
    size_t target_index = 0;
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(target_key, target_node);
      target_index = trace.top().second;
      trace.pop();

      // check a parent node is live
      parent = CastAddress<InternalNode_t *>(trace.top().first);
      const auto parent_status = parent->GetStatusWordProtected();
      if (parent_status.IsFrozen()) {
        continue;
      }

      // check a left/right sibling node is live
      BaseNode_t *tmp_node;
      std::tie(tmp_node, sibling_is_left) =
          GetMergeableSibling(parent, target_index, target_size, true);
      if (tmp_node == nullptr) {
        return false;  // there is no live sibling node
      }
      sibling_node = CastAddress<LeafNode_t *>(tmp_node);
      const auto sibling_status = sibling_node->GetStatusWordProtected();
      if (sibling_status.IsFrozen()) {
        continue;
      }

      // pre-freezing of SMO targets
      MwCASDescriptor desc;
      parent->SetStatusForMwCAS(desc, parent_status, parent_status.Freeze());
      sibling_node->SetStatusForMwCAS(desc, sibling_status, sibling_status.Freeze());
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    // create new nodes
    const auto deleted_index = (sibling_is_left) ? target_index - 1 : target_index;
    const auto sibling_meta = sibling_node->GatherSortedLiveMetadata();
    const auto merged_node =
        LeafNode_t::Merge(target_node, sorted_meta, sibling_node, sibling_meta, sibling_is_left);
    const auto new_parent = InternalNode_t::NewParentForMerge(parent, merged_node, deleted_index);
    const auto new_occupied_size = new_parent->GetStatusWord().GetOccupiedSize();

    // install new nodes
    InstallNewNode(trace, new_parent, target_key, parent);
    gc_.AddGarbage(target_node);
    gc_.AddGarbage(parent);
    gc_.AddGarbage(sibling_node);

    // check whether it is required to merge a new parent node
    if (!HaveSameAddress(new_parent, GetRoot()) && new_occupied_size < min_node_size_) {
      MergeInternalNodes(new_parent, target_key, target_key_length);
    }

    return true;
  }

  void
  MergeInternalNodes(  //
      InternalNode_t *target_node,
      const Key &target_key,
      const size_t target_key_length)
  {
    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    std::stack<std::pair<BaseNode_t *, size_t>> trace;
    InternalNode_t *parent = nullptr, *sibling_node = nullptr;
    bool sibling_is_left;
    size_t target_index = 0;
    while (true) {
      // check a target node is not frozen and live
      const auto target_status = target_node->GetStatusWordProtected();
      if (target_status.IsFrozen()) {
        return;  // other SMOs are modifying a target node
      }

      // trace and get the embedded index of a target node
      trace = TraceTargetNode(target_key, target_node);
      target_index = trace.top().second;
      trace.pop();

      // check a parent node is live
      parent = CastAddress<InternalNode_t *>(trace.top().first);
      const auto parent_status = parent->GetStatusWordProtected();
      if (parent_status.IsFrozen()) {
        continue;
      }

      // check a left/right sibling node is live
      const auto target_size = target_status.GetOccupiedSize();
      BaseNode_t *tmp_node;
      std::tie(tmp_node, sibling_is_left) =
          GetMergeableSibling(parent, target_index, target_size, true);
      if (tmp_node == nullptr) {
        return;  // there is no live sibling node
      }
      sibling_node = CastAddress<InternalNode_t *>(tmp_node);
      const auto sibling_status = sibling_node->GetStatusWordProtected();
      if (sibling_status.IsFrozen()) {
        continue;
      }

      // pre-freezing of SMO targets
      auto desc = MwCASDescriptor{};
      parent->SetStatusForMwCAS(desc, parent_status, parent_status.Freeze());
      target_node->SetStatusForMwCAS(desc, target_status, target_status.Freeze());
      sibling_node->SetStatusForMwCAS(desc, sibling_status, sibling_status.Freeze());
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    // create new nodes
    const auto deleted_index = (sibling_is_left) ? target_index - 1 : target_index;
    const auto merged_node = InternalNode_t::Merge(target_node, sibling_node, sibling_is_left);
    InternalNode_t *new_parent = nullptr;
    if (parent->GetSortedCount() > 2) {
      new_parent = InternalNode_t::NewParentForMerge(parent, merged_node, deleted_index);
    } else {
      // if a merged node is an only child, swap it for a new parent node
      new_parent = merged_node;
    }
    const auto new_occupied_size = new_parent->GetStatusWord().GetOccupiedSize();

    // install new nodes
    InstallNewNode(trace, new_parent, target_key, parent);
    gc_.AddGarbage(target_node);
    gc_.AddGarbage(parent);
    gc_.AddGarbage(sibling_node);

    // check whether it is required to merge a parent node
    if (!HaveSameAddress(new_parent, GetRoot()) && new_occupied_size < min_node_size_) {
      MergeInternalNodes(new_parent, target_key, target_key_length);
    }
  }

  void
  InstallNewNode(  //
      std::stack<std::pair<BaseNode_t *, size_t>> &trace,
      const void *new_node,
      const Key &target_key,
      const void *target_node)
  {
    while (true) {
      MwCASDescriptor desc;
      if (trace.size() > 1) {
        /*--------------------------------------------------------------------------------------------
         * Swapping a new internal node
         *------------------------------------------------------------------------------------------*/

        // prepare installing nodes
        const auto [old_node, swapping_index] = trace.top();
        trace.pop();
        auto parent_node = trace.top().first;

        // check wether related nodes are frozen
        const auto parent_status = parent_node->GetStatusWordProtected();
        if (parent_status.IsFrozen()) {
          trace = TraceTargetNode(target_key, target_node);
          continue;
        }

        // install a new internal node by PMwCAS
        parent_node->SetStatusForMwCAS(desc, parent_status, parent_status);
        parent_node->SetChildForMwCAS(desc, swapping_index, old_node, new_node);
      } else {
        /*--------------------------------------------------------------------------------------------
         * Swapping a new root node
         *------------------------------------------------------------------------------------------*/

        const auto old_node = trace.top().first;
        SetRootForMwCAS(desc, old_node, new_node);
      }

      if (desc.MwCAS()) return;

      trace = TraceTargetNode(target_key, target_node);
    }
  }

 public:
  /*################################################################################################
   * Public constructor/destructor
   *##############################################################################################*/

  explicit BzTree(const size_t node_size = 4096,
                  const size_t min_node_size = 256,
                  const size_t min_free_space = 256,
                  const size_t expected_free_space = 1024,
                  const size_t max_deleted_size = 1024,
                  const size_t max_merged_size = 2048)
      : node_size_{node_size},
        min_node_size_{min_node_size},
        min_free_space_{min_free_space},
        expected_free_space_{expected_free_space},
        max_deleted_size_{max_deleted_size},
        max_merged_size_{max_merged_size},
        index_epoch_{1},
        gc_{1000}
  {
    // initialize a tree structure: one internal node with one leaf node
    const auto root_node = InternalNode_t::CreateInitialRoot(node_size_);
    root_ = reinterpret_cast<uintptr_t>(root_node);
  }

  ~BzTree() = default;

  BzTree(const BzTree &) = delete;
  BzTree &operator=(const BzTree &) = delete;
  BzTree(BzTree &&) = delete;
  BzTree &operator=(BzTree &&) = delete;

  /*################################################################################################
   * Public read APIs
   *##############################################################################################*/

  std::pair<ReturnCode, std::unique_ptr<Record_t>>
  Read(const Key &key)
  {
    const auto guard = gc_.CreateEpochGuard();

    auto leaf_node = SearchLeafNode(&key, true).second;
    auto [rc, payload] = leaf_node->Read(key);
    if (rc == NodeReturnCode::kSuccess) {
      return std::pair{ReturnCode::kSuccess, std::move(payload)};
    } else {
      return {ReturnCode::kKeyNotExist, nullptr};
    }
  }

  std::pair<ReturnCode, std::vector<std::unique_ptr<Record_t>>>
  Scan(  //
      const Key &begin_key_orig,
      const bool begin_is_closed_orig,
      const Key &end_key,
      const bool end_is_closed)
  {
    Key begin_key = begin_key_orig;
    bool begin_is_closed = begin_is_closed_orig;

    std::vector<std::unique_ptr<Record_t>> all_results;
    while (true) {
      auto [rc, leaf_results] = ScanPerLeaf(&begin_key, begin_is_closed, &end_key, end_is_closed);
      // concatanate scan results for each leaf node
      all_results.reserve(all_results.size() + leaf_results.size());
      all_results.insert(all_results.end(), std::make_move_iterator(leaf_results.begin()),
                         std::make_move_iterator(leaf_results.end()));
      if (rc == ReturnCode::kScanInProgress) {
        begin_key = all_results.back()->GetKey();
        begin_is_closed = false;
      } else {
        break;
      }
    }
    return {ReturnCode::kSuccess, std::move(all_results)};
  }

  std::pair<ReturnCode, std::vector<std::unique_ptr<Record_t>>>
  ScanLess(  //
      const Key &end_key,
      const bool end_is_closed)
  {
    Key tmp_key;
    Key *begin_key = nullptr;
    bool begin_is_closed = false;

    std::vector<std::unique_ptr<Record_t>> all_results;
    while (true) {
      auto [rc, leaf_results] = ScanPerLeaf(begin_key, begin_is_closed, &end_key, end_is_closed);
      // concatanate scan results for each leaf node
      all_results.reserve(all_results.size() + leaf_results.size());
      all_results.insert(all_results.end(), std::make_move_iterator(leaf_results.begin()),
                         std::make_move_iterator(leaf_results.end()));
      if (rc == ReturnCode::kScanInProgress) {
        tmp_key = all_results.back()->GetKey();
        begin_key = &tmp_key;
        begin_is_closed = false;
      } else {
        break;
      }
    }
    return {ReturnCode::kSuccess, std::move(all_results)};
  }

  std::pair<ReturnCode, std::vector<std::unique_ptr<Record_t>>>
  ScanGreater(  //
      const Key &begin_key_orig,
      const bool begin_is_closed_orig)
  {
    Key begin_key = begin_key_orig;
    bool begin_is_closed = begin_is_closed_orig;

    std::vector<std::unique_ptr<Record_t>> all_results;
    while (true) {
      auto [rc, leaf_results] = ScanPerLeaf(&begin_key, begin_is_closed, nullptr, false);
      // concatanate scan results for each leaf node
      all_results.reserve(all_results.size() + leaf_results.size());
      all_results.insert(all_results.end(), std::make_move_iterator(leaf_results.begin()),
                         std::make_move_iterator(leaf_results.end()));
      if (rc == ReturnCode::kScanInProgress) {
        begin_key = all_results.back()->GetKey();
        begin_is_closed = false;
      } else {
        break;
      }
    }
    return {ReturnCode::kSuccess, std::move(all_results)};
  }

  /*################################################################################################
   * Public write APIs
   *##############################################################################################*/

  ReturnCode
  Write(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload,
      const size_t payload_length)
  {
    const auto guard = gc_.CreateEpochGuard();

    LeafNode_t *leaf_node;
    NodeReturnCode rc;
    StatusWord node_status;
    do {
      leaf_node = SearchLeafNode(&key, true).second;
      std::tie(rc, node_status) =
          leaf_node->Write(key, key_length, payload, payload_length, index_epoch_);
    } while (rc != NodeReturnCode::kSuccess);

    if (NeedConsolidation(node_status)) {
      // invoke consolidation with a new thread
      ConsolidateLeafNode(leaf_node, key, key_length);
    }

    return ReturnCode::kSuccess;
  }

  ReturnCode
  Write(  //
      const Key &key,
      const Payload &payload,
      const size_t payload_length)
  {
    return Write(key, sizeof(Key), payload, payload_length);
  }

  ReturnCode
  Write(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload)
  {
    return Write(key, key_length, payload, sizeof(Payload));
  }

  ReturnCode
  Write(  //
      const Key &key,
      const Payload &payload)
  {
    return Write(key, sizeof(Key), payload, sizeof(Payload));
  }

  ReturnCode
  Insert(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload,
      const size_t payload_length)
  {
    const auto guard = gc_.CreateEpochGuard();

    LeafNode_t *leaf_node;
    NodeReturnCode rc;
    StatusWord node_status;
    do {
      leaf_node = SearchLeafNode(&key, true).second;
      std::tie(rc, node_status) =
          leaf_node->Insert(key, key_length, payload, payload_length, index_epoch_);
    } while (rc != NodeReturnCode::kSuccess && rc != NodeReturnCode::kKeyExist);

    if (NeedConsolidation(node_status)) {
      // invoke consolidation with a new thread
      ConsolidateLeafNode(leaf_node, key, key_length);
    }

    if (rc == NodeReturnCode::kKeyExist) {
      return ReturnCode::kKeyExist;
    }
    return ReturnCode::kSuccess;
  }

  ReturnCode
  Insert(  //
      const Key &key,
      const Payload &payload,
      const size_t payload_length)
  {
    return Insert(key, sizeof(Key), payload, payload_length);
  }

  ReturnCode
  Insert(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload)
  {
    return Insert(key, key_length, payload, sizeof(Payload));
  }

  ReturnCode
  Insert(  //
      const Key &key,
      const Payload &payload)
  {
    return Insert(key, sizeof(Key), payload, sizeof(Payload));
  }

  ReturnCode
  Update(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload,
      const size_t payload_length)
  {
    const auto guard = gc_.CreateEpochGuard();

    LeafNode_t *leaf_node;
    NodeReturnCode rc;
    StatusWord node_status;
    do {
      leaf_node = SearchLeafNode(&key, true).second;
      std::tie(rc, node_status) =
          leaf_node->Update(key, key_length, payload, payload_length, index_epoch_);
    } while (rc != NodeReturnCode::kSuccess && rc != NodeReturnCode::kKeyNotExist);

    if (NeedConsolidation(node_status)) {
      // invoke consolidation with a new thread
      ConsolidateLeafNode(leaf_node, key, key_length);
    }

    if (rc == NodeReturnCode::kKeyNotExist) {
      return ReturnCode::kKeyNotExist;
    }
    return ReturnCode::kSuccess;
  }

  ReturnCode
  Update(  //
      const Key &key,
      const Payload &payload,
      const size_t payload_length)
  {
    return Update(key, sizeof(Key), payload, payload_length);
  }

  ReturnCode
  Update(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload)
  {
    return Update(key, key_length, payload, sizeof(Payload));
  }

  ReturnCode
  Update(  //
      const Key &key,
      const Payload &payload)
  {
    return Update(key, sizeof(Key), payload, sizeof(Payload));
  }

  ReturnCode
  Delete(  //
      const Key &key,
      const size_t key_length)
  {
    const auto guard = gc_.CreateEpochGuard();

    LeafNode_t *leaf_node;
    NodeReturnCode rc;
    StatusWord node_status;
    do {
      leaf_node = SearchLeafNode(&key, true).second;
      std::tie(rc, node_status) = leaf_node->Delete(key, key_length);
    } while (rc != NodeReturnCode::kSuccess && rc != NodeReturnCode::kKeyNotExist);

    if (NeedConsolidation(node_status)) {
      // invoke consolidation with a new thread
      ConsolidateLeafNode(leaf_node, key, key_length);
    }

    if (rc == NodeReturnCode::kKeyNotExist) {
      return ReturnCode::kKeyNotExist;
    }
    return ReturnCode::kSuccess;
  }

  ReturnCode
  Delete(const Key &key)
  {
    return Delete(key, sizeof(Key));
  }
};

}  // namespace dbgroup::index::bztree
