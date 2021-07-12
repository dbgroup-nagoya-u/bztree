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

  /// an epoch to count the number of failure
  const size_t index_epoch_;

  /// a root node of BzTree
  BaseNode_t *root_;

  /// garbage collector
  TLSBasedMemoryManager<BaseNode_t> gc_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  constexpr BaseNode_t *
  GetRoot()
  {
    return ReadMwCASField<BaseNode_t *>(&root_);
  }

  constexpr std::pair<void *, BaseNode_t *>
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
      current_node = InternalNode_t::GetChildNode(current_node, index);
    } while (!current_node->IsLeaf());

    return {node_key, current_node};
  }

  constexpr std::stack<std::pair<BaseNode_t *, size_t>>
  TraceTargetNode(  //
      const Key key,
      const void *target_node)
  {
    // trace nodes to a target internal node
    std::stack<std::pair<BaseNode_t *, size_t>> trace;
    size_t index = 0;
    auto current_node = GetRoot();
    while (!HaveSameAddress(current_node, target_node) && !current_node->IsLeaf()) {
      trace.emplace(current_node, index);
      index = current_node->SearchSortedMetadata(key, true).second;
      current_node = InternalNode_t::GetChildNode(current_node, index);
    }
    trace.emplace(current_node, index);

    return trace;
  }

  static constexpr bool
  NeedConsolidation(  //
      const BaseNode_t *leaf_node,
      const StatusWord status)
  {
    return status.GetRecordCount() - leaf_node->GetSortedCount() > kMaxUnsortedRecNum
           || status.GetDeletedSize() > kMaxDeletedSpaceSize;
  }

  static constexpr bool
  NeedSplit(  //
      const BaseNode_t *internal_node,
      const size_t key_length)
  {
    return internal_node->GetStatusWordProtected().GetOccupiedSize() + key_length
           > kPageSize - 2 * kWordLength;
  }

  static constexpr bool
  NeedMerge(const BaseNode_t *internal_node)
  {
    return internal_node->GetSortedCount() < kMinSortedRecNum;
  }

  static constexpr std::pair<BaseNode_t *, bool>
  GetSiblingNode(  //
      BaseNode_t *parent_node,
      const size_t target_index,
      const size_t target_size)
  {
    if (target_index > 0) {
      const auto sibling_node = InternalNode_t::GetChildNode(parent_node, target_index - 1);
      const auto sibling_size = sibling_node->GetStatusWordProtected().GetLiveDataSize();
      if ((target_size + sibling_size) < kPageSize / 2) return {sibling_node, true};
    }
    if (target_index < parent_node->GetSortedCount() - 1) {
      const auto sibling_node = InternalNode_t::GetChildNode(parent_node, target_index + 1);
      const auto sibling_size = sibling_node->GetStatusWordProtected().GetLiveDataSize();
      if ((target_size + sibling_size) < kPageSize / 2) return {sibling_node, false};
    }
    return {nullptr, false};
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

  constexpr auto
  ScanPerLeaf(  //
      const Key *begin_key,
      const bool begin_is_closed,
      const Key *end_key,
      const bool end_is_closed)
  {
    const auto guard = gc_.CreateEpochGuard();

    auto [node_key, leaf_node] = SearchLeafNode(begin_key, begin_is_closed);
    auto [rc, scan_results] =
        LeafNode_t::Scan(leaf_node, begin_key, begin_is_closed, end_key, end_is_closed);

    if (node_key == nullptr
        || (end_key != nullptr
            && (Compare{}(*end_key, Cast<Key>(node_key))
                || IsEqual<Compare>(*end_key, Cast<Key>(node_key))))) {
      return std::make_pair(ReturnCode::kSuccess, std::move(scan_results));
    } else {
      return std::make_pair(ReturnCode::kScanInProgress, std::move(scan_results));
    }
  }

  /*################################################################################################
   * Internal structure modification functoins
   *##############################################################################################*/

  constexpr void
  ConsolidateLeafNode(  //
      BaseNode_t *target_node,
      const Key target_key,
      const size_t target_key_length)
  {
    // freeze a target node and perform consolidation
    if (target_node->Freeze() != NodeReturnCode::kSuccess) return;

    // gather sorted live metadata of a targetnode, and check whether split/merge is required
    const auto live_meta = LeafNode_t::GatherSortedLiveMetadata(target_node);
    const auto occupied_size = ComputeOccupiedSize(live_meta);
    if (occupied_size > kPageSize - kMinFreeSpaceSize) {
      SplitLeafNode(target_node, target_key, live_meta);
      return;
    } else if (live_meta.size() < kMinSortedRecNum) {
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

  constexpr void
  SplitLeafNode(  //
      const BaseNode_t *target_node,
      const Key target_key,
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
    BaseNode_t *parent = nullptr;
    size_t target_index = 0;
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(target_key, target_node);
      target_index = trace.top().second;
      trace.pop();

      // check whether it is required to split a parent node
      parent = trace.top().first;
      if (NeedSplit(parent, split_key_length)) {
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
    const auto new_parent =
        InternalNode_t::NewParentForSplit(parent, left_node, right_node, target_index);

    // install new nodes
    InstallNewNode(trace, new_parent, target_key, parent);
    gc_.AddGarbage(target_node);
    gc_.AddGarbage(parent);
  }

  constexpr void
  SplitInternalNode(  //
      BaseNode_t *target_node,
      const Key target_key)
  {
    // get a split index and a corresponding key length
    const auto left_record_count = (target_node->GetSortedCount() / 2);
    const auto [split_key, split_key_length] =
        target_node->GetKeyAndItsLength(left_record_count - 1);

    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    std::stack<std::pair<BaseNode_t *, size_t>> trace;
    BaseNode_t *parent = nullptr;
    size_t target_index = 0;
    while (true) {
      // check a target node is live
      const auto target_status = target_node->GetStatusWordProtected();
      if (target_status.IsFrozen()) return;

      // trace and get the embedded index of a target node
      trace = TraceTargetNode(target_key, target_node);
      target_index = trace.top().second;

      // check whether it is required to split a parent node
      MwCASDescriptor desc;
      if (trace.size() > 1) {  // target is not a root node (i.e., there is a parent node)
        trace.pop();
        parent = trace.top().first;
        if (NeedSplit(parent, split_key_length)) {
          SplitInternalNode(parent, target_key);
          continue;
        }

        // check a parent node is live
        const auto parent_status = parent->GetStatusWordProtected();
        if (parent_status.IsFrozen()) continue;

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
    BaseNode_t *new_parent;
    if (parent != nullptr) {
      // target is not a root node
      new_parent = InternalNode_t::NewParentForSplit(parent, left_node, right_node, target_index);
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

  constexpr bool
  MergeLeafNodes(  //
      const BaseNode_t *target_node,
      const Key target_key,
      const size_t target_key_length,
      const size_t target_size,
      const std::vector<std::pair<Key, Metadata>> &sorted_meta)
  {
    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    std::stack<std::pair<BaseNode_t *, size_t>> trace;
    BaseNode_t *parent = nullptr, *sibling_node = nullptr;
    bool sibling_is_left = true;
    size_t target_index = 0;
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(target_key, target_node);
      target_index = trace.top().second;
      if (!HaveSameAddress(trace.top().first, target_node)) return false;
      trace.pop();

      // check a parent node is live
      parent = trace.top().first;
      const auto parent_status = parent->GetStatusWordProtected();
      if (parent_status.IsFrozen()) continue;

      // check a left/right sibling node is live
      std::tie(sibling_node, sibling_is_left) = GetSiblingNode(parent, target_index, target_size);
      if (sibling_node == nullptr) return false;  // there is no live sibling node
      const auto sibling_status = sibling_node->GetStatusWordProtected();
      if (sibling_status.IsFrozen()) {
        if (sibling_is_left) continue;
        return false;
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
    const auto sibling_meta = LeafNode_t::GatherSortedLiveMetadata(sibling_node);
    BaseNode_t *merged_node;
    size_t deleted_index;
    if (sibling_is_left) {
      merged_node = LeafNode_t::Merge(sibling_node, sibling_meta, target_node, sorted_meta);
      deleted_index = target_index - 1;
    } else {
      merged_node = LeafNode_t::Merge(target_node, sorted_meta, sibling_node, sibling_meta);
      deleted_index = target_index;
    }
    const auto new_parent = InternalNode_t::NewParentForMerge(parent, merged_node, deleted_index);

    // install new nodes
    InstallNewNode(trace, new_parent, target_key, parent);
    gc_.AddGarbage(target_node);
    gc_.AddGarbage(parent);
    gc_.AddGarbage(sibling_node);

    // check whether it is required to merge a new parent node
    if (trace.size() != 0 && NeedMerge(new_parent)) {
      MergeInternalNodes(new_parent, target_key, target_key_length);
    }
    return true;
  }

  constexpr void
  MergeInternalNodes(  //
      BaseNode_t *target_node,
      const Key target_key,
      const size_t target_key_length)
  {
    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    std::stack<std::pair<BaseNode_t *, size_t>> trace;
    BaseNode_t *parent = nullptr, *sibling_node = nullptr;
    bool sibling_is_left;
    size_t target_index = 0;
    while (true) {
      // check a target node is not frozen and live
      const auto target_status = target_node->GetStatusWordProtected();
      if (target_status.IsFrozen()) return;

      // trace and get the embedded index of a target node
      trace = TraceTargetNode(target_key, target_node);
      target_index = trace.top().second;
      trace.pop();

      // check a parent node is live
      parent = trace.top().first;
      const auto parent_status = parent->GetStatusWordProtected();
      if (parent_status.IsFrozen()) continue;

      // check a left/right sibling node is live
      const auto target_size = target_status.GetOccupiedSize();
      std::tie(sibling_node, sibling_is_left) = GetSiblingNode(parent, target_index, target_size);
      if (sibling_node == nullptr) return;  // there is no live sibling node

      const auto sibling_status = sibling_node->GetStatusWordProtected();
      if (sibling_status.IsFrozen()) continue;

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
    BaseNode_t *merged_node;
    size_t deleted_index;
    if (sibling_is_left) {
      merged_node = InternalNode_t::Merge(sibling_node, target_node);
      deleted_index = target_index - 1;
    } else {
      merged_node = InternalNode_t::Merge(target_node, sibling_node);
      deleted_index = target_index;
    }
    const auto new_parent = InternalNode_t::NewParentForMerge(parent, merged_node, deleted_index);

    // install new nodes
    InstallNewNode(trace, new_parent, target_key, parent);
    gc_.AddGarbage(target_node);
    gc_.AddGarbage(parent);
    gc_.AddGarbage(sibling_node);

    // check whether it is required to merge a parent node
    if (trace.size() != 0 && NeedMerge(new_parent)) {
      MergeInternalNodes(new_parent, target_key, target_key_length);
    }
  }

  constexpr void
  InstallNewNode(  //
      std::stack<std::pair<BaseNode_t *, size_t>> &trace,
      BaseNode_t *new_node,
      const Key target_key,
      const BaseNode_t *target_node)
  {
    while (true) {
      MwCASDescriptor desc;
      if (trace.size() > 1) {
        /*------------------------------------------------------------------------------------------
         * Swapping a new internal node
         *----------------------------------------------------------------------------------------*/

        // prepare installing nodes
        auto [old_node, index] = trace.top();
        if (!HaveSameAddress(old_node, target_node)) return;
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
        parent_node->SetPayloadForMwCAS(desc, parent_node->GetMetadata(index), old_node, new_node);
      } else {
        /*------------------------------------------------------------------------------------------
         * Swapping a new root node
         *----------------------------------------------------------------------------------------*/

        const auto old_node = trace.top().first;
        if (!HaveSameAddress(old_node, target_node)) return;
        trace.pop();
        desc.AddMwCASTarget(&root_, old_node, new_node);
      }

      if (desc.MwCAS()) return;

      trace = TraceTargetNode(target_key, target_node);
    }
  }

 public:
  /*################################################################################################
   * Public constructor/destructor
   *##############################################################################################*/

  BzTree() : index_epoch_{1}, root_{InternalNode_t::CreateInitialRoot()}, gc_{1000} {}

  ~BzTree() = default;

  BzTree(const BzTree &) = delete;
  BzTree &operator=(const BzTree &) = delete;
  BzTree(BzTree &&) = delete;
  BzTree &operator=(BzTree &&) = delete;

  /*################################################################################################
   * Public read APIs
   *##############################################################################################*/

  constexpr auto
  Read(const Key key)
  {
    const auto guard = gc_.CreateEpochGuard();

    const auto leaf_node = SearchLeafNode(&key, true).second;
    auto [rc, payload] = LeafNode_t::Read(leaf_node, key);
    if (rc == NodeReturnCode::kSuccess) {
      return std::make_pair(ReturnCode::kSuccess, std::move(payload));
    }
    return std::make_pair(ReturnCode::kKeyNotExist, std::move(payload));
  }

  /*################################################################################################
   * Public write APIs
   *##############################################################################################*/

  constexpr ReturnCode
  Write(  //
      const Key key,
      const Payload payload,
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))
  {
    const auto guard = gc_.CreateEpochGuard();

    while (true) {
      auto leaf_node = SearchLeafNode(&key, true).second;
      const auto [rc, status] =
          LeafNode_t::Write(leaf_node, key, key_length, payload, payload_length, index_epoch_);

      if (rc == NodeReturnCode::kSuccess) {
        if (NeedConsolidation(leaf_node, status)) ConsolidateLeafNode(leaf_node, key, key_length);
        break;
      } else if (rc == NodeReturnCode::kNoSpace) {
        ConsolidateLeafNode(leaf_node, key, key_length);
      }
    }
    return ReturnCode::kSuccess;
  }

  constexpr ReturnCode
  Insert(  //
      const Key key,
      const Payload payload,
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))
  {
    const auto guard = gc_.CreateEpochGuard();

    while (true) {
      auto leaf_node = SearchLeafNode(&key, true).second;
      const auto [rc, status] =
          LeafNode_t::Insert(leaf_node, key, key_length, payload, payload_length, index_epoch_);

      if (rc == NodeReturnCode::kSuccess || rc == NodeReturnCode::kKeyExist) {
        if (NeedConsolidation(leaf_node, status)) ConsolidateLeafNode(leaf_node, key, key_length);
        if (rc == NodeReturnCode::kKeyExist) return ReturnCode::kKeyExist;
        break;
      } else if (rc == NodeReturnCode::kNoSpace) {
        ConsolidateLeafNode(leaf_node, key, key_length);
      }
    }
    return ReturnCode::kSuccess;
  }

  constexpr ReturnCode
  Update(  //
      const Key key,
      const Payload payload,
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))
  {
    const auto guard = gc_.CreateEpochGuard();

    while (true) {
      auto leaf_node = SearchLeafNode(&key, true).second;
      const auto [rc, status] =
          LeafNode_t::Update(leaf_node, key, key_length, payload, payload_length, index_epoch_);

      if (rc == NodeReturnCode::kSuccess || rc == NodeReturnCode::kKeyNotExist) {
        if (NeedConsolidation(leaf_node, status)) ConsolidateLeafNode(leaf_node, key, key_length);
        if (rc == NodeReturnCode::kKeyNotExist) return ReturnCode::kKeyNotExist;
        break;
      } else if (rc == NodeReturnCode::kNoSpace) {
        ConsolidateLeafNode(leaf_node, key, key_length);
      }
    }
    return ReturnCode::kSuccess;
  }

  constexpr ReturnCode
  Delete(  //
      const Key key,
      const size_t key_length = sizeof(Key))
  {
    const auto guard = gc_.CreateEpochGuard();

    while (true) {
      auto leaf_node = SearchLeafNode(&key, true).second;
      const auto [rc, status] = LeafNode_t::Delete(leaf_node, key, key_length);

      if (rc == NodeReturnCode::kSuccess || rc == NodeReturnCode::kKeyNotExist) {
        if (NeedConsolidation(leaf_node, status)) ConsolidateLeafNode(leaf_node, key, key_length);
        if (rc == NodeReturnCode::kKeyNotExist) return ReturnCode::kKeyNotExist;
        break;
      } else if (rc == NodeReturnCode::kNoSpace) {
        ConsolidateLeafNode(leaf_node, key, key_length);
      }
    }
    return ReturnCode::kSuccess;
  }
};

}  // namespace dbgroup::index::bztree
