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
#include "component/record_page.hpp"
#include "memory/epoch_based_gc.hpp"
#include "utility.hpp"

namespace dbgroup::index::bztree
{
// declare alias to visible components
using component::RecordPage;

template <class Key, class Payload, class Compare = std::less<Key>>
class BzTree
{
  using Metadata = component::Metadata;
  using StatusWord = component::StatusWord;
  using Node_t = component::Node<Key, Payload, Compare>;
  using MetaArray = std::array<Metadata, Node_t::kMaxRecordNum>;
  using NodeReturnCode = component::NodeReturnCode;
  using RecordPage_t = RecordPage<Key, Payload>;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<Node_t>;
  using NodeRef = std::pair<Node_t *, size_t>;
  using NodeStack = std::vector<NodeRef, ::dbgroup::memory::STLAlloc<NodeRef>>;
  using MwCASDescriptor = component::MwCASDescriptor;

 private:
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

  Node_t *
  GetRoot()
  {
    return component::ReadMwCASField<Node_t *>(&root_);
  }

  Node_t *
  SearchLeafNode(  //
      const Key key,
      const bool range_is_closed)
  {
    auto current_node = GetRoot();
    do {
      const auto index = internal::SearchChildNode(current_node, key, range_is_closed);
      current_node = internal::GetChildNode(current_node, index);
    } while (!current_node->IsLeaf());

    return current_node;
  }

  Node_t *
  SearchLeftEdgeLeaf()
  {
    auto current_node = GetRoot();
    do {
      current_node = internal::GetChildNode(current_node, 0);
    } while (!current_node->IsLeaf());

    return current_node;
  }

  NodeStack
  TraceTargetNode(  //
      const Key key,
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

  static constexpr bool
  NeedSplit(  //
      const Node_t *internal_node,
      const size_t key_length)
  {
    return internal_node->GetStatusWordProtected().GetOccupiedSize() + key_length
           > kPageSize - 2 * kWordLength;
  }

  static std::pair<Node_t *, bool>
  GetSiblingNode(  //
      Node_t *parent,
      const size_t target_pos,
      const size_t target_size)
  {
    if (target_pos > 0) {
      const auto sib_node = internal::GetChildNode(parent, target_pos - 1);
      const auto sib_size = sib_node->GetStatusWordProtected().GetLiveDataSize();
      if ((target_size + sib_size) < kPageSize / 2) return {sib_node, true};
    }
    if (target_pos < parent->GetSortedCount() - 1) {
      const auto sib_node = internal::GetChildNode(parent, target_pos + 1);
      const auto sib_size = sib_node->GetStatusWordProtected().GetLiveDataSize();
      if ((target_size + sib_size) < kPageSize / 2) return {sib_node, false};
    }
    return {nullptr, false};
  }

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

  void
  ConsolidateLeafNode(  //
      Node_t *node,
      const Key key,
      const size_t key_length)
  {
    // freeze a target node and perform consolidation
    if (node->Freeze() != NodeReturnCode::kSuccess) return;

    // gather sorted live metadata of a targetnode, and check whether split/merge is required
    const auto [metadata, rec_count] = leaf::GatherSortedLiveMetadata(node);
    const auto target_size = ComputeOccupiedSize(metadata, rec_count);
    if (target_size > kPageSize - kMinFreeSpaceSize) {
      SplitLeafNode(node, key, metadata, rec_count);
      return;
    } else if (rec_count < kMinSortedRecNum) {
      if (MergeLeafNodes(node, key, key_length, target_size, metadata, rec_count)) return;
    }

    // install a new node
    const auto new_node = leaf::Consolidate(node, metadata, rec_count);
    auto trace = TraceTargetNode(key, node);
    InstallNewNode(trace, new_node, key, node);

    // register frozen nodes with garbage collection
    gc_.AddGarbage(node);
  }

  void
  SplitLeafNode(  //
      const Node_t *node,
      const Key key,
      const MetaArray &metadata,
      const size_t rec_count)
  {
    const size_t left_rec_count = rec_count / 2;
    const auto split_key_length = metadata[left_rec_count - 1].GetKeyLength();

    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    NodeStack trace;
    Node_t *parent = nullptr;
    size_t target_pos = 0;
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(key, node);
      target_pos = trace.back().second;
      trace.pop_back();

      // check whether it is required to split a parent node
      parent = trace.back().first;
      if (NeedSplit(parent, split_key_length)) {
        SplitInternalNode(parent, key);
        continue;
      }

      // pre-freezing of SMO targets
      if (parent->Freeze() == NodeReturnCode::kSuccess) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    // create new nodes
    const auto [left_node, right_node] = leaf::Split(node, metadata, rec_count, left_rec_count);
    const auto new_parent = internal::NewParentForSplit(parent, left_node, right_node, target_pos);

    // install new nodes
    InstallNewNode(trace, new_parent, key, parent);

    // register frozen nodes with garbage collection
    gc_.AddGarbage(node);
    gc_.AddGarbage(parent);
  }

  void
  SplitInternalNode(  //
      Node_t *node,
      const Key key)
  {
    // get a split index and a corresponding key length
    const auto left_rec_count = (node->GetSortedCount() / 2);
    const auto split_key_length = node->GetMetadata(left_rec_count - 1).GetKeyLength();

    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    NodeStack trace;
    Node_t *parent = nullptr;
    size_t target_pos = 0;
    while (true) {
      // check a target node is live
      const auto target_status = node->GetStatusWordProtected();
      if (target_status.IsFrozen()) return;

      // trace and get the embedded index of a target node
      trace = TraceTargetNode(key, node);
      target_pos = trace.back().second;

      MwCASDescriptor desc;
      if (trace.size() > 1) {  // target is not a root node (i.e., there is a parent node)
        trace.pop_back();
        parent = trace.back().first;

        // check whether it is required to split a parent node
        if (NeedSplit(parent, split_key_length)) {
          SplitInternalNode(parent, key);
          continue;
        }

        // check a parent node is live
        const auto parent_status = parent->GetStatusWordProtected();
        if (parent_status.IsFrozen()) continue;

        // pre-freezing of SMO targets
        parent->SetStatusForMwCAS(desc, parent_status, parent_status.Freeze());
      }

      // pre-freezing of SMO targets
      node->SetStatusForMwCAS(desc, target_status, target_status.Freeze());
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    // create new nodes
    const auto [left_node, right_node] = internal::Split(node, left_rec_count);
    Node_t *new_parent;
    if (parent != nullptr) {  // target is not a root node
      new_parent = internal::NewParentForSplit(parent, left_node, right_node, target_pos);
    } else {  // target is a root node
      new_parent = internal::CreateNewRoot(left_node, right_node);
      parent = node;  // set parent as a target node for installation
    }

    // install new nodes
    InstallNewNode(trace, new_parent, key, parent);

    // register frozen nodes with garbage collection
    gc_.AddGarbage(node);
    if (parent != node) gc_.AddGarbage(parent);
  }

  bool
  MergeLeafNodes(  //
      const Node_t *node,
      const Key key,
      const size_t key_length,
      const size_t target_size,
      const MetaArray &metadata,
      const size_t rec_count)
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
    Node_t *merged_node;
    size_t deleted_pos;
    if (sib_is_left) {
      merged_node = leaf::Merge(sib_node, sib_meta, sib_rec_count, node, metadata, rec_count);
      deleted_pos = target_pos - 1;
    } else {
      merged_node = leaf::Merge(node, metadata, rec_count, sib_node, sib_meta, sib_rec_count);
      deleted_pos = target_pos;
    }
    const auto new_parent = internal::NewParentForMerge(parent, merged_node, deleted_pos);

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

  void
  MergeInternalNodes(  //
      Node_t *node,
      const Key key,
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
    Node_t *merged_node;
    size_t deleted_pos;
    if (sib_is_left) {
      merged_node = internal::Merge(sib_node, node);
      deleted_pos = target_pos - 1;
    } else {
      merged_node = internal::Merge(node, sib_node);
      deleted_pos = target_pos;
    }
    const auto new_parent = internal::NewParentForMerge(parent, merged_node, deleted_pos);

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

  void
  InstallNewNode(  //
      NodeStack &trace,
      Node_t *new_node,
      const Key target_key,
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
          trace = TraceTargetNode(target_key, target_node);
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

      trace = TraceTargetNode(target_key, target_node);
    }
  }

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

    ::dbgroup::memory::Delete(node);
  }

 public:
  /*################################################################################################
   * Public constructor/destructor
   *##############################################################################################*/

  explicit BzTree(const size_t gc_interval_microsec = 100000)
      : index_epoch_{1},
        root_{internal::CreateInitialRoot<Key, Payload, Compare>()},
        gc_{gc_interval_microsec}
  {
    gc_.StartGC();
  }

  ~BzTree() { DeleteChildren(GetRoot()); }

  BzTree(const BzTree &) = delete;
  BzTree &operator=(const BzTree &) = delete;
  BzTree(BzTree &&) = delete;
  BzTree &operator=(BzTree &&) = delete;

  /*################################################################################################
   * Public read APIs
   *##############################################################################################*/

  auto
  Read(const Key key)
  {
    const auto guard = gc_.CreateEpochGuard();

    const auto node = SearchLeafNode(key, true);

    Payload payload{};
    const auto rc = leaf::Read(node, key, payload);
    if (rc == NodeReturnCode::kSuccess) {
      if constexpr (std::is_same_v<Payload, char *>) {
        return std::make_pair(ReturnCode::kSuccess, std::unique_ptr<char>{payload});
      } else {
        return std::make_pair(ReturnCode::kSuccess, std::move(payload));
      }
    }
    if constexpr (std::is_same_v<Payload, char *>) {
      return std::make_pair(ReturnCode::kKeyNotExist, std::unique_ptr<char>{});
    } else {
      return std::make_pair(ReturnCode::kKeyNotExist, Payload{});
    }
  }

  void
  Scan(  //
      RecordPage_t &page,
      const Key *begin_key = nullptr,
      const bool begin_is_closed = false,
      const Key *end_key = nullptr,
      const bool end_is_closed = false)
  {
    const auto guard = gc_.CreateEpochGuard();

    const auto node =
        (begin_key == nullptr) ? SearchLeftEdgeLeaf() : SearchLeafNode(*begin_key, begin_is_closed);
    leaf::Scan(node, begin_key, begin_is_closed, end_key, end_is_closed, page);
  }

  /*################################################################################################
   * Public write APIs
   *##############################################################################################*/

  ReturnCode
  Write(  //
      const Key key,
      const Payload payload,
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))
  {
    const auto guard = gc_.CreateEpochGuard();

    while (true) {
      auto node = SearchLeafNode(key, true);
      const auto rc = leaf::Write(node, key, key_length, payload, payload_length, index_epoch_);

      if (rc == NodeReturnCode::kSuccess) {
        break;
      } else if (rc == NodeReturnCode::kNoSpace) {
        ConsolidateLeafNode(node, key, key_length);
      }
    }
    return ReturnCode::kSuccess;
  }

  ReturnCode
  Insert(  //
      const Key key,
      const Payload payload,
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
      } else if (rc == NodeReturnCode::kNoSpace) {
        ConsolidateLeafNode(node, key, key_length);
      }
    }
    return ReturnCode::kSuccess;
  }

  ReturnCode
  Update(  //
      const Key key,
      const Payload payload,
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
      } else if (rc == NodeReturnCode::kNoSpace) {
        ConsolidateLeafNode(node, key, key_length);
      }
    }
    return ReturnCode::kSuccess;
  }

  ReturnCode
  Delete(  //
      const Key key,
      const size_t key_length = sizeof(Key))
  {
    const auto guard = gc_.CreateEpochGuard();

    while (true) {
      auto node = SearchLeafNode(key, true);
      const auto rc = leaf::Delete(node, key, key_length);

      if (rc == NodeReturnCode::kSuccess || rc == NodeReturnCode::kKeyNotExist) {
        if (rc == NodeReturnCode::kKeyNotExist) return ReturnCode::kKeyNotExist;
        break;
      } else if (rc == NodeReturnCode::kNoSpace) {
        ConsolidateLeafNode(node, key, key_length);
      }
    }
    return ReturnCode::kSuccess;
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

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
