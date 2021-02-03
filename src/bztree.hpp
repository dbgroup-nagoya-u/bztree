// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <array>
#include <atomic>
#include <map>
#include <memory>
#include <stack>
#include <tuple>
#include <utility>
#include <vector>

#include "internal_node.hpp"
#include "leaf_node.hpp"

namespace bztree
{
template <class Compare>
class BzTree
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // an entire node size in bytes
  const size_t node_size_;
  // if a data block size exceeds this threshold, invoke consolidation
  const size_t block_size_threshold_;
  // if a deleted block size exceeds this threshold, invoke consolidation
  const size_t deleted_size_threshold_;
  // a paramater to prepare a free space for a merged node
  const size_t desired_free_space_;
  // if an occupied size of a consolidated node is less than this threshold, invoke merging
  const size_t node_size_min_threshold_;
  // if an occupied size of a merged node exceeds this threshold, cancel merging
  const size_t max_merged_size_;
  // an epoch to count the number of failure
  const size_t index_epoch_;
  // a comparator to compare input keys
  const Compare comparator_;

  // a root node of BzTree
  PayloadUnion root_;

  // a pool of descriptors for MwCAS
  std::unique_ptr<pmwcas::DescriptorPool> descriptor_pool_;

  /*------------------------------------------------------------------------------------------------
   * Temporal garbage collector
   *----------------------------------------------------------------------------------------------*/

  /// The number of the maximum garbages
  static constexpr size_t kMaxGarbage = 1E6;

  /// A list of garbage nodes
  std::array<void *, kMaxGarbage> garbage_nodes;

  /// The number of garbage nodes. This count is utilized to reserve a garbage region in
  /// multi-thread environment.
  std::atomic<size_t> garbage_count{0};

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  BaseNode *
  GetRootAsNode() const
  {
    return static_cast<BaseNode *>(reinterpret_cast<void *>(root_.payload.value));
  }

  LeafNode *
  SearchLeafNode(  //
      const void *key,
      const bool range_is_closed) const
  {
    assert(!GetRootAsNode()->IsLeaf());  // a root node must be an internal node

    auto current_node = GetRootAsNode();
    do {
      const auto index = current_node->SearchSortedMetadata(key, range_is_closed, comparator_);
      current_node = BitCast<BaseNode *>(current_node->GetPayloadAddr(index));
    } while (!current_node->IsLeaf());

    return BitCast<LeafNode *>(current_node);
  }

  std::stack<std::pair<BaseNode *, size_t>>
  SearchLeafNodeWithTrace(const void *key) const
  {
    assert(!GetRootAsNode()->IsLeaf());  // a root node must be an internal node

    // trace nodes to a target leaf node
    auto current_node = GetRootAsNode();
    std::stack<std::pair<BaseNode *, size_t>> trace;
    trace.emplace(current_node, 0);
    do {
      const auto index = current_node->SearchSortedMetadata(key, true, comparator_);
      current_node = BitCast<BaseNode *>(current_node->GetPayloadAddr(index));
      trace.emplace(current_node, index);
    } while (!current_node->IsLeaf());

    return trace;
  }

  std::stack<std::pair<BaseNode *, size_t>>
  SearchInternalNodeWithTrace(  //
      const void *key,
      InternalNode *target_node) const
  {
    assert(!GetRootAsNode()->IsLeaf());  // a root node must be an internal node

    // trace nodes to a target internal node
    auto current_node = GetRootAsNode();
    std::stack<std::pair<BaseNode *, size_t>> trace;
    trace.emplace(current_node, 0);
    do {
      if (HaveSameAddress(current_node, target_node)) {
        // find a target node
        return trace;
      }
      const auto index = current_node->SearchSortedMetadata(key, true, comparator_);
      current_node = BitCast<BaseNode *>(current_node->GetPayloadAddr(index));
      trace.emplace(current_node, index);
    } while (!current_node->IsLeaf());

    return trace;
  }

  constexpr bool
  NeedConsolidation(const StatusWord status) const
  {
    return status.GetBlockSize() > block_size_threshold_
           || status.GetDeletedSize() > deleted_size_threshold_;
  }

  static size_t
  ComputeOccupiedSize(const std::vector<std::pair<void *, Metadata>> &live_meta)
  {
    size_t block_size = 0;
    for (auto &&[key, meta] : live_meta) {
      block_size += meta.GetTotalLength();
    }
    block_size += kHeaderLength + (kWordLength * live_meta.size());

    return block_size;
  }

  uint32_t
  SetRootForMwCAS(  //
      const void *old_root_node,
      const void *new_root_node,
      pmwcas::Descriptor *descriptor)
  {
    return descriptor->AddEntry(&(root_.int_payload), PayloadUnion{old_root_node}.int_payload,
                                PayloadUnion{new_root_node}.int_payload);
  }

  /*------------------------------------------------------------------------------------------------
   * Temporal utilities for garbage collect
   *----------------------------------------------------------------------------------------------*/

  size_t
  ReserveGabageRegion(const size_t num_garbage)
  {
    size_t expected = garbage_count.load();
    size_t reserved_count;

    do {
      reserved_count = expected + num_garbage;
    } while (!garbage_count.compare_exchange_weak(expected, reserved_count));

    return expected;
  }

  /*################################################################################################
   * Internal structure modification functoins
   *##############################################################################################*/

  void
  ConsolidateLeafNode(  //
      LeafNode *target_leaf,
      const void *target_key,
      const size_t target_key_length)
  {
    // freeze a target node and perform consolidation
    target_leaf->Freeze(descriptor_pool_.get());

    // gather sorted live metadata of a targetnode, and check whether split/merge is required
    const auto live_meta = target_leaf->GatherSortedLiveMetadata(comparator_);
    const auto occupied_size = ComputeOccupiedSize(live_meta);
    if (occupied_size + desired_free_space_ > node_size_) {
      SplitLeafNode(target_leaf, target_key, live_meta);
      return;
    } else if (occupied_size < node_size_min_threshold_) {
      MergeLeafNode(target_leaf, target_key, target_key_length, occupied_size, live_meta);
      return;
    }

    // install a new node
    auto new_leaf = LeafNode::Consolidate(target_leaf, live_meta);
    pmwcas::Descriptor *pd;
    auto epoch_manager = descriptor_pool_->GetEpoch();
    do {
      // check whether a target node remains
      auto trace = SearchLeafNodeWithTrace(target_key);
      const auto current_leaf_node = trace.top().first;
      if (!HaveSameAddress(target_leaf, current_leaf_node)) {
        // other threads have already performed consolidation
        delete new_leaf;
        return;
      }

      // check a parent status
      trace.pop();  // remove a leaf node
      auto [parent_node, target_index] = trace.top();
      const auto parent_status = parent_node->GetStatusWordProtected(epoch_manager);
      if (StatusWord::IsFrozen(parent_status)) {
        continue;
      }

      // swap a consolidated node for an old one
      pd = descriptor_pool_->AllocateDescriptor();
      parent_node->SetStatusForMwCAS(parent_status, parent_status, pd);
      parent_node->SetPayloadForMwCAS(target_index, target_leaf, new_leaf, pd);
    } while (!pd->MwCAS());
    // ...WIP...: delete target node
  }

  void
  SplitLeafNode(  //
      const LeafNode *target_leaf,
      const void *target_key,
      const std::vector<std::pair<void *, Metadata>> &sorted_meta)
  {
    // get a separator key and its length
    const auto left_record_count = (sorted_meta.size() / 2);
    const auto [split_key, split_meta] = sorted_meta[left_record_count];
    const auto split_key_length = split_meta.GetKeyLength();

    bool install_success;
    do {
      // check whether a target node remains
      auto trace = SearchLeafNodeWithTrace(target_key);
      const auto [current_leaf, target_index] = trace.top();
      if (!HaveSameAddress(target_leaf, current_leaf)) {
        return;  // other threads have already performed splitting
      }

      // check whether it is required to split a parent node
      trace.pop();  // remove a leaf node
      const auto parent = BitCast<InternalNode *>(trace.top().first);
      if (parent->NeedSplit(split_key_length, kWordLength)) {
        // invoke a parent (internal) node splitting
        SplitInternalNode(parent, target_key);
        continue;
      }

      // create new nodes
      const auto [left_leaf, right_leaf] =
          LeafNode::Split(target_leaf, sorted_meta, left_record_count);
      const auto new_parent = InternalNode::NewParentForSplit(parent, split_key, split_key_length,
                                                              left_leaf, right_leaf, target_index);

      // try installation of new nodes
      install_success = InstallNewInternalNode(trace, new_parent);
      if (install_success) {
        // ...WIP...: delete old nodes
      } else {
        delete left_leaf;
        delete right_leaf;
        delete new_parent;
      }
    } while (!install_success);
  }

  void
  SplitInternalNode(  //
      const InternalNode *target_node,
      const void *target_key)
  {
    // get a split index and a corresponding key length
    const auto left_record_count = (target_node->GetSortedCount() / 2);
    const auto [split_key, split_key_length] =
        target_node->GetKeyAndItsLength(left_record_count - 1);

    bool install_success;
    do {
      // check whether a target node remains
      auto trace = SearchInternalNodeWithTrace(target_key, target_node);
      const auto [current_node, target_index] = trace.top();
      if (current_node->IsLeaf()) {
        // there is no target node, because other threads have already performed SMOs
        return;
      }

      // create new nodes
      InternalNode *left_node, *right_node, *new_parent;
      if (trace.size() == 1) {
        // split a root node
        std::tie(left_node, right_node) = InternalNode::Split(target_node, left_record_count);
        new_parent = InternalNode::CreateNewRoot(left_node, right_node);
      } else {
        // check whether it is required to split a parent node
        trace.pop();  // remove a target node
        const auto parent = BitCast<InternalNode *>(trace.top().first);
        if (parent->NeedSplit(split_key_length, kWordLength)) {
          // invoke a parent (internal) node splitting
          SplitInternalNode(parent, target_key);
          continue;
        }
        std::tie(left_node, right_node) = InternalNode::Split(target_node, left_record_count);
        new_parent = InternalNode::NewParentForSplit(parent, split_key, split_key_length, left_node,
                                                     right_node, target_index);
      }

      // try installation of new nodes
      install_success = InstallNewInternalNode(trace, new_parent);
      if (install_success) {
        // ...WIP...: delete old nodes
      } else {
        delete left_node;
        delete right_node;
        delete new_parent;
      }
    } while (!install_success);
  }

  void
  MergeLeafNodes(  //
      const LeafNode *target_node,
      const void *target_key,
      const size_t target_key_length,
      const size_t target_size,
      const std::vector<std::pair<void *, Metadata>> &sorted_meta)
  {
    bool install_success;
    do {
      // check whether a target node remains
      auto trace = SearchLeafNodeWithTrace(target_key);
      const auto [current_leaf, target_index] = trace.top();
      if (!HaveSameAddress(target_node, current_leaf)) {
        return;  // other threads have already performed merging
      }

      // check whether it is required to merge a parent node
      trace.pop();  // remove a leaf node
      const auto parent = BitCast<InternalNode *>(trace.top().first);
      if (parent->NeedMerge(target_key_length, kWordLength, node_size_min_threshold_)) {
        // invoke a parent (internal) node merging
        MergeInternalNodes(parent, target_key, target_key_length);
        continue;
      }

      // create new nodes
      InternalNode *new_parent;
      LeafNode *sibling_node, *merged_node;
      size_t deleted_index;
      if (parent->CanMergeLeftSibling(target_index, target_size, max_merged_size_)) {
        deleted_index = target_index - 1;
        sibling_node = BitCast<LeafNode *>(parent->GetPayloadAddr(deleted_index));
        const auto sibling_meta = sibling_node->GatherSortedLiveMetadata(comparator_);
        merged_node = LeafNode::Merge(target_node, sorted_meta, sibling_node, sibling_meta, true);
      } else if (parent->CanMergeRightSibling(target_index, target_size, max_merged_size_)) {
        const auto right_index = target_index + 1;
        deleted_index = target_index;
        sibling_node = BitCast<LeafNode *>(parent->GetPayloadAddr(right_index));
        const auto sibling_meta = sibling_node->GatherSortedLiveMetadata(comparator_);
        merged_node = LeafNode::Merge(target_node, sorted_meta, sibling_node, sibling_meta, false);
      } else {
        return;  // there is no space to perform merge operation
      }
      new_parent = InternalNode::NewParentForMerge(parent, merged_node, deleted_index);

      // try installation of new nodes
      install_success = InstallNewInternalNode(trace, new_parent);
      if (install_success) {
        // ...WIP...: delete old nodes
      } else {
        delete merged_node;
        delete new_parent;
      }
    } while (!install_success);
  }

  void
  MergeInternalNodes(  //
      const InternalNode *target_node,
      const void *target_key,
      const size_t target_key_length)
  {
    bool install_success;
    do {
      // check whether a target node remains
      auto trace = SearchInternalNodeWithTrace(target_key, target_node);
      const auto [current_node, target_index] = trace.top();
      if (current_node->IsLeaf()) {
        // there is no target node, because other threads have already performed SMOs
        return;
      }

      // check whether it is required to merge a parent node
      trace.pop();  // remove a target node
      const auto parent = BitCast<InternalNode *>(trace.top().first);
      if (!HaveSameAddress(parent, GetRootAsNode())
          && parent->NeedMerge(target_key_length, kWordLength, node_size_min_threshold_)) {
        // invoke a parent (internal) node merging
        MergeInternalNodes(parent, target_key, target_key_length);
        continue;
      }

      // create new nodes
      InternalNode *sibling_node, *merged_node, *new_parent;
      size_t deleted_index;
      const auto target_size = target_node->GetStatusWord().GetOccupiedSize();
      if (parent->CanMergeLeftSibling(target_index, target_size, max_merged_size_)) {
        deleted_index = target_index - 1;
        sibling_node = BitCast<InternalNode *>(parent->GetPayloadAddr(deleted_index));
        merged_node = InternalNode::Merge(target_node, sibling_node, true);
      } else if (parent->CanMergeRightSibling(target_index, target_size, max_merged_size_)) {
        const auto right_index = target_index + 1;
        deleted_index = target_index;
        sibling_node = BitCast<InternalNode *>(parent->GetPayloadAddr(right_index));
        merged_node = InternalNode::Merge(target_node, sibling_node, false);
      } else {
        return;  // there is no space to perform merge operation
      }
      new_parent = InternalNode::NewParentForMerge(parent, merged_node, deleted_index);

      if (new_parent->GetSortedCount() == 1) {
        // shrink BzTree
        new_parent = merged_node;
      }

      // try installation of new nodes
      install_success = InstallNewInternalNode(trace, new_parent);
      if (install_success) {
        // ...WIP...: delete old nodes
      } else {
        delete merged_node;
        delete new_parent;
      }
    } while (!install_success);
  }

  bool
  InstallNewInternalNode(  //
      std::stack<std::pair<BaseNode *, size_t>> *trace,
      const InternalNode *new_internal_node)
  {
    auto *pd = descriptor_pool_->AllocateDescriptor();
    auto epoch_manager = descriptor_pool_->GetEpoch();

    if (trace->size() > 1) {
      /*--------------------------------------------------------------------------------------------
       * Swapping a new internal node
       *------------------------------------------------------------------------------------------*/

      // prepare installing nodes
      auto [old_internal_node, swapping_index] = trace->top();
      trace->pop();
      auto parent_node = trace->top().first;

      // check wether related nodes are frozen
      const auto status = old_internal_node->GetStatusWordProtected(epoch_manager);
      const auto parent_status = parent_node->GetStatusWordProtected(epoch_manager);
      if (status.IsFrozen() || parent_status.IsFrozen()) {
        return false;
      }

      // freeze an old internal node
      const auto frozen_status = status.Freeze();

      // install a new internal node by PMwCAS
      old_internal_node->SetStatusForMwCAS(status, frozen_status, pd);
      parent_node->SetPayloadForMwCAS(swapping_index, old_internal_node, new_internal_node, pd);
      parent_node->SetStatusForMwCAS(parent_status, parent_status, pd);  // check concurrent SMOs
    } else {
      /*--------------------------------------------------------------------------------------------
       * Swapping a new root node
       *------------------------------------------------------------------------------------------*/

      auto old_root_node = trace->top().first;

      // check wether an old root node is frozen
      const auto status = old_root_node->GetStatusWordProtected(epoch_manager);
      if (status.IsFrozen()) {
        return false;
      }

      // freeze an old root node
      const auto frozen_status = status.Freeze();

      // install a new root node by PMwCAS
      auto *pd = descriptor_pool_->AllocateDescriptor();
      old_root_node->SetStatusForMwCAS(status, frozen_status, pd);
      SetRootForMwCAS(old_root_node, new_internal_node, pd);
    }

    return pd->MwCAS();
  }

 public:
  /*################################################################################################
   * Public constructor/destructor
   *##############################################################################################*/

  explicit BzTree(pmwcas::DescriptorPool *pool,
                  const Compare comparator,
                  const size_t node_size = 4096,
                  const size_t block_size_threshold = 3072,
                  const size_t deleted_size_threshold = 1024,
                  const size_t desired_free_space = 512,
                  const size_t node_size_min_threshold = 256,
                  const size_t merged_size_threshold = 2048)
      : node_size_{node_size},
        block_size_threshold_{block_size_threshold},
        deleted_size_threshold_{deleted_size_threshold},
        desired_free_space_{desired_free_space},
        node_size_min_threshold_{node_size_min_threshold},
        max_merged_size_{merged_size_threshold},
        comparator_{comparator},
        index_epoch_{0}
  {
    descriptor_pool_.reset(pool);
  }

  BzTree(const BzTree &) = delete;
  BzTree &operator=(const BzTree &) = delete;
  BzTree(BzTree &&) = default;
  BzTree &operator=(BzTree &&) = default;
  virtual ~BzTree() = default;

  /*################################################################################################
   * Public read APIs
   *##############################################################################################*/

  std::pair<ReturnCode, std::unique_ptr<std::byte[]>>
  Read(const void *key)
  {
    auto leaf_node = SearchLeafNode(key, true);
    const auto [return_code, payload] = leaf_node->Read(key, comparator_);
    if (return_code == BaseNode::NodeReturnCode::kSuccess) {
      return {ReturnCode::kSuccess, payload};
    } else {
      return {ReturnCode::kKeyNotExist, nullptr};
    }
  }

  std::pair<ReturnCode,
            std::vector<std::pair<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>>>>
  Scan(  //
      const void *begin_key,
      const bool begin_is_closed,
      const void *end_key,
      const bool end_is_closed)
  {
    std::vector<std::pair<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>>> all_results;
    BaseNode::NodeReturnCode scan_in_progress;
    do {
      const auto [return_code, leaf_results] =
          ScanPerLeaf(begin_key, begin_is_closed, end_key, end_is_closed);
      // concatanate scan results for each leaf node
      all_results.reserve(all_results.size() + leaf_results.size());
      all_results.insert(all_results.end(), leaf_results.begin(), leaf_results.end());
      // continue until searching all target leaf nodes
      scan_in_progress = return_code;
      begin_key = all_results.back().first.get();
      begin_is_closed = false;
    } while (scan_in_progress == BaseNode::NodeReturnCode::kScanInProgress);

    return {ReturnCode::kSuccess, all_results};
  }

  std::pair<ReturnCode,
            std::vector<std::pair<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>>>>
  ScanPerLeaf(  //
      const void *begin_key,
      const bool begin_is_closed,
      const void *end_key,
      const bool end_is_closed)
  {
    auto leaf_node = SearchLeafNode(begin_key, begin_is_closed);
    const auto [return_code, scan_results] =
        leaf_node->Scan(begin_key, begin_is_closed, end_key, end_is_closed, comparator_);
    if (return_code == BaseNode::NodeReturnCode::kScanInProgress) {
      return {ReturnCode::kScanInProgress, scan_results};
    } else {
      return {ReturnCode::kSuccess, scan_results};
    }
  }

  /*################################################################################################
   * Public write APIs
   *##############################################################################################*/

  ReturnCode
  Write(  //
      const void *key,
      const size_t key_length,
      const void *payload,
      const size_t payload_length)
  {
    LeafNode *leaf_node;
    BaseNode::NodeReturnCode return_code;
    StatusWord node_status;
    bool is_retry = false;
    do {
      leaf_node = SearchLeafNode(key, true);
      std::tie(return_code, node_status) = leaf_node->Write(
          key, key_length, payload, payload_length, index_epoch_, descriptor_pool_.get());
      if (is_retry && return_code == BaseNode::NodeReturnCode::kFrozen) {
        // invoke consolidation in this thread
        ConsolidateLeafNode(leaf_node, key, key_length);
        is_retry = false;
      } else {
        is_retry = true;
      }
    } while (return_code == BaseNode::NodeReturnCode::kFrozen);

    if (NeedConsolidation(node_status)) {
      // invoke consolidation with a new thread
      std::thread t(ConsolidateLeafNode, leaf_node, key, key_length);
      t.detach();
    }
    return ReturnCode::kSuccess;
  }

  ReturnCode
  Insert(  //
      const void *key,
      const size_t key_length,
      const void *payload,
      const size_t payload_length)
  {
    LeafNode *leaf_node;
    BaseNode::NodeReturnCode return_code;
    StatusWord node_status;
    bool is_retry = false;
    do {
      leaf_node = SearchLeafNode(key, true);
      std::tie(return_code, node_status) =
          leaf_node->Insert(key, key_length, payload, payload_length, index_epoch_, comparator_,
                            descriptor_pool_.get());
      if (return_code == BaseNode::NodeReturnCode::kKeyExist) {
        return ReturnCode::kKeyExist;
      } else if (is_retry && return_code == BaseNode::NodeReturnCode::kFrozen) {
        // invoke consolidation in this thread
        ConsolidateLeafNode(leaf_node, key, key_length);
        is_retry = false;
      } else {
        is_retry = true;
      }
    } while (return_code == BaseNode::NodeReturnCode::kFrozen);

    if (NeedConsolidation(node_status)) {
      // invoke consolidation with a new thread
      std::thread t(ConsolidateLeafNode, leaf_node, key, key_length);
      t.detach();
    }
    return ReturnCode::kSuccess;
  }

  ReturnCode
  Update(  //
      const void *key,
      const size_t key_length,
      const void *payload,
      const size_t payload_length)
  {
    LeafNode *leaf_node;
    BaseNode::NodeReturnCode return_code;
    StatusWord node_status;
    bool is_retry = false;
    do {
      leaf_node = SearchLeafNode(key, true);
      std::tie(return_code, node_status) =
          leaf_node->Update(key, key_length, payload, payload_length,  //
                            index_epoch_, comparator_, descriptor_pool_.get());
      if (return_code == BaseNode::NodeReturnCode::kKeyNotExist) {
        return ReturnCode::kKeyNotExist;
      } else if (is_retry && return_code == BaseNode::NodeReturnCode::kFrozen) {
        // invoke consolidation in this thread
        ConsolidateLeafNode(leaf_node, key, key_length);
        is_retry = false;
      } else {
        is_retry = true;
      }
    } while (return_code == BaseNode::NodeReturnCode::kFrozen);

    if (NeedConsolidation(node_status)) {
      // invoke consolidation with a new thread
      std::thread t(ConsolidateLeafNode, leaf_node, key, key_length);
      t.detach();
    }
    return ReturnCode::kSuccess;
  }

  ReturnCode
  Delete(  //
      const void *key,
      const size_t key_length)
  {
    LeafNode *leaf_node;
    BaseNode::NodeReturnCode return_code;
    StatusWord node_status;
    bool is_retry = false;
    do {
      leaf_node = SearchLeafNode(key, true);
      std::tie(return_code, node_status) =
          leaf_node->Delete(key, key_length, comparator_, descriptor_pool_.get());
      if (return_code == BaseNode::NodeReturnCode::kKeyNotExist) {
        return ReturnCode::kKeyNotExist;
      } else if (is_retry && return_code == BaseNode::NodeReturnCode::kFrozen) {
        // invoke consolidation in this thread
        ConsolidateLeafNode(leaf_node, key, key_length);
        is_retry = false;
      } else {
        is_retry = true;
      }
    } while (return_code == BaseNode::NodeReturnCode::kFrozen);

    if (NeedConsolidation(node_status)) {
      // invoke consolidation with a new thread
      std::thread t(ConsolidateLeafNode, leaf_node, key, key_length);
      t.detach();
    }
    return ReturnCode::kSuccess;
  }
};

}  // namespace bztree
