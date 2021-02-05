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
   * Internal enum and constants
   *##############################################################################################*/

  static constexpr size_t kDescriptorPoolSize = 1E4;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // an entire node size in bytes
  const size_t node_size_;
  // if an occupied size of a consolidated node is less than this threshold, invoke merging
  const size_t min_node_size_;
  // the minimum size of free space in bytes
  const size_t min_free_space_;
  // an expected size of free space after SMOs in bytes
  const size_t expected_free_space_;
  // if a deleted block size exceeds this threshold, invoke consolidation
  const size_t max_deleted_size_;
  // if an occupied size of a merged node exceeds this threshold, cancel merging
  const size_t max_merged_size_;
  // an epoch to count the number of failure
  const size_t index_epoch_;
  // a comparator to compare input keys
  const Compare comparator_ = Compare{};

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
  GetRoot()
  {
    const auto protected_root = root_.target_field.GetValue(descriptor_pool_->GetEpoch());
    const auto root_uptr = PayloadUnion{protected_root}.payload.value;
    return static_cast<BaseNode *>(reinterpret_cast<void *>(root_uptr));
  }

  LeafNode *
  SearchLeafNode(  //
      const void *key,
      const bool range_is_closed)
  {
    auto current_node = GetRoot();
    do {
      const auto index =
          current_node->SearchSortedMetadata(key, range_is_closed, comparator_).second;
      current_node = BitCast<InternalNode *>(current_node)->GetChildNode(index);
    } while (!current_node->IsLeaf());

    return BitCast<LeafNode *>(current_node);
  }

  std::tuple<BaseNode *, size_t, std::stack<std::pair<BaseNode *, size_t>>>
  TraceTargetNode(  //
      const void *key,
      const void *target_node)
  {
    // trace nodes to a target internal node
    std::stack<std::pair<BaseNode *, size_t>> trace;
    auto index = 0UL;
    auto current_node = GetRoot();
    while (!HaveSameAddress(current_node, target_node) && !current_node->IsLeaf()) {
      trace.emplace(current_node, index);
      index = current_node->SearchSortedMetadata(key, true, comparator_).second;
      current_node = BitCast<InternalNode *>(current_node)->GetChildNode(index);
    }

    return {current_node, index, trace};
  }

  constexpr bool
  NeedConsolidation(const StatusWord status) const
  {
    return status.GetOccupiedSize() + min_free_space_ > node_size_
           || status.GetDeletedSize() > max_deleted_size_;
  }

  static constexpr size_t
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
    const auto old_union = PayloadUnion{old_root_node};
    const auto new_union = PayloadUnion{new_root_node};
    return descriptor->AddEntry(&(root_.int_payload), old_union.int_payload, new_union.int_payload);
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
    if (occupied_size + expected_free_space_ > node_size_) {
      SplitLeafNode(target_leaf, target_key, live_meta);
      return;
    } else if (occupied_size < min_node_size_) {
      MergeLeafNodes(target_leaf, target_key, target_key_length, occupied_size, live_meta);
      return;
    }

    // install a new node
    auto new_leaf = LeafNode::Consolidate(target_leaf, live_meta);
    pmwcas::Descriptor *pd;
    auto epoch_manager = descriptor_pool_->GetEpoch();
    do {
      // check whether a target node remains
      auto [current_leaf_node, unused, trace] = TraceTargetNode(target_key, target_leaf);
      if (!HaveSameAddress(target_leaf, current_leaf_node)) {
        // other threads have already performed consolidation
        delete new_leaf;
        return;
      }

      // check a parent status
      auto [parent_node, target_index] = trace.top();
      const auto parent_status = parent_node->GetStatusWordProtected(epoch_manager);
      if (parent_status.IsFrozen()) {
        continue;
      }

      // swap a consolidated node for an old one
      pd = descriptor_pool_->AllocateDescriptor();
      parent_node->SetStatusForMwCAS(parent_status, parent_status, pd);
      parent_node->SetPayloadForMwCAS(target_index, target_leaf, new_leaf, pd);
    } while (!pd->MwCAS());

    // Temporal implementation of garbage collection
    const auto reserved_index = ReserveGabageRegion(1);
    garbage_nodes[reserved_index] = target_leaf;
  }

  void
  SplitLeafNode(  //
      const LeafNode *target_leaf,
      const void *target_key,
      const std::vector<std::pair<void *, Metadata>> &sorted_meta)
  {
    // get a separator key and its length
    const auto left_record_count = (sorted_meta.size() / 2);
    const auto [split_key, split_meta] = sorted_meta[left_record_count - 1];
    const auto split_key_length = split_meta.GetKeyLength();

    const auto tmp_key = *BitCast<uint64_t *>(split_key);

    bool install_success;
    do {
      // check whether a target node remains
      auto [current_leaf, target_index, trace] = TraceTargetNode(target_key, target_leaf);
      if (!HaveSameAddress(target_leaf, current_leaf)) {
        return;  // other threads have already performed splitting
      }

      // check whether it is required to split a parent node
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
      install_success = InstallNewInternalNode(&trace, new_parent);
      if (install_success) {
        // Temporal implementation of garbage collection
        const auto reserved_index = ReserveGabageRegion(2);
        garbage_nodes[reserved_index] = const_cast<LeafNode *>(target_leaf);
        garbage_nodes[reserved_index + 1] = parent;
      } else {
        delete left_leaf;
        delete right_leaf;
        delete new_parent;
      }
    } while (!install_success);
  }

  void
  SplitInternalNode(  //
      InternalNode *target_node,
      const void *target_key)
  {
    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    // check whether a target node remains
    auto [current_node, target_index, trace] = TraceTargetNode(target_key, target_node);
    if (current_node->IsLeaf()) {
      return;  // other threads have already performed SMOs
    }

    // freeze a target node only if it is not a root node
    if (!HaveSameAddress(target_node, GetRoot())
        && target_node->Freeze(descriptor_pool_.get()) == BaseNode::NodeReturnCode::kFrozen) {
      return;  // a target node is modified by concurrent SMOs
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    // get a split index and a corresponding key length
    const auto left_record_count = (target_node->GetSortedCount() / 2);
    const auto [split_key, split_key_length] =
        target_node->GetKeyAndItsLength(left_record_count - 1);

    const auto tmp_key = *BitCast<uint64_t *>(split_key);

    bool install_success;
    do {
      // check whether a target node remains
      std::tie(current_node, target_index, trace) = TraceTargetNode(target_key, target_node);
      if (current_node->IsLeaf()) {
        return;  // other threads have already performed SMOs
      }

      // create new nodes
      InternalNode *left_node, *right_node, *parent, *new_parent;
      if (trace.empty()) {
        // split a root node
        std::tie(left_node, right_node) = InternalNode::Split(target_node, left_record_count);
        new_parent = InternalNode::CreateNewRoot(left_node, right_node);
        // push an old root for installation
        trace.emplace(GetRoot(), 0);
        // retain an old root node for garbage collection
        parent = BitCast<InternalNode *>(GetRoot());
      } else {
        // check whether it is required to split a parent node
        parent = BitCast<InternalNode *>(trace.top().first);
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
      install_success = InstallNewInternalNode(&trace, new_parent);
      if (install_success) {
        // Temporal implementation of garbage collection
        const auto reserved_index = ReserveGabageRegion(2);
        garbage_nodes[reserved_index] = target_node;
        garbage_nodes[reserved_index + 1] = parent;
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
    // variables shared by phase 1 & 2
    LeafNode *sibling_node = nullptr;
    bool sibling_is_left;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    // check a target node remians
    auto [current_leaf, target_index, trace] = TraceTargetNode(target_key, target_node);
    if (!HaveSameAddress(target_node, current_leaf)) {
      return;  // other threads have already performed merging
    }

    // check a left/right sibling node is not frozen
    auto parent = BitCast<InternalNode *>(trace.top().first);
    if (parent->CanMergeLeftSibling(target_index, target_size, max_merged_size_)) {
      sibling_node = BitCast<LeafNode *>(parent->GetChildNode(target_index - 1));
      if (sibling_node->Freeze(descriptor_pool_.get()) == BaseNode::NodeReturnCode::kSuccess) {
        sibling_is_left = true;
      } else {
        sibling_node = nullptr;
      }
    }
    if (sibling_node == nullptr
        && parent->CanMergeRightSibling(target_index, target_size, max_merged_size_)) {
      sibling_node = BitCast<LeafNode *>(parent->GetChildNode(target_index + 1));
      if (sibling_node->Freeze(descriptor_pool_.get()) == BaseNode::NodeReturnCode::kSuccess) {
        sibling_is_left = false;
      } else {
        sibling_node = nullptr;
      }
    }
    if (sibling_node == nullptr) {
      return;  // there is no live sibling node
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    InternalNode *new_parent;
    bool install_success;
    do {
      // check whether a target node remains
      std::tie(current_leaf, target_index, trace) = TraceTargetNode(target_key, target_node);
      if (!HaveSameAddress(target_node, current_leaf)) {
        return;  // other threads have already performed merging
      }

      // create new nodes
      const auto deleted_index = (sibling_is_left) ? target_index - 1 : target_index;
      const auto sibling_meta = sibling_node->GatherSortedLiveMetadata(comparator_);
      const auto merged_node =
          LeafNode::Merge(target_node, sorted_meta, sibling_node, sibling_meta, sibling_is_left);
      parent = BitCast<InternalNode *>(trace.top().first);
      new_parent = InternalNode::NewParentForMerge(parent, merged_node, deleted_index);

      // try installation of new nodes
      install_success = InstallNewInternalNode(&trace, new_parent);
      if (install_success) {
        // Temporal implementation of garbage collection
        const auto reserved_index = ReserveGabageRegion(3);
        garbage_nodes[reserved_index] = const_cast<LeafNode *>(target_node);
        garbage_nodes[reserved_index + 1] = sibling_node;
        garbage_nodes[reserved_index + 2] = parent;
      } else {
        delete merged_node;
        delete new_parent;
      }
    } while (!install_success);

    // check whether it is required to merge a new parent node
    if (!HaveSameAddress(new_parent, GetRoot())
        && new_parent->GetStatusWord().GetOccupiedSize() < min_node_size_) {
      // invoke a parent (internal) node merging
      MergeInternalNodes(new_parent, target_key, target_key_length);
    }
  }

  void
  MergeInternalNodes(  //
      InternalNode *target_node,
      const void *target_key,
      const size_t target_key_length)
  {
    // variables shared by phase 1 & 2
    InternalNode *sibling_node = nullptr;
    bool sibling_is_left;
    pmwcas::Descriptor *desc;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------------------*/

    do {
      // check a target node is not frozen and live
      const auto target_status = target_node->GetStatusWordProtected(descriptor_pool_->GetEpoch());
      if (target_status.IsFrozen()) {
        return;  // other SMOs are modifying a target node
      }
      auto [current_node, target_index, trace] = TraceTargetNode(target_key, target_node);
      if (!HaveSameAddress(target_node, current_node)) {
        return;  // a target node is deleted by SMOs
      }

      // check a left/right sibling node is not frozen
      auto parent = BitCast<InternalNode *>(trace.top().first);
      const auto target_size = target_node->GetStatusWord().GetOccupiedSize();
      StatusWord sibling_status;
      if (parent->CanMergeLeftSibling(target_index, target_size, max_merged_size_)) {
        sibling_node = BitCast<InternalNode *>(parent->GetChildNode(target_index - 1));
        sibling_status = sibling_node->GetStatusWordProtected(descriptor_pool_->GetEpoch());
        if (!sibling_status.IsFrozen()) {
          sibling_is_left = true;
        } else {
          sibling_node = nullptr;
        }
      }
      if (sibling_node == nullptr
          && parent->CanMergeRightSibling(target_index, target_size, max_merged_size_)) {
        sibling_node = BitCast<InternalNode *>(parent->GetChildNode(target_index + 1));
        sibling_status = sibling_node->GetStatusWordProtected(descriptor_pool_->GetEpoch());
        if (!sibling_status.IsFrozen()) {
          sibling_is_left = false;
        } else {
          sibling_node = nullptr;
        }
      }
      if (sibling_node == nullptr) {
        return;  // there is no live sibling node
      }

      // freeze target and sibling nodes
      const auto frozen_target_status = target_status.Freeze();
      const auto frozen_sibling_status = sibling_status.Freeze();
      desc = descriptor_pool_->AllocateDescriptor();
      target_node->SetStatusForMwCAS(target_status, frozen_target_status, desc);
      sibling_node->SetStatusForMwCAS(sibling_status, frozen_sibling_status, desc);
    } while (!desc->MwCAS());

    /*----------------------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------------------*/

    InternalNode *new_parent;
    bool install_success;
    do {
      // check whether a target node remains
      auto [current_node, target_index, trace] = TraceTargetNode(target_key, target_node);
      if (current_node->IsLeaf()) {
        return;  // other threads have already performed SMOs
      }

      // create new nodes
      const auto deleted_index = (sibling_is_left) ? target_index - 1 : target_index;
      const auto merged_node = InternalNode::Merge(target_node, sibling_node, true);
      const auto parent = BitCast<InternalNode *>(trace.top().first);
      new_parent = InternalNode::NewParentForMerge(parent, merged_node, deleted_index);
      if (new_parent->GetSortedCount() == 1) {
        // if a merged node is an only child, swap it for a new parent node
        new_parent = merged_node;
      }

      // try installation of new nodes
      install_success = InstallNewInternalNode(&trace, new_parent);
      if (install_success) {
        // Temporal implementation of garbage collection
        const auto reserved_index = ReserveGabageRegion(3);
        garbage_nodes[reserved_index] = target_node;
        garbage_nodes[reserved_index + 1] = sibling_node;
        garbage_nodes[reserved_index + 2] = parent;
      } else {
        delete merged_node;
        delete new_parent;
      }
    } while (!install_success);

    // check whether it is required to merge a parent node
    if (!HaveSameAddress(new_parent, GetRoot())
        && new_parent->GetStatusWord().GetOccupiedSize() < min_node_size_) {
      // invoke a parent (internal) node merging
      MergeInternalNodes(new_parent, target_key, target_key_length);
    }
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
      old_root_node->SetStatusForMwCAS(status, frozen_status, pd);
      SetRootForMwCAS(old_root_node, new_internal_node, pd);
    }

    return pd->MwCAS();
  }

 public:
  /*################################################################################################
   * Public constructor/destructor
   *##############################################################################################*/

  explicit BzTree(const size_t node_size = 4096,
                  const size_t min_node_size = 256,
                  const size_t min_free_space = 3072,
                  const size_t expected_free_space = 512,
                  const size_t max_deleted_size = 1024,
                  const size_t max_merged_size = 2048)
      : node_size_{node_size},
        min_node_size_{min_node_size},
        min_free_space_{min_free_space},
        expected_free_space_{expected_free_space},
        max_deleted_size_{max_deleted_size},
        max_merged_size_{max_merged_size},
        index_epoch_{0}
  {
    // initialize a MwCAS descriptor pool
    pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create, pmwcas::DefaultAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
    if (const auto cpu_num = std::thread::hardware_concurrency(); cpu_num > 0) {
      descriptor_pool_.reset(new pmwcas::DescriptorPool{kDescriptorPoolSize, cpu_num, false});
    } else {
      // if the program cannot recognize the number of CPU cores, use 64 partitions as default
      descriptor_pool_.reset(new pmwcas::DescriptorPool{kDescriptorPoolSize, 64, false});
    }

    // initialize a tree structure: one internal node with one leaf node
    const auto root_node = InternalNode::CreateInitialRoot(node_size_);
    root_.payload = PtrPayload{root_node};
  }

  BzTree(const BzTree &) = delete;
  BzTree &operator=(const BzTree &) = delete;
  BzTree(BzTree &&) = default;
  BzTree &operator=(BzTree &&) = default;
  ~BzTree() = default;

  /*################################################################################################
   * Public read APIs
   *##############################################################################################*/

  std::pair<ReturnCode, std::unique_ptr<std::byte[]>>
  Read(const void *key)
  {
    auto leaf_node = SearchLeafNode(key, true);
    auto [return_code, payload] = leaf_node->Read(key, comparator_, descriptor_pool_->GetEpoch());
    if (return_code == BaseNode::NodeReturnCode::kSuccess) {
      return std::pair{ReturnCode::kSuccess, std::move(payload)};
    } else {
      return {ReturnCode::kKeyNotExist, nullptr};
    }
  }

  std::pair<ReturnCode,
            std::vector<std::pair<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>>>>
  Scan(  //
      const void *begin_key,
      bool begin_is_closed,
      const void *end_key,
      const bool end_is_closed)
  {
    std::vector<std::pair<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>>> all_results;
    while (true) {
      auto [return_code, leaf_results] =
          ScanPerLeaf(begin_key, begin_is_closed, end_key, end_is_closed);
      // concatanate scan results for each leaf node
      all_results.reserve(all_results.size() + leaf_results.size());
      all_results.insert(all_results.end(), std::make_move_iterator(leaf_results.begin()),
                         std::make_move_iterator(leaf_results.end()));
      if (return_code == ReturnCode::kScanInProgress) {
        begin_key = all_results.back().first.get();
        begin_is_closed = false;
      } else {
        break;
      }
    }
    return {ReturnCode::kSuccess, std::move(all_results)};
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
    auto [return_code, scan_results] =
        leaf_node->Scan(begin_key, begin_is_closed, end_key, end_is_closed, comparator_,
                        descriptor_pool_->GetEpoch());
    if (return_code == BaseNode::NodeReturnCode::kScanInProgress) {
      return {ReturnCode::kScanInProgress, std::move(scan_results)};
    } else {
      return {ReturnCode::kSuccess, std::move(scan_results)};
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
      std::thread t(&BzTree::ConsolidateLeafNode, this, leaf_node, key, key_length);
      t.join();
      // t.detach();
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
      std::thread t(&BzTree::ConsolidateLeafNode, this, leaf_node, key, key_length);
      t.join();
      // t.detach();
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
      std::thread t(&BzTree::ConsolidateLeafNode, this, leaf_node, key, key_length);
      t.join();
      // t.detach();
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
      std::thread t(&BzTree::ConsolidateLeafNode, this, leaf_node, key, key_length);
      t.join();
      // t.detach();
    }
    return ReturnCode::kSuccess;
  }
};

}  // namespace bztree
