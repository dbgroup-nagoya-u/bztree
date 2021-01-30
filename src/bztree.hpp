// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <functional>
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
template <class Key, class Payload, class Compare = std::less<Key>>
class BzTree
{
  using NodeReturnCode = BaseNode<Key, Payload, Compare>::NodeReturnCode;

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
  BaseNode<Key, Payload, Compare> *root_;
  std::unique_ptr<pmwcas::DescriptorPool> descriptor_pool_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  LeafNode<Key, Payload, Compare> *
  SearchLeafNode(  //
      const Key key,
      const bool range_is_closed) const
  {
    assert(!root_->IsLeaf());  // a root node must be an internal node

    auto current_node = root_;
    do {
      current_node = dynamic_cast<InternalNode<Key, Payload, Compare> *>(current_node)
                         ->SearchChildNode(key, range_is_closed, comparator_)
                         .first;
    } while (!current_node->IsLeaf());
    return dynamic_cast<LeafNode<Key, Payload, Compare> *>(current_node);
  }

  std::stack<std::pair<BaseNode<Key, Payload, Compare> *, size_t>>
  SearchLeafNodeWithTrace(const Key key) const
  {
    assert(!root_->IsLeaf());  // a root node must be an internal node

    // set a root node
    auto current_node = root_;
    size_t index = 0;

    // trace nodes to a target leaf node
    std::stack<std::pair<BaseNode<Key, Payload, Compare> *, size_t>> trace;
    trace.emplace(current_node, index);
    do {
      std::tie(current_node, index) =
          dynamic_cast<InternalNode<Key, Payload, Compare> *>(current_node)
              ->SearchChildNode(key, true, comparator_);
      trace.emplace(current_node, index);
    } while (!current_node->IsLeaf());

    return trace;
  }

  std::stack<std::pair<BaseNode<Key, Payload, Compare> *, size_t>>
  SearchInternalNodeWithTrace(  //
      const Key key,
      InternalNode<Key, Payload, Compare> *target_node) const
  {
    assert(!root_->IsLeaf());  // a root node must be an internal node

    // set a root node
    auto current_node = root_;
    size_t index = 0;

    // trace nodes to a target internal node
    std::stack<std::pair<BaseNode<Key, Payload, Compare> *, size_t>> trace;
    trace.emplace(current_node, index);
    do {
      if (HaveSameAddress(current_node, target_node)) {
        // find a target node, so return the trace
        return trace;
      }
      std::tie(current_node, index) =
          dynamic_cast<InternalNode<Key, Payload, Compare> *>(current_node)
              ->SearchChildNode(key, true, comparator_);
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

  uint32_t
  SetRootForMwCAS(  //
      BaseNode<Key, Payload, Compare> *old_root_node,
      BaseNode<Key, Payload, Compare> *new_root_node,
      pmwcas::Descriptor *descriptor)
  {
    return descriptor->AddEntry(CastToUint64Ptr(&root_), CastToUint64(old_root_node),
                                CastToUint64(new_root_node));
  }

  std::pair<const std::byte *, size_t>
  SearchSeparatorKey(  //
      std::map<const std::byte *, Metadata>::iterator meta_iter,
      const size_t half_cout)
  {
    for (size_t index = 0; index < half_cout; ++index) {
      ++meta_iter;
    }
    return {meta_iter->first, meta_iter->second.GetKeyLength()};
  }

  /*################################################################################################
   * Internal structure modification functoins
   *##############################################################################################*/

  void
  ConsolidateLeafNode(  //
      LeafNode<Key, Payload, Compare> *target_leaf,
      const Key target_key,
      const size_t target_key_length)
  {
    // freeze a target node and perform consolidation
    target_leaf->Freeze(descriptor_pool_.get());

    // gather sorted live metadata of a targetnode, and check whether split/merge is required
    const auto live_meta = target_leaf->GatherSortedLiveMetadata(comparator_);
    const auto occupied_size = BaseNode<Key, Payload, Compare>::ComputeOccupiedSize(live_meta);
    if (occupied_size + desired_free_space_ > node_size_) {
      SplitLeafNode(target_leaf, target_key, live_meta);
      return;
    } else if (occupied_size < node_size_min_threshold_) {
      MergeLeafNode(target_leaf, target_key, target_key_length, occupied_size, live_meta);
      return;
    }

    // install a new node
    auto new_leaf = target_leaf->Consolidate(live_meta);
    pmwcas::Descriptor *pd;
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
      const auto parent_status = parent_node->GetStatusWordProtected();
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
      LeafNode<Key, Payload, Compare> *target_leaf,
      const Key target_key,
      const std::vector<std::pair<std::byte *, Metadata>> &sorted_meta)
  {
    assert(target_leaf->IsFrozen());  // a splitting node must be locked

    // get a separator key and its length
    const auto left_record_count = (sorted_meta.size() / 2);
    const auto [split_key, split_key_length] =
        SearchSeparatorKey(sorted_meta.begin(), left_record_count);

    bool install_success;
    do {
      // check whether a target node remains
      auto trace = SearchLeafNodeWithTrace(target_key);
      auto [current_leaf, target_index] = trace.top();
      if (!HaveSameAddress(target_leaf, current_leaf)) {
        return;  // other threads have already performed splitting
      }

      // check whether it is required to split a parent node
      trace.pop();  // remove a leaf node
      auto parent = dynamic_cast<InternalNode<Key, Payload, Compare> *>(trace.top().first);
      if (parent->NeedSplit(split_key_length, kPointerLength)) {
        // invoke a parent (internal) node splitting
        SplitInternalNode(parent, target_key);
        continue;
      }

      // create new nodes
      auto [left_leaf, right_leaf] = target_leaf->Split(sorted_meta, left_record_count);
      auto new_parent = parent->NewParentForSplit(left_leaf, right_leaf, target_index);

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
      InternalNode<Key, Payload, Compare> *target_node,
      const Key target_key)
  {
    assert(target_node->IsFrozen());  // a splitting node must be locked

    // get a split index and a corresponding key length
    const auto left_record_count = (target_node->GetSortedCount() / 2);
    const auto split_key_length = target_node->GetKeyLength(left_record_count - 1);

    bool install_success;
    do {
      // check whether a target node remains
      auto trace = SearchInternalNodeWithTrace(target_key, target_node);
      auto [current_node, target_index] = trace.top();
      if (current_node->IsLeaf()) {
        // there is no target node, because other threads have already performed SMOs
        return;
      }

      // create new nodes
      BaseNode<Key, Payload, Compare> *new_parent, *left_node, *right_node;
      if (trace.size() == 1) {
        // split a root node
        std::tie(left_node, right_node) = target_node->Split(left_record_count);
        new_parent = InternalNode<Key, Payload, Compare>::NewRoot(left_node, right_node);
      } else {
        // check whether it is required to split a parent node
        trace.pop();  // remove a target node
        auto parent = dynamic_cast<InternalNode<Key, Payload, Compare> *>(trace.top().first);
        if (parent->NeedSplit(split_key_length, kPointerLength)) {
          // invoke a parent (internal) node splitting
          SplitInternalNode(parent, target_key);
          continue;
        }
        std::tie(left_node, right_node) = target_node->Split(left_record_count);
        new_parent = parent->NewParentForSplit(left_node, right_node, target_index);
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
      LeafNode<Key, Payload, Compare> *target_node,
      const Key target_key,
      const size_t target_key_length,
      const size_t target_size,
      const std::vector<std::pair<std::byte *, Metadata>> &sorted_meta)
  {
    assert(target_node->IsFrozen());  // a merging node must be locked

    bool install_success;
    do {
      // check whether a target node remains
      auto trace = SearchLeafNodeWithTrace(target_key);
      auto [current_leaf, target_index] = trace.top();
      if (!HaveSameAddress(target_node, current_leaf)) {
        return;  // other threads have already performed merging
      }

      // check whether it is required to merge a parent node
      trace.pop();  // remove a leaf node
      auto parent = dynamic_cast<InternalNode<Key, Payload, Compare> *>(trace.top().first);
      if (parent->NeedMerge(target_key_length, kPointerLength, node_size_min_threshold_)) {
        // invoke a parent (internal) node merging
        MergeInternalNodes(parent, target_key, target_key_length);
        continue;
      }

      // create new nodes
      BaseNode<Key, Payload, Compare> *merged_node, *new_parent;
      LeafNode<Key, Payload, Compare> *sibling_node;
      size_t deleted_index;
      if (parent->CanMergeLeftSibling(target_index, target_size, max_merged_size_)) {
        deleted_index = target_index - 1;
        sibling_node =
            dynamic_cast<LeafNode<Key, Payload, Compare> *>(parent->GetChildNode(deleted_index));
        const auto sibling_meta = sibling_node->GatherSortedLiveMetadata(comparator_);
        merged_node = target_node->Merge(sorted_meta, sibling_node, sibling_meta, true);
      } else if (parent->CanMergeRightSibling(target_index, target_size, max_merged_size_)) {
        const auto right_index = target_index + 1;
        deleted_index = target_index;
        sibling_node =
            dynamic_cast<LeafNode<Key, Payload, Compare> *>(parent->GetChildNode(right_index));
        const auto sibling_meta = sibling_node->GatherSortedLiveMetadata(comparator_);
        merged_node = target_node->Merge(sorted_meta, sibling_node, sibling_meta, false);
      } else {
        return;  // there is no space to perform merge operation
      }
      new_parent = parent->NewParentForMerge(merged_node, deleted_index, comparator_);

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
      InternalNode<Key, Payload, Compare> *target_node,
      const Key target_key,
      const size_t target_key_length)
  {
    assert(target_node->IsFrozen());  // a merging node must be locked

    bool install_success;
    do {
      // check whether a target node remains
      auto trace = SearchInternalNodeWithTrace(target_key, target_node);
      auto [current_node, target_index] = trace.top();
      if (current_node->IsLeaf()) {
        // there is no target node, because other threads have already performed SMOs
        return;
      }

      // check whether it is required to merge a parent node
      trace.pop();  // remove a target node
      auto parent = dynamic_cast<InternalNode<Key, Payload, Compare> *>(trace.top().first);
      if (!HaveSameAddress(parent, root_)
          && parent->NeedMerge(target_key_length, kPointerLength, node_size_min_threshold_)) {
        // invoke a parent (internal) node merging
        MergeInternalNodes(parent, target_key, target_key_length);
        continue;
      }

      // create new nodes
      BaseNode<Key, Payload, Compare> *merged_node, *new_parent;
      InternalNode<Key, Payload, Compare> *sibling_node;
      size_t deleted_index;
      const auto target_size = target_node->GetOccupiedSize();
      if (parent->CanMergeLeftSibling(target_index, target_size, max_merged_size_)) {
        deleted_index = target_index - 1;
        sibling_node = dynamic_cast<InternalNode<Key, Payload, Compare> *>(
            parent->GetChildNode(deleted_index));
        merged_node = target_node->Merge(sibling_node, true);
      } else if (parent->CanMergeRightSibling(target_index, target_size, max_merged_size_)) {
        const auto right_index = target_index + 1;
        deleted_index = target_index;
        sibling_node =
            dynamic_cast<InternalNode<Key, Payload, Compare> *>(parent->GetChildNode(right_index));
        merged_node = target_node->Merge(sibling_node, false);
      } else {
        return;  // there is no space to perform merge operation
      }
      new_parent = parent->NewParentForMerge(merged_node, deleted_index, comparator_);

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
      std::stack<std::pair<BaseNode<Key, Payload, Compare> *, size_t>> *trace,
      BaseNode<Key, Payload, Compare> *new_internal_node)
  {
    auto *pd = descriptor_pool_->AllocateDescriptor();

    if (trace->size() > 1) {
      /*--------------------------------------------------------------------------------------------
       * Swapping a new internal node
       *------------------------------------------------------------------------------------------*/

      // prepare installing nodes
      auto [old_internal_node, swapping_index] = trace->top();
      trace->pop();
      auto parent_node = trace->top().first;

      // check wether related nodes are frozen
      const auto status = old_internal_node->GetStatusWordProtected();
      const auto parent_status = parent_node->GetStatusWordProtected();
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
      const auto status = old_root_node->GetStatusWordProtected();
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

  std::pair<ReturnCode, std::unique_ptr<Payload>>
  Read(const Key key)
  {
    auto leaf_node = SearchLeafNode(key, true);
    const auto [return_code, payload] = leaf_node->Read(key, comparator_);
    if (return_code == NodeReturnCode::kSuccess) {
      return {ReturnCode::kSuccess, payload};
    } else {
      return {ReturnCode::kKeyNotExist, nullptr};
    }
  }

  std::pair<ReturnCode, std::vector<std::pair<std::unique_ptr<Key>, std::unique_ptr<Payload>>>>
  Scan(  //
      const Key begin_key,
      const bool begin_is_closed,
      const Key end_key,
      const bool end_is_closed)
  {
    std::vector<std::pair<std::unique_ptr<Key>, std::unique_ptr<Payload>>> all_results;
    NodeReturnCode scan_in_progress;
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
    } while (scan_in_progress == NodeReturnCode::kScanInProgress);

    return {ReturnCode::kSuccess, all_results};
  }

  std::pair<ReturnCode, std::vector<std::pair<std::unique_ptr<Key>, std::unique_ptr<Payload>>>>
  ScanPerLeaf(  //
      const Key begin_key,
      const bool begin_is_closed,
      const Key end_key,
      const bool end_is_closed)
  {
    auto leaf_node = SearchLeafNode(begin_key, begin_is_closed);
    const auto [return_code, scan_results] =
        leaf_node->Scan(begin_key, begin_is_closed, end_key, end_is_closed, comparator_);
    if (return_code == NodeReturnCode::kScanInProgress) {
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
      const Key key,
      const size_t key_length,
      const Payload payload,
      const size_t payload_length)
  {
    LeafNode<Key, Payload, Compare> *leaf_node;
    NodeReturnCode return_code;
    StatusWord node_status;
    bool is_retry = false;
    do {
      leaf_node = SearchLeafNode(key, true);
      std::tie(return_code, node_status) = leaf_node->Write(
          key, key_length, payload, payload_length, index_epoch_, descriptor_pool_.get());
      if (is_retry && return_code == NodeReturnCode::kFrozen) {
        // invoke consolidation in this thread
        ConsolidateLeafNode(leaf_node, key, key_length);
        is_retry = false;
      } else {
        is_retry = true;
      }
    } while (return_code == NodeReturnCode::kFrozen);

    if (NeedConsolidation(node_status)) {
      // invoke consolidation with a new thread
      std::thread t(ConsolidateLeafNode, leaf_node, key, key_length);
      t.detach();
    }
    return ReturnCode::kSuccess;
  }

  ReturnCode
  Insert(  //
      const Key key,
      const size_t key_length,
      const Payload payload,
      const size_t payload_length)
  {
    LeafNode<Key, Payload, Compare> *leaf_node;
    NodeReturnCode return_code;
    StatusWord node_status;
    bool is_retry = false;
    do {
      leaf_node = SearchLeafNode(key, true);
      std::tie(return_code, node_status) =
          leaf_node->Insert(key, key_length, payload, payload_length, index_epoch_, comparator_,
                            descriptor_pool_.get());
      if (return_code == NodeReturnCode::kKeyExist) {
        return ReturnCode::kKeyExist;
      } else if (is_retry && return_code == NodeReturnCode::kFrozen) {
        // invoke consolidation in this thread
        ConsolidateLeafNode(leaf_node, key, key_length);
        is_retry = false;
      } else {
        is_retry = true;
      }
    } while (return_code == NodeReturnCode::kFrozen);

    if (NeedConsolidation(node_status)) {
      // invoke consolidation with a new thread
      std::thread t(ConsolidateLeafNode, leaf_node, key, key_length);
      t.detach();
    }
    return ReturnCode::kSuccess;
  }

  ReturnCode
  Update(  //
      const Key key,
      const size_t key_length,
      const Payload payload,
      const size_t payload_length)
  {
    LeafNode<Key, Payload, Compare> *leaf_node;
    NodeReturnCode return_code;
    StatusWord node_status;
    bool is_retry = false;
    do {
      leaf_node = SearchLeafNode(key, true);
      std::tie(return_code, node_status) =
          leaf_node->Update(key, key_length, payload, payload_length,  //
                            index_epoch_, comparator_, descriptor_pool_.get());
      if (return_code == NodeReturnCode::kKeyNotExist) {
        return ReturnCode::kKeyNotExist;
      } else if (is_retry && return_code == NodeReturnCode::kFrozen) {
        // invoke consolidation in this thread
        ConsolidateLeafNode(leaf_node, key, key_length);
        is_retry = false;
      } else {
        is_retry = true;
      }
    } while (return_code == NodeReturnCode::kFrozen);

    if (NeedConsolidation(node_status)) {
      // invoke consolidation with a new thread
      std::thread t(ConsolidateLeafNode, leaf_node, key, key_length);
      t.detach();
    }
    return ReturnCode::kSuccess;
  }

  ReturnCode
  Delete(  //
      const Key key,
      const size_t key_length)
  {
    LeafNode<Key, Payload, Compare> *leaf_node;
    NodeReturnCode return_code;
    StatusWord node_status;
    bool is_retry = false;
    do {
      leaf_node = SearchLeafNode(key, true);
      std::tie(return_code, node_status) =
          leaf_node->Delete(key, key_length, comparator_, descriptor_pool_.get());
      if (return_code == NodeReturnCode::kKeyNotExist) {
        return ReturnCode::kKeyNotExist;
      } else if (is_retry && return_code == NodeReturnCode::kFrozen) {
        // invoke consolidation in this thread
        ConsolidateLeafNode(leaf_node, key, key_length);
        is_retry = false;
      } else {
        is_retry = true;
      }
    } while (return_code == NodeReturnCode::kFrozen);

    if (NeedConsolidation(node_status)) {
      // invoke consolidation with a new thread
      std::thread t(ConsolidateLeafNode, leaf_node, key, key_length);
      t.detach();
    }
    return ReturnCode::kSuccess;
  }
};

}  // namespace bztree
