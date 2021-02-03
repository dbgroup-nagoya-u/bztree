// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <utility>

#include "base_node.hpp"

namespace bztree
{
class InternalNode : public BaseNode
{
 private:
  /*################################################################################################
   * Internal constructor/destructor
   *##############################################################################################*/

  explicit InternalNode(const size_t node_size) : BaseNode{node_size, false} {}

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  constexpr BaseNode *
  GetChildNode(const size_t index) const
  {
    return static_cast<BaseNode *>(reinterpret_cast<void *>(PayloadToPtr(GetPayloadAddr(index))));
  }

  static constexpr size_t
  AlignKeyLengthToWord(const size_t key_length)
  {
    const auto pad_length = key_length % kWordLength;
    return (pad_length == 0) ? key_length : key_length + (kWordLength - pad_length);
  }

  static void
  CopySortedRecords(  //
      InternalNode *target_node,
      const InternalNode *original_node,
      const size_t begin_index,
      const size_t end_index)
  {
    const auto node_size = target_node->GetNodeSize();
    auto sorted_count = target_node->GetSortedCount();
    auto offset = node_size - target_node->GetStatusWord().GetBlockSize();
    for (size_t index = begin_index; index < end_index; ++index, ++sorted_count) {
      const auto meta = original_node->GetMetadata(index);
      // copy a record
      const auto key = original_node->GetKeyAddr(meta);
      const auto key_length = meta.GetKeyLength();
      const auto payload = original_node->GetPayloadAddr(meta);
      const auto payload_length = meta.GetPayloadLength();
      offset = target_node->CopyRecord(key, key_length, payload, payload_length, offset);
      // copy metadata
      const auto new_meta = meta.UpdateOffset(offset);
      target_node->SetMetadata(sorted_count, new_meta);
    }
    target_node->SetStatusWord(kInitStatusWord.AddRecordInfo(sorted_count, node_size - offset, 0));
    target_node->SetSortedCount(sorted_count);
  }

 public:
  /*################################################################################################
   * Public constructor/destructor
   *##############################################################################################*/

  InternalNode(const InternalNode &) = delete;
  InternalNode &operator=(const InternalNode &) = delete;
  InternalNode(InternalNode &&) = default;
  InternalNode &operator=(InternalNode &&) = default;
  ~InternalNode() = default;

  /*################################################################################################
   * Public builders
   *##############################################################################################*/

  static InternalNode *
  CreateEmptyNode(const size_t node_size)
  {
    assert((node_size % kWordLength) == 0);

    auto aligned_page = aligned_alloc(kWordLength, node_size);
    auto new_node = new (aligned_page) InternalNode{node_size};
    return new_node;
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr bool
  NeedSplit(  //
      const size_t key_length,
      const size_t payload_length) const
  {
    const auto new_block_size =
        GetStatusWord().GetOccupiedSize() + kWordLength + key_length + payload_length;
    return new_block_size > GetNodeSize();
  }

  constexpr bool
  NeedMerge(  //
      const size_t key_length,
      const size_t payload_length,
      const size_t min_node_size) const
  {
    const int64_t new_block_size =
        GetStatusWord().GetOccupiedSize() - kWordLength - (key_length + payload_length);
    return new_block_size < static_cast<int64_t>(min_node_size);
  }

  constexpr bool
  CanMergeLeftSibling(  //
      const size_t index,
      const size_t merged_node_size,
      const size_t max_merged_node_size) const
  {
    assert(index < GetSortedCount());  // an input index must be within range

    if (index == 0) {
      return false;
    } else {
      const auto data_size = GetChildNode(index - 1)->GetStatusWord().GetApproxDataSize();
      return (merged_node_size + data_size) < max_merged_node_size;
    }
  }

  constexpr bool
  CanMergeRightSibling(  //
      const size_t index,
      const size_t merged_node_size,
      const size_t max_merged_node_size) const
  {
    assert(index < GetSortedCount());  // an input index must be within range

    if (index == GetSortedCount() - 1) {
      return false;
    } else {
      const auto data_size = GetChildNode(index + 1)->GetStatusWord().GetApproxDataSize();
      return (merged_node_size + data_size) < max_merged_node_size;
    }
  }

  /*################################################################################################
   * Public structure modification operations
   *##############################################################################################*/

  static std::pair<InternalNode *, InternalNode *>
  Split(  //
      const InternalNode *target_node,
      const size_t left_record_count)
  {
    const auto node_size = target_node->GetNodeSize();

    // create a split left node
    auto left_node = CreateEmptyNode(node_size);
    left_node->CopySortedRecords(left_node, target_node, 0, left_record_count);

    // create a split right node
    auto right_node = CreateEmptyNode(node_size);
    right_node->CopySortedRecords(right_node, target_node, left_record_count,
                                  target_node->GetSortedCount());

    return {left_node, right_node};
  }

  static InternalNode *
  Merge(  //
      const InternalNode *target_node,
      const InternalNode *sibling_node,
      const bool sibling_is_left)
  {
    const auto node_size = target_node->GetNodeSize();

    // create a merged node
    auto merged_node = CreateEmptyNode(node_size);
    if (sibling_is_left) {
      merged_node->CopySortedRecords(merged_node, sibling_node, 0, sibling_node->GetSortedCount());
      merged_node->CopySortedRecords(merged_node, target_node, 0, target_node->GetSortedCount());
    } else {
      merged_node->CopySortedRecords(merged_node, target_node, 0, target_node->GetSortedCount());
      merged_node->CopySortedRecords(merged_node, sibling_node, 0, sibling_node->GetSortedCount());
    }

    return merged_node;
  }

  static InternalNode *
  CreateNewRoot(  //
      const InternalNode *left_child,
      const InternalNode *right_child)
  {
    const auto node_size = left_child->GetNodeSize();
    auto offset = node_size;
    auto new_root = CreateEmptyNode(offset);

    // insert a left child node
    const auto left_meta = left_child->GetMetadata(left_child->GetSortedCount() - 1);
    const auto left_key = left_child->GetKeyAddr(left_meta);
    const auto left_key_length = left_meta.GetKeyLength();
    offset = new_root->CopyRecord(left_key, left_key_length, &left_child, kWordLength, offset);
    const auto new_left_meta =
        kInitMetadata.SetRecordInfo(offset, left_key_length, left_key_length + kWordLength);
    new_root->SetMetadata(0, new_left_meta);

    // insert a right child node
    const auto right_meta = right_child->GetMetadata(right_child->GetSortedCount() - 1);
    const auto right_key = right_child->GetKeyAddr(right_meta);
    const auto right_key_length = right_meta.GetKeyLength();
    offset = new_root->CopyRecord(right_key, right_key_length, &right_child, kWordLength, offset);
    const auto new_right_meta =
        kInitMetadata.SetRecordInfo(offset, right_key_length, right_key_length + kWordLength);
    new_root->SetMetadata(1, new_right_meta);

    // set a new header
    new_root->SetSortedCount(2);
    new_root->SetStatusWord(kInitStatusWord.AddRecordInfo(2, node_size - offset, 0));

    return new_root;
  }

  static InternalNode *
  NewParentForSplit(  //
      const InternalNode *old_parent,
      const void *new_key,
      const size_t new_key_length,
      const void *left_addr,
      const void *right_addr,
      const size_t split_index)
  {
    const auto node_size = old_parent->GetNodeSize();
    auto offset = node_size;
    auto new_parent = CreateEmptyNode(offset);

    // copy child nodes with inserting new split ones
    auto record_count = old_parent->GetSortedCount();
    for (size_t old_idx = 0, new_idx = 0; old_idx < record_count; ++old_idx, ++new_idx) {
      // prepare copying record and metadata
      const auto meta = old_parent->GetMetadata(old_idx);
      const auto key = old_parent->GetKeyAddr(meta);
      const auto key_length = meta.GetKeyLength();
      auto node_addr = old_parent->GetPayloadAddr(meta);
      if (old_idx == split_index) {
        // insert a split left child
        const auto aligned_length = AlignKeyLengthToWord(new_key_length);
        offset = new_parent->CopyRecord(new_key, aligned_length, &left_addr, kWordLength, offset);
        const auto total_length = aligned_length + kWordLength;
        const auto left_meta = kInitMetadata.SetRecordInfo(offset, aligned_length, total_length);
        new_parent->SetMetadata(new_idx++, left_meta);
        // continue with a split right child
        node_addr = &right_addr;
      }
      // copy a child node
      offset = new_parent->CopyRecord(key, key_length, node_addr, kWordLength, offset);
      const auto new_meta = meta.UpdateOffset(offset);
      new_parent->SetMetadata(new_idx, new_meta);
    }

    // set a new header
    new_parent->SetSortedCount(++record_count);
    new_parent->SetStatusWord(kInitStatusWord.AddRecordInfo(record_count, node_size - offset, 0));

    return new_parent;
  }

  static InternalNode *
  NewParentForMerge(  //
      const InternalNode *old_parent,
      const void *merged_child_addr,
      const size_t deleted_index)
  {
    const auto node_size = old_parent->GetNodeSize();
    auto offset = node_size;
    auto new_parent = CreateEmptyNode(offset);

    // copy child nodes with deleting a merging target node
    auto record_count = old_parent->GetSortedCount();
    for (size_t old_idx = 0, new_idx = 0; old_idx < record_count; ++old_idx, ++new_idx) {
      // prepare copying record and metadata
      auto meta = old_parent->GetMetadata(old_idx);
      auto key = old_parent->GetKeyAddr(meta);
      auto key_length = meta.GetKeyLength();
      auto node_addr = old_parent->GetPayloadAddr(meta);
      if (old_idx == deleted_index) {
        // skip a deleted node and insert a merged node
        meta = old_parent->GetMetadata(++old_idx);
        key = old_parent->GetKeyAddr(meta);
        key_length = meta.GetKeyLength();
        node_addr = &merged_child_addr;
      }
      // copy a child node
      offset = new_parent->CopyRecord(key, key_length, node_addr, kWordLength, offset);
      const auto new_meta = meta.UpdateOffset(offset);
      new_parent->SetMetadata(new_idx, new_meta);
    }

    // set a new header
    new_parent->SetSortedCount(--record_count);
    new_parent->SetStatusWord(kInitStatusWord.AddRecordInfo(record_count, node_size - offset, 0));

    return new_parent;
  }
};

}  // namespace bztree
