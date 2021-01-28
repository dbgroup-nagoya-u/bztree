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
   * Internal utility functions
   *##############################################################################################*/

  void
  CopySortedRecords(  //
      InternalNode *original_node,
      const size_t begin_index,
      const size_t end_index)
  {
    auto sorted_count = GetSortedCount();
    auto offset = GetNodeSize() - GetBlockSize();
    for (size_t index = begin_index; index < end_index; ++index) {
      const auto meta = original_node->GetMetadata(index);
      // copy a record
      const auto key = original_node->GetKeyPtr(meta);
      const auto key_length = meta.GetKeyLength();
      const auto payload = original_node->GetPayloadPtr(meta);
      const auto payload_length = meta.GetPayloadLength();
      offset = CopyRecord(key, key_length, payload, payload_length, offset);
      // copy metadata
      const auto new_meta = meta.UpdateOffset(offset);
      SetMetadata(sorted_count++, new_meta);
    }
    SetStatusWord(kInitStatusWord.AddRecordInfo(sorted_count, offset, 0));
    SetSortedCount(sorted_count);
  }

 public:
  /*################################################################################################
   * Public constructor/destructor
   *##############################################################################################*/

  explicit InternalNode(const size_t node_size) : BaseNode{node_size, false} {}

  InternalNode(const InternalNode &) = delete;
  InternalNode &operator=(const InternalNode &) = delete;
  InternalNode(InternalNode &&) = default;
  InternalNode &operator=(InternalNode &&) = default;
  ~InternalNode() = default;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  bool
  NeedSplit(  //
      const size_t key_length,
      const size_t payload_length)
  {
    const auto metadata_size = kWordLength * (GetRecordCount() + 1);
    const auto block_size = key_length + payload_length + GetBlockSize();
    return (BaseNode::kHeaderLength + metadata_size + block_size) > GetNodeSize();
  }

  bool
  NeedMerge(  //
      const size_t key_length,
      const size_t payload_length,
      const size_t min_node_size)
  {
    const auto metadata_size = kWordLength * (GetRecordCount() - 1);
    const auto block_size = GetBlockSize() - (key_length + payload_length);
    return (BaseNode::kHeaderLength + metadata_size + block_size) < min_node_size;
  }

  bool
  CanMergeLeftSibling(  //
      const size_t index,
      const size_t merged_node_size,
      const size_t max_merged_node_size)
  {
    assert(index < GetSortedCount());  // an input index must be within range

    if (index == 0) {
      return false;
    } else {
      auto child_node = GetChildNode(index - 1);
      return (child_node->GetApproximateDataSize() + merged_node_size) < max_merged_node_size;
    }
  }

  bool
  CanMergeRightSibling(  //
      const size_t index,
      const size_t merged_node_size,
      const size_t max_merged_node_size)
  {
    assert(index < GetSortedCount());  // an input index must be within range

    if (index == GetSortedCount() - 1) {
      return false;
    } else {
      auto child_node = GetChildNode(index + 1);
      return (child_node->GetApproximateDataSize() + merged_node_size) < max_merged_node_size;
    }
  }

  size_t
  GetOccupiedSize()
  {
    return kHeaderLength + (GetSortedCount() * kWordLength) + GetBlockSize();
  }

  BaseNode *
  GetChildNode(const size_t index)
  {
    return reinterpret_cast<BaseNode *>(GetPayloadPtr(GetMetadata(index)));
  }

  template <class Compare>
  std::pair<BaseNode *, size_t>
  SearchChildNode(  //
      const std::byte *key,
      const bool range_is_closed,
      Compare comp)
  {
    const auto index = SearchSortedMetadata(key, range_is_closed, comp).second;
    return {GetChildNode(index), index};
  }

  /*################################################################################################
   * Public structure modification operations
   *##############################################################################################*/

  std::pair<BaseNode *, BaseNode *>
  Split(const size_t left_record_count)
  {
    const auto node_size = GetNodeSize();

    // create a split left node
    auto left_node = new InternalNode{node_size};
    left_node->CopySortedRecords(this, 0, left_record_count);

    // create a split right node
    auto right_node = new InternalNode{node_size};
    right_node->CopySortedRecords(this, left_record_count, GetSortedCount());

    return {reinterpret_cast<BaseNode *>(left_node), reinterpret_cast<BaseNode *>(right_node)};
  }

  BaseNode *
  Merge(  //
      InternalNode *sibling_node,
      const bool sibling_is_left)
  {
    const auto node_size = GetNodeSize();

    // create a merged node
    auto merged_node = new InternalNode{node_size};
    if (sibling_is_left) {
      merged_node->CopySortedRecords(sibling_node, 0, sibling_node->GetSortedCount());
      merged_node->CopySortedRecords(this, 0, GetSortedCount());
    } else {
      merged_node->CopySortedRecords(this, 0, GetSortedCount());
      merged_node->CopySortedRecords(sibling_node, 0, sibling_node->GetSortedCount());
    }

    return reinterpret_cast<BaseNode *>(merged_node);
  }

  BaseNode *
  NewParentForSplit(  //
      BaseNode *left_child,
      BaseNode *right_child,
      const size_t split_index)
  {
    auto offset = GetNodeSize();
    auto new_parent = new InternalNode{offset};

    // copy child nodes with inserting new split ones
    auto record_count = GetSortedCount();
    for (size_t old_idx = 0, new_idx = 0; old_idx < record_count; ++old_idx, ++new_idx) {
      // prepare copying record and metadata
      const auto meta = GetMetadata(old_idx);
      const auto key = GetKeyPtr(meta);
      const auto key_length = meta.GetKeyLength();
      auto node_addr = GetPayloadPtr(meta);
      if (old_idx == split_index) {
        // prepare left child information
        const auto last_meta = left_child->GetMetadata(left_child->GetSortedCount() - 1);
        const auto new_key = reinterpret_cast<InternalNode *>(left_child)->GetKeyPtr(last_meta);
        const auto new_key_length = last_meta.GetKeyLength();
        const auto left_addr = reinterpret_cast<std::byte *>(left_child);
        // insert a split left child
        offset = new_parent->CopyRecord(new_key, new_key_length, left_addr, kPointerLength, offset);
        const auto total_length = new_key_length + kPointerLength;
        const auto left_meta = kInitMetadata.SetRecordInfo(offset, new_key_length, total_length);
        new_parent->SetMetadata(new_idx++, left_meta);
        // insert a split right child
        node_addr = reinterpret_cast<std::byte *>(right_child);
      }
      // copy a child node
      offset = new_parent->CopyRecord(key, key_length, node_addr, kPointerLength, offset);
      const auto new_meta = meta.UpdateOffset(offset);
      new_parent->SetMetadata(new_idx, new_meta);
    }

    // set a new header
    SetSortedCount(++record_count);
    SetStatusWord(kInitStatusWord.AddRecordInfo(record_count, offset, 0));

    return reinterpret_cast<BaseNode *>(new_parent);
  }

  static BaseNode *
  NewRoot(  //
      BaseNode *left_child,
      BaseNode *right_child)
  {
    auto offset = left_child->GetNodeSize();
    auto new_root = new InternalNode{offset};

    // insert a left child node
    auto meta = left_child->GetMetadata(left_child->GetSortedCount() - 1);
    auto key = reinterpret_cast<InternalNode *>(left_child)->GetKeyPtr(meta);
    auto key_length = meta.GetKeyLength();
    auto node_addr = reinterpret_cast<std::byte *>(left_child);
    offset = new_root->CopyRecord(key, key_length, node_addr, kPointerLength, offset);
    auto new_meta = kInitMetadata.SetRecordInfo(offset, key_length, key_length + kPointerLength);
    new_root->SetMetadata(0, new_meta);

    // insert a right child node
    meta = right_child->GetMetadata(right_child->GetSortedCount() - 1);
    key = reinterpret_cast<InternalNode *>(right_child)->GetKeyPtr(meta);
    key_length = meta.GetKeyLength();
    node_addr = reinterpret_cast<std::byte *>(right_child);
    offset = new_root->CopyRecord(key, key_length, node_addr, kPointerLength, offset);
    new_meta = kInitMetadata.SetRecordInfo(offset, key_length, key_length + kPointerLength);
    new_root->SetMetadata(1, new_meta);

    // set a new header
    new_root->SetSortedCount(2);
    new_root->SetStatusWord(kInitStatusWord.AddRecordInfo(2, offset, 0));

    return reinterpret_cast<BaseNode *>(new_root);
  }

  template <class Compare>
  BaseNode *
  NewParentForMerge(  //
      BaseNode *merged_child,
      const size_t deleted_index,
      Compare comp)
  {
    auto offset = GetNodeSize();
    auto new_parent = new InternalNode{offset};

    // copy child nodes with deleting a merging target node
    auto record_count = GetSortedCount();
    for (size_t old_idx = 0, new_idx = 0; old_idx < record_count; ++old_idx, ++new_idx) {
      // prepare copying record and metadata
      auto meta = GetMetadata(old_idx);
      auto key = GetKeyPtr(meta);
      auto key_length = meta.GetKeyLength();
      auto node_addr = GetPayloadPtr(meta);
      if (old_idx == deleted_index) {
        // skip a deleted node and insert a merged node
        meta = GetMetadata(++old_idx);
        key = GetKeyPtr(meta);
        key_length = meta.GetKeyLength();
        node_addr = reinterpret_cast<std::byte *>(merged_child);
      }
      // copy a child node
      offset = new_parent->CopyRecord(key, key_length, node_addr, kPointerLength, offset);
      const auto new_meta = meta.UpdateOffset(offset);
      new_parent->SetMetadata(new_idx, new_meta);
    }

    // set a new header
    new_parent->SetSortedCount(--record_count);
    new_parent->SetStatusWord(kInitStatusWord.AddRecordInfo(record_count, offset, 0));

    return reinterpret_cast<BaseNode *>(new_parent);
  }
};

}  // namespace bztree
