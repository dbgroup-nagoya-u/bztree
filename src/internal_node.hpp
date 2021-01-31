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
      const void *key,
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
};

}  // namespace bztree
