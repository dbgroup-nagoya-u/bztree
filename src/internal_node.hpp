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
   * Internal getters/setters
   *##############################################################################################*/

  constexpr BaseNode *
  GetChildNode(const size_t index) const
  {
    return BitCast<BaseNode *>(GetPayloadAddr(index));
  }

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  static void
  CopySortedRecords(  //
      InternalNode *target_node,
      const InternalNode *original_node,
      const size_t begin_index,
      const size_t end_index)
  {
    auto sorted_count = target_node->GetSortedCount();
    auto offset = target_node->GetNodeSize() - target_node->GetStatusWord().GetBlockSize();
    for (size_t index = begin_index; index < end_index; ++index) {
      const auto meta = original_node->GetMetadata(index);
      // copy a record
      const auto key = original_node->GetKeyAddr(meta);
      const auto key_length = meta.GetKeyLength();
      const auto payload = original_node->GetPayloadAddr(meta);
      const auto payload_length = meta.GetPayloadLength();
      offset = target_node->CopyRecord(key, key_length, payload, payload_length, offset);
      // copy metadata
      const auto new_meta = meta.UpdateOffset(offset);
      target_node->SetMetadata(sorted_count++, new_meta);
    }
    target_node->SetStatusWord(kInitStatusWord.AddRecordInfo(sorted_count, offset, 0));
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
    const auto new_block_size =
        GetStatusWord().GetOccupiedSize() - kWordLength - (key_length + payload_length);
    return new_block_size < min_node_size;
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
};

}  // namespace bztree
