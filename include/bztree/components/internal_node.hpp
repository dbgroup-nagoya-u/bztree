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

#include <functional>
#include <utility>

#include "base_node.hpp"

namespace dbgroup::index::bztree
{
template <class Key, class Payload, class Compare = std::less<Key>>
class InternalNode
{
 private:
  using BaseNode_t = BaseNode<Key, Payload, Compare>;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  static constexpr size_t
  AlignKeyLengthToWord(const size_t key_length)
  {
    const auto pad_length = key_length % kWordLength;
    return (pad_length == 0) ? key_length : key_length + (kWordLength - pad_length);
  }

  static constexpr size_t
  SetChild(  //
      BaseNode_t *node,
      const Key &key,
      const size_t key_length,
      const uintptr_t child_addr,
      size_t offset)
  {
    // align memory address
    offset -= kWordLength + offset % kWordLength;
    memcpy(ShiftAddress(node, offset), &child_addr, kWordLength);

    if (key_length > 0) {
      offset -= key_length;
      node->SetKey(key, key_length, offset);
    }

    return offset;
  }

  static constexpr void
  CopySortedRecords(  //
      BaseNode_t *target_node,
      const BaseNode_t *original_node,
      const size_t begin_index,
      const size_t end_index)
  {
    const auto node_size = target_node->GetNodeSize();
    auto record_count = target_node->GetSortedCount();
    auto offset = node_size - target_node->GetStatusWord().GetBlockSize();
    for (size_t index = begin_index; index < end_index; ++index, ++record_count) {
      const auto meta = original_node->GetMetadata(index);
      // copy a record
      const auto key = CastKey<Key>(original_node->GetKeyAddr(meta));
      const auto key_length = meta.GetKeyLength();
      const auto child_addr = GetChildAddrProtected(original_node, meta);
      offset = SetChild(target_node, key, key_length, child_addr, offset);
      // copy metadata
      const auto new_meta = meta.UpdateOffset(offset);
      target_node->SetMetadata(record_count, new_meta);
    }
    target_node->SetSortedCount(record_count);
    target_node->SetStatus(StatusWord{}.AddRecordInfo(record_count, node_size - offset, 0));
  }

 public:
  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  static constexpr uintptr_t
  GetChildAddrProtected(  //
      const BaseNode_t *node,
      const Metadata meta)
  {
    return ReadMwCASField<uintptr_t>(node->GetPayloadAddr(meta));
  }

  static constexpr BaseNode_t *
  GetChildNode(  //
      const BaseNode_t *node,
      const size_t index)
  {
    const auto meta = node->GetMetadata(index);
    return reinterpret_cast<BaseNode_t *>(GetChildAddrProtected(node, meta));
  }

  static constexpr bool
  NeedSplit(  //
      const BaseNode_t *node,
      const size_t key_length,
      const size_t payload_length)
  {
    const auto new_block_size = node->GetStatusWordProtected().GetOccupiedSize() + kWordLength
                                + key_length + payload_length;
    return new_block_size > node->GetNodeSize();
  }

  static constexpr bool
  NeedMerge(  //
      const BaseNode_t *node,
      const size_t key_length,
      const size_t payload_length,
      const size_t min_node_size)
  {
    const int64_t new_block_size = node->GetStatusWordProtected().GetOccupiedSize() - kWordLength
                                   - (key_length + payload_length);
    return new_block_size < static_cast<int64_t>(min_node_size);
  }

  static constexpr bool
  CanMergeLeftSibling(  //
      const BaseNode_t *node,
      const size_t index,
      const size_t merged_node_size,
      const size_t max_merged_node_size)
  {
    assert(index < node->GetSortedCount());  // an input index must be within range

    if (index == 0) {
      return false;
    } else {
      const auto data_size =
          GetChildNode(node, index - 1)->GetStatusWordProtected().GetLiveDataSize();
      return (merged_node_size + data_size) < max_merged_node_size;
    }
  }

  static constexpr bool
  CanMergeRightSibling(  //
      const BaseNode_t *node,
      const size_t index,
      const size_t merged_node_size,
      const size_t max_merged_node_size)
  {
    assert(index < node->GetSortedCount());  // an input index must be within range

    if (index == node->GetSortedCount() - 1) {
      return false;
    } else {
      const auto data_size =
          GetChildNode(node, index + 1)->GetStatusWordProtected().GetLiveDataSize();
      return (merged_node_size + data_size) < max_merged_node_size;
    }
  }

  /*################################################################################################
   * Public structure modification operations
   *##############################################################################################*/

  static constexpr BaseNode_t *
  CreateInitialRoot(const size_t node_size)
  {
    auto root = BaseNode_t::CreateEmptyNode(node_size, false);
    const auto leaf_node = BaseNode_t::CreateEmptyNode(node_size, true);

    constexpr auto key = Key{};  // act as a positive infinity value
    constexpr auto key_length = 0;
    const auto leaf_addr = reinterpret_cast<uintptr_t>(leaf_node);
    constexpr auto total_length = kWordLength;

    // set an inital leaf node
    const auto offset = SetChild(root, key, key_length, leaf_addr, node_size);
    const auto meta = Metadata{}.SetRecordInfo(offset, key_length, total_length);
    root->SetMetadata(0, meta);

    // set a new header
    root->SetSortedCount(1);
    root->SetStatus(StatusWord{}.AddRecordInfo(1, node_size - offset, 0));

    return root;
  }

  static constexpr std::pair<BaseNode_t *, BaseNode_t *>
  Split(  //
      const BaseNode_t *target_node,
      const size_t left_record_count)
  {
    const auto node_size = target_node->GetNodeSize();

    // create a split left node
    auto left_node = BaseNode_t::CreateEmptyNode(node_size, false);
    CopySortedRecords(left_node, target_node, 0, left_record_count);

    // create a split right node
    auto right_node = BaseNode_t::CreateEmptyNode(node_size, false);
    CopySortedRecords(right_node, target_node, left_record_count, target_node->GetSortedCount());

    return {left_node, right_node};
  }

  static constexpr BaseNode_t *
  Merge(  //
      const BaseNode_t *target_node,
      const BaseNode_t *sibling_node,
      const bool sibling_is_left)
  {
    const auto node_size = target_node->GetNodeSize();

    // create a merged node
    auto merged_node = BaseNode_t::CreateEmptyNode(node_size, false);
    if (sibling_is_left) {
      CopySortedRecords(merged_node, sibling_node, 0, sibling_node->GetSortedCount());
      CopySortedRecords(merged_node, target_node, 0, target_node->GetSortedCount());
    } else {
      CopySortedRecords(merged_node, target_node, 0, target_node->GetSortedCount());
      CopySortedRecords(merged_node, sibling_node, 0, sibling_node->GetSortedCount());
    }

    return merged_node;
  }

  static constexpr BaseNode_t *
  CreateNewRoot(  //
      const BaseNode_t *left_child,
      const BaseNode_t *right_child)
  {
    const auto node_size = left_child->GetNodeSize();
    auto offset = node_size;
    auto new_root = BaseNode_t::CreateEmptyNode(offset, false);

    // insert a left child node
    const auto left_meta = left_child->GetMetadata(left_child->GetSortedCount() - 1);
    const auto left_key = CastKey<Key>(left_child->GetKeyAddr(left_meta));
    const auto left_key_length = left_meta.GetKeyLength();
    const auto left_child_addr = reinterpret_cast<uintptr_t>(left_child);
    offset = SetChild(new_root, left_key, left_key_length, left_child_addr, offset);
    const auto new_left_meta =
        Metadata{}.SetRecordInfo(offset, left_key_length, left_key_length + kWordLength);
    new_root->SetMetadata(0, new_left_meta);

    // insert a right child node
    const auto right_meta = right_child->GetMetadata(right_child->GetSortedCount() - 1);
    const auto right_key = CastKey<Key>(right_child->GetKeyAddr(right_meta));
    const auto right_key_length = right_meta.GetKeyLength();
    const auto right_child_addr = reinterpret_cast<uintptr_t>(right_child);
    offset = SetChild(new_root, right_key, right_key_length, right_child_addr, offset);
    const auto new_right_meta =
        Metadata{}.SetRecordInfo(offset, right_key_length, right_key_length + kWordLength);
    new_root->SetMetadata(1, new_right_meta);

    // set a new header
    new_root->SetSortedCount(2);
    new_root->SetStatus(StatusWord{}.AddRecordInfo(2, node_size - offset, 0));

    return new_root;
  }

  static constexpr BaseNode_t *
  NewParentForSplit(  //
      const BaseNode_t *old_parent,
      const Key &new_key,
      const size_t new_key_length,
      const void *left_addr,
      const void *right_addr,
      const size_t split_index)
  {
    const auto node_size = old_parent->GetNodeSize();
    auto offset = node_size;
    auto new_parent = BaseNode_t::CreateEmptyNode(offset, false);

    // copy child nodes with inserting new split ones
    auto record_count = old_parent->GetSortedCount();
    for (size_t old_idx = 0, new_idx = 0; old_idx < record_count; ++old_idx, ++new_idx) {
      // prepare copying record and metadata
      const auto meta = old_parent->GetMetadata(old_idx);
      const auto key = CastKey<Key>(old_parent->GetKeyAddr(meta));
      const auto key_length = meta.GetKeyLength();
      auto node_addr = GetChildAddrProtected(old_parent, meta);
      if (old_idx == split_index) {
        // insert a split left child
        const auto left_addr_uintptr = reinterpret_cast<uintptr_t>(left_addr);
        const auto prev_offset = offset;
        offset = SetChild(new_parent, new_key, new_key_length, left_addr_uintptr, offset);
        const auto total_length = prev_offset - offset;
        const auto left_meta = Metadata{}.SetRecordInfo(offset, new_key_length, total_length);
        new_parent->SetMetadata(new_idx++, left_meta);
        // continue with a split right child
        node_addr = reinterpret_cast<uintptr_t>(right_addr);
      }
      // copy a child node
      offset = SetChild(new_parent, key, key_length, node_addr, offset);
      const auto new_meta = meta.UpdateOffset(offset);
      new_parent->SetMetadata(new_idx, new_meta);
    }

    // set a new header
    new_parent->SetSortedCount(++record_count);
    new_parent->SetStatus(StatusWord{}.AddRecordInfo(record_count, node_size - offset, 0));

    return new_parent;
  }

  static constexpr BaseNode_t *
  NewParentForMerge(  //
      const BaseNode_t *old_parent,
      const void *merged_child_addr,
      const size_t deleted_index)
  {
    const auto node_size = old_parent->GetNodeSize();
    auto offset = node_size;
    auto new_parent = BaseNode_t::CreateEmptyNode(offset, false);

    // copy child nodes with deleting a merging target node
    auto record_count = old_parent->GetSortedCount();
    for (size_t old_idx = 0, new_idx = 0; old_idx < record_count; ++old_idx, ++new_idx) {
      // prepare copying record and metadata
      auto meta = old_parent->GetMetadata(old_idx);
      auto key = CastKey<Key>(old_parent->GetKeyAddr(meta));
      auto key_length = meta.GetKeyLength();
      auto node_addr = GetChildAddrProtected(old_parent, meta);
      if (old_idx == deleted_index) {
        // skip a deleted node and insert a merged node
        meta = old_parent->GetMetadata(++old_idx);
        key = CastKey<Key>(old_parent->GetKeyAddr(meta));
        key_length = meta.GetKeyLength();
        node_addr = reinterpret_cast<uintptr_t>(merged_child_addr);
      }
      // copy a child node
      offset = SetChild(new_parent, key, key_length, node_addr, offset);
      const auto new_meta = meta.UpdateOffset(offset);
      new_parent->SetMetadata(new_idx, new_meta);
    }

    // set a new header
    new_parent->SetSortedCount(--record_count);
    new_parent->SetStatus(StatusWord{}.AddRecordInfo(record_count, node_size - offset, 0));

    return new_parent;
  }
};

}  // namespace dbgroup::index::bztree
