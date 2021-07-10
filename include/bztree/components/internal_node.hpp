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
  AlignOffset(const size_t offset)
  {
    if constexpr (std::is_same_v<Key, char *> || sizeof(Key) % kWordLength != 0) {
      return offset - (offset & (kWordLength - 1));
    }
    return offset;
  }

  static constexpr BaseNode_t *
  GetChildAddrProtected(  //
      const BaseNode_t *node,
      const Metadata meta)
  {
    return ReadMwCASField<BaseNode_t *>(node->GetPayloadAddr(meta));
  }

  static constexpr size_t
  SetChild(  //
      BaseNode_t *node,
      const Key key,
      const size_t key_length,
      const BaseNode_t *child_addr,
      size_t offset)
  {
    offset = node->SetPayload(AlignOffset(offset), child_addr, kWordLength);
    if (key_length > 0) {
      offset = node->SetKey(offset, key, key_length);
    }
    return offset;
  }

  static constexpr size_t
  InsertChild(  //
      BaseNode_t *target_node,
      const BaseNode_t *child_node,
      const size_t target_index,
      size_t offset)
  {
    const auto meta = child_node->GetMetadata(child_node->GetSortedCount() - 1);
    const auto key = child_node->GetKey(meta);
    const auto key_length = meta.GetKeyLength();
    offset = SetChild(target_node, key, key_length, child_node, offset);
    const auto inserted_meta =
        Metadata{}.SetRecordInfo(offset, key_length, key_length + kWordLength);
    target_node->SetMetadata(target_index, inserted_meta);

    return offset;
  }

  static constexpr size_t
  InsertChild(  //
      BaseNode_t *target_node,
      const BaseNode_t *child_node,
      const size_t target_index,
      size_t offset,
      const BaseNode_t *orig_node,
      const size_t orig_index)
  {
    const auto meta = orig_node->GetMetadata(orig_index);
    const auto key = orig_node->GetKey(meta);
    const auto key_length = meta.GetKeyLength();
    offset = SetChild(target_node, key, key_length, child_node, offset);
    target_node->SetMetadata(target_index, meta.UpdateOffset(offset));

    return offset;
  }

  static constexpr size_t
  CopySortedRecords(  //
      BaseNode_t *target_node,
      size_t record_count,
      size_t offset,
      const BaseNode_t *orig_node,
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index > 0);

    const auto end_offset = orig_node->GetMetadata(end_index - 1).GetOffset();
    const auto copy_end = AlignOffset(offset);

    // copy metadata
    if (record_count == 0 && begin_index == 0) {
      // directly copy metadata
      memcpy(ShiftAddress(target_node, kHeaderLength),  //
             ShiftAddress(orig_node, kHeaderLength),    //
             kWordLength * end_index);
      record_count += end_index;
      offset = end_offset;
    } else {
      // insert metadata one by one
      for (size_t index = begin_index; index < end_index; ++index, ++record_count) {
        const auto meta = orig_node->GetMetadata(index);
        offset = AlignOffset(offset) - meta.GetTotalLength();
        const auto new_meta = meta.UpdateOffset(offset);
        target_node->SetMetadata(record_count, new_meta);
      }
    }

    // copy records
    memcpy(ShiftAddress(target_node, offset),    //
           ShiftAddress(orig_node, end_offset),  //
           copy_end - offset);

    return offset;
  }

 public:
  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

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
      const size_t key_length)
  {
    const auto new_block_size =
        node->GetStatusWordProtected().GetOccupiedSize() + 2 * kWordLength + key_length;
    return new_block_size > kPageSize;
  }

  static constexpr std::pair<BaseNode_t *, bool>
  GetMergeableSibling(  //
      BaseNode_t *parent,
      const size_t target_index,
      const size_t target_size,
      const size_t max_merged_node_size)
  {
    if (target_index > 0) {
      const auto sibling_node = GetChildNode(parent, target_index - 1);
      const auto sibling_size = sibling_node->GetStatusWordProtected().GetLiveDataSize();
      if ((target_size + sibling_size) < max_merged_node_size) {
        return {sibling_node, true};
      }
    }
    if (target_index < parent->GetSortedCount() - 1) {
      const auto sibling_node = GetChildNode(parent, target_index + 1);
      const auto sibling_size = sibling_node->GetStatusWordProtected().GetLiveDataSize();
      if ((target_size + sibling_size) < max_merged_node_size) {
        return {sibling_node, false};
      }
    }
    return {nullptr, false};
  }

  /*################################################################################################
   * Public structure modification operations
   *##############################################################################################*/

  static constexpr BaseNode_t *
  CreateInitialRoot()
  {
    auto root = BaseNode_t::CreateEmptyNode(kInternalFlag);
    const auto leaf_node = BaseNode_t::CreateEmptyNode(kLeafFlag);

    // set an inital leaf node
    root->SetPayload(kPageSize, leaf_node, kWordLength);
    root->SetMetadata(0, Metadata{}.SetRecordInfo(kPageSize - kWordLength, 0, kWordLength));

    // set a new header
    root->SetSortedCount(1);
    root->SetStatus(StatusWord{}.AddRecordInfo(1, kWordLength, 0));

    return root;
  }

  static constexpr std::pair<BaseNode_t *, BaseNode_t *>
  Split(  //
      const BaseNode_t *target_node,
      const size_t left_rec_count)
  {
    const auto rec_count = target_node->GetSortedCount();
    const auto right_rec_count = rec_count - left_rec_count;

    // create a split left node
    auto left_node = BaseNode_t::CreateEmptyNode(kInternalFlag);
    auto offset = CopySortedRecords(left_node, 0, kPageSize, target_node, 0, left_rec_count);
    left_node->SetSortedCount(left_rec_count);
    left_node->SetStatus(
        StatusWord{}.AddRecordInfo(left_rec_count, kPageSize - AlignOffset(offset), 0));

    // create a split right node
    auto right_node = BaseNode_t::CreateEmptyNode(kInternalFlag);
    offset = CopySortedRecords(right_node, 0, kPageSize, target_node, left_rec_count, rec_count);
    right_node->SetSortedCount(right_rec_count);
    right_node->SetStatus(
        StatusWord{}.AddRecordInfo(right_rec_count, kPageSize - AlignOffset(offset), 0));

    return {left_node, right_node};
  }

  static constexpr BaseNode_t *
  Merge(  //
      const BaseNode_t *left_node,
      const BaseNode_t *right_node)
  {
    const auto left_rec_count = left_node->GetSortedCount();
    const auto right_rec_count = right_node->GetSortedCount();
    const auto rec_count = left_rec_count + right_rec_count;

    // create a merged node
    auto merged_node = BaseNode_t::CreateEmptyNode(kInternalFlag);
    auto offset = CopySortedRecords(merged_node, 0, kPageSize, left_node, 0, left_rec_count);
    offset = CopySortedRecords(merged_node, left_rec_count, offset, right_node, 0, right_rec_count);
    merged_node->SetSortedCount(rec_count);
    merged_node->SetStatus(
        StatusWord{}.AddRecordInfo(rec_count, kPageSize - AlignOffset(offset), 0));

    return merged_node;
  }

  static constexpr BaseNode_t *
  CreateNewRoot(  //
      const BaseNode_t *left_child,
      const BaseNode_t *right_child)
  {
    auto offset = kPageSize;
    auto new_root = BaseNode_t::CreateEmptyNode(kInternalFlag);

    // insert children
    offset = InsertChild(new_root, left_child, 0, offset);
    offset = InsertChild(new_root, right_child, 1, offset);

    // set a new header
    new_root->SetSortedCount(2);
    new_root->SetStatus(StatusWord{}.AddRecordInfo(2, kPageSize - AlignOffset(offset), 0));

    return new_root;
  }

  static constexpr BaseNode_t *
  NewParentForSplit(  //
      const BaseNode_t *old_parent,
      const BaseNode_t *left_node,
      const BaseNode_t *right_node,
      const size_t split_index)
  {
    const auto rec_count = old_parent->GetSortedCount();
    auto new_parent = BaseNode_t::CreateEmptyNode(kInternalFlag);
    size_t offset = kPageSize;

    if (split_index > 0) {
      // copy left sorted records
      offset = CopySortedRecords(new_parent, 0, offset, old_parent, 0, split_index);
    }

    // insert split nodes
    offset = InsertChild(new_parent, left_node, split_index, offset);
    offset = InsertChild(new_parent, right_node, split_index + 1, offset, old_parent, split_index);

    if (split_index < rec_count - 1) {
      // copy right sorted records
      offset = CopySortedRecords(new_parent, split_index + 2, offset,  //
                                 old_parent, split_index + 1, rec_count);
    }

    // set an updated header
    new_parent->SetSortedCount(rec_count + 1);
    new_parent->SetStatus(
        StatusWord{}.AddRecordInfo(rec_count + 1, kPageSize - AlignOffset(offset), 0));

    return new_parent;
  }

  static constexpr BaseNode_t *
  NewParentForMerge(  //
      const BaseNode_t *old_parent,
      const BaseNode_t *merged_node,
      const size_t del_index)
  {
    const auto rec_count = old_parent->GetSortedCount();
    auto new_parent = BaseNode_t::CreateEmptyNode(kInternalFlag);
    size_t offset = kPageSize;

    if (del_index > 0) {
      // copy left sorted records
      offset = CopySortedRecords(new_parent, 0, offset, old_parent, 0, del_index);
    }

    // insert a merged node
    offset = InsertChild(new_parent, merged_node, del_index, offset, old_parent, del_index + 1);

    if (del_index < rec_count - 2) {
      // copy right sorted records
      offset = CopySortedRecords(new_parent, del_index + 1, offset,  //
                                 old_parent, del_index + 2, rec_count);
    }

    new_parent->SetSortedCount(rec_count - 1);
    new_parent->SetStatus(
        StatusWord{}.AddRecordInfo(rec_count - 1, kPageSize - AlignOffset(offset), 0));

    return new_parent;
  }
};

}  // namespace dbgroup::index::bztree
