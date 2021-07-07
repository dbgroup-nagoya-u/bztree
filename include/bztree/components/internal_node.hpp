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
  GetAlignedOffset(const size_t offset)
  {
    if constexpr (std::is_same_v<Key, char *>) {
      return offset - (offset & (kWordLength - 1));
    } else if constexpr (sizeof(Key) % kWordLength != 0) {
      constexpr auto kAlignedSize = kWordLength - sizeof(Key) % kWordLength;
      return offset - kAlignedSize;
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
    offset = node->SetPayload(GetAlignedOffset(offset), child_addr, kWordLength);
    if (key_length > 0) {
      offset = node->SetKey(offset, key, key_length);
    }
    return offset;
  }

  static constexpr size_t
  InsertNewChild(  //
      BaseNode_t *inserted_node,
      const BaseNode_t *target_node,
      const size_t target_index,
      size_t offset)
  {
    const auto meta = target_node->GetMetadata(target_node->GetSortedCount() - 1);
    const auto key = CastKey<Key>(target_node->GetKeyAddr(meta));
    const auto key_length = meta.GetKeyLength();
    offset = SetChild(inserted_node, key, key_length, target_node, offset);
    const auto inserted_meta =
        Metadata{}.SetRecordInfo(offset, key_length, key_length + kWordLength);
    inserted_node->SetMetadata(target_index, inserted_meta);

    return offset;
  }

  static constexpr void
  CopySortedRecords(  //
      BaseNode_t *target_node,
      const BaseNode_t *original_node,
      const size_t begin_index,
      const size_t end_index)
  {
    auto record_count = target_node->GetSortedCount();
    auto offset = kPageSize - target_node->GetStatusWord().GetBlockSize();
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
    target_node->SetStatus(StatusWord{}.AddRecordInfo(record_count, kPageSize - offset, 0));
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
  CreateInitialRoot()
  {
    auto root = BaseNode_t::CreateEmptyNode(kInternalFlag);
    const auto leaf_node = BaseNode_t::CreateEmptyNode(kLeafFlag);

    // set an inital leaf node
    const auto offset = SetChild(root, Key{}, 0, leaf_node, kPageSize);
    const auto meta = Metadata{}.SetRecordInfo(offset, 0, kWordLength);
    root->SetMetadata(0, meta);

    // set a new header
    root->SetSortedCount(1);
    root->SetStatus(StatusWord{}.AddRecordInfo(1, kPageSize - offset, 0));

    return root;
  }

  static constexpr std::pair<BaseNode_t *, BaseNode_t *>
  Split(  //
      const BaseNode_t *target_node,
      const size_t left_record_count)
  {
    // create a split left node
    auto left_node = BaseNode_t::CreateEmptyNode(kInternalFlag);
    CopySortedRecords(left_node, target_node, 0, left_record_count);

    // create a split right node
    auto right_node = BaseNode_t::CreateEmptyNode(kInternalFlag);
    CopySortedRecords(right_node, target_node, left_record_count, target_node->GetSortedCount());

    return {left_node, right_node};
  }

  static constexpr BaseNode_t *
  Merge(  //
      const BaseNode_t *left_node,
      const BaseNode_t *right_node)
  {
    // create a merged node
    auto merged_node = BaseNode_t::CreateEmptyNode(kInternalFlag);
    CopySortedRecords(merged_node, left_node, 0, left_node->GetSortedCount());
    CopySortedRecords(merged_node, right_node, 0, right_node->GetSortedCount());

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
    offset = InsertNewChild(new_root, left_child, 0, offset);
    offset = GetAlignedOffset(InsertNewChild(new_root, right_child, 1, offset));

    // set a new header
    new_root->SetSortedCount(2);
    new_root->SetStatus(StatusWord{}.AddRecordInfo(2, kPageSize - offset, 0));

    return new_root;
  }

  static constexpr BaseNode_t *
  NewParentForSplit(  //
      const BaseNode_t *old_parent,
      const BaseNode_t *left_node,
      const BaseNode_t *right_node,
      const size_t split_index)
  {
    auto new_parent = BaseNode_t::CreateEmptyNode(kInternalFlag);

    // copy left sorted records
    CopySortedRecords(new_parent, old_parent, 0, split_index);

    // insert split nodes
    const auto status = new_parent->GetStatusWord();
    auto offset = kPageSize - status.GetBlockSize();
    offset = InsertNewChild(new_parent, left_node, split_index, offset);
    offset = GetAlignedOffset(InsertNewChild(new_parent, right_node, split_index + 1, offset));
    new_parent->SetSortedCount(split_index + 2);
    new_parent->SetStatus(status.AddRecordInfo(2, kPageSize - offset, 0));

    // copy right sorted records
    CopySortedRecords(new_parent, old_parent, split_index + 1, old_parent->GetSortedCount());

    return new_parent;
  }

  static constexpr BaseNode_t *
  NewParentForMerge(  //
      const BaseNode_t *old_parent,
      const BaseNode_t *merged_node,
      const size_t deleted_index)
  {
    auto new_parent = BaseNode_t::CreateEmptyNode(kInternalFlag);

    // copy left sorted records
    CopySortedRecords(new_parent, old_parent, 0, deleted_index);

    // insert merged node
    const auto status = new_parent->GetStatusWord();
    auto offset = kPageSize - status.GetBlockSize();
    offset = GetAlignedOffset(InsertNewChild(new_parent, merged_node, deleted_index, offset));
    new_parent->SetSortedCount(deleted_index + 1);
    new_parent->SetStatus(status.AddRecordInfo(1, kPageSize - offset, 0));

    // copy right sorted records
    CopySortedRecords(new_parent, old_parent, deleted_index + 2, old_parent->GetSortedCount());

    return new_parent;
  }
};

}  // namespace dbgroup::index::bztree
