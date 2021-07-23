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

#include "node.hpp"

namespace dbgroup::index::bztree::internal
{
using component::AlignOffset;
using component::Metadata;
using component::Node;
using component::ReadMwCASField;
using component::StatusWord;

/*################################################################################################
 * Internal utility functions
 *##############################################################################################*/

template <class Key, class Payload, class Compare>
constexpr void
_SetChild(  //
    Node<Key, Payload, Compare> *node,
    const Key key,
    const size_t key_length,
    const Node<Key, Payload, Compare> *child_addr,
    size_t &offset)
{
  AlignOffset<Key>(offset);
  node->SetPayload(offset, child_addr, kWordLength);
  if (key_length > 0) {
    node->SetKey(offset, key, key_length);
  }
}

template <class Key, class Payload, class Compare>
constexpr void
_InsertChild(  //
    Node<Key, Payload, Compare> *target_node,
    const Node<Key, Payload, Compare> *child_node,
    const size_t target_index,
    size_t &offset)
{
  const auto meta = child_node->GetMetadata(child_node->GetSortedCount() - 1);
  const auto key = child_node->GetKey(meta);
  const auto key_length = meta.GetKeyLength();
  _SetChild(target_node, key, key_length, child_node, offset);
  const auto inserted_meta = Metadata{}.SetRecordInfo(offset, key_length, key_length + kWordLength);
  target_node->SetMetadata(target_index, inserted_meta);
}

template <class Key, class Payload, class Compare>
constexpr void
_InsertChild(  //
    Node<Key, Payload, Compare> *target_node,
    const Node<Key, Payload, Compare> *child_node,
    const size_t target_index,
    size_t &offset,
    const Node<Key, Payload, Compare> *orig_node,
    const size_t orig_index)
{
  const auto meta = orig_node->GetMetadata(orig_index);
  const auto key = orig_node->GetKey(meta);
  const auto key_length = meta.GetKeyLength();
  _SetChild(target_node, key, key_length, child_node, offset);
  target_node->SetMetadata(target_index, meta.UpdateOffset(offset));
}

template <class Key, class Payload, class Compare>
constexpr void
_CopySortedRecords(  //
    Node<Key, Payload, Compare> *target_node,
    size_t record_count,
    size_t &offset,
    const Node<Key, Payload, Compare> *orig_node,
    const size_t begin_index,
    const size_t end_index)
{
  assert(end_index > 0);

  AlignOffset<Key>(offset);
  const auto end_offset = orig_node->GetMetadata(end_index - 1).GetOffset();
  const auto copy_end = offset;

  // copy metadata
  if (record_count == 0 && begin_index == 0) {
    // directly copy metadata
    memcpy(ShiftAddress(target_node, component::kHeaderLength),  //
           ShiftAddress(orig_node, component::kHeaderLength),    //
           kWordLength * end_index);
    record_count += end_index;
    offset = end_offset;
  } else {
    // insert metadata one by one
    for (size_t index = begin_index; index < end_index; ++index, ++record_count) {
      const auto meta = orig_node->GetMetadata(index);
      AlignOffset<Key>(offset);
      offset -= meta.GetTotalLength();
      const auto new_meta = meta.UpdateOffset(offset);
      target_node->SetMetadata(record_count, new_meta);
    }
  }

  // copy records
  memcpy(ShiftAddress(target_node, offset),    //
         ShiftAddress(orig_node, end_offset),  //
         copy_end - offset);
}

/*################################################################################################
 * Public getters/setters
 *##############################################################################################*/

template <class Key, class Payload, class Compare>
constexpr Node<Key, Payload, Compare> *
GetChildNode(  //
    const Node<Key, Payload, Compare> *node,
    const size_t index)
{
  const auto meta = node->GetMetadata(index);
  return ReadMwCASField<Node<Key, Payload, Compare> *>(node->GetPayloadAddr(meta));
}

/*################################################################################################
 * Public structure modification operations
 *##############################################################################################*/

template <class Key, class Payload, class Compare>
constexpr Node<Key, Payload, Compare> *
CreateInitialRoot()
{
  auto root = Node<Key, Payload, Compare>::CreateEmptyNode(false);
  const auto leaf_node = Node<Key, Payload, Compare>::CreateEmptyNode(true);

  // set an inital leaf node
  auto offset = kPageSize;
  root->SetPayload(offset, leaf_node, kWordLength);
  root->SetMetadata(0, Metadata{}.SetRecordInfo(offset, 0, kWordLength));

  // set a new header
  root->SetSortedCount(1);
  root->SetStatus(StatusWord{1, kWordLength});

  return root;
}

template <class Key, class Payload, class Compare>
constexpr std::pair<Node<Key, Payload, Compare> *, Node<Key, Payload, Compare> *>
Split(  //
    const Node<Key, Payload, Compare> *target_node,
    const size_t left_rec_count)
{
  const auto rec_count = target_node->GetSortedCount();
  const auto right_rec_count = rec_count - left_rec_count;

  // create a split left node
  auto left_node = Node<Key, Payload, Compare>::CreateEmptyNode(false);
  auto offset = kPageSize;
  _CopySortedRecords(left_node, 0, offset, target_node, 0, left_rec_count);

  AlignOffset<Key>(offset);
  left_node->SetSortedCount(left_rec_count);
  left_node->SetStatus(StatusWord{left_rec_count, kPageSize - offset});

  // create a split right node
  auto right_node = Node<Key, Payload, Compare>::CreateEmptyNode(false);
  offset = kPageSize;
  _CopySortedRecords(right_node, 0, offset, target_node, left_rec_count, rec_count);

  AlignOffset<Key>(offset);
  right_node->SetSortedCount(right_rec_count);
  right_node->SetStatus(StatusWord{right_rec_count, kPageSize - offset});

  return {left_node, right_node};
}

template <class Key, class Payload, class Compare>
constexpr Node<Key, Payload, Compare> *
Merge(  //
    const Node<Key, Payload, Compare> *left_node,
    const Node<Key, Payload, Compare> *right_node)
{
  const auto left_rec_count = left_node->GetSortedCount();
  const auto right_rec_count = right_node->GetSortedCount();
  const auto rec_count = left_rec_count + right_rec_count;

  // create a merged node
  auto merged_node = Node<Key, Payload, Compare>::CreateEmptyNode(false);
  auto offset = kPageSize;
  _CopySortedRecords(merged_node, 0, offset, left_node, 0, left_rec_count);
  _CopySortedRecords(merged_node, left_rec_count, offset, right_node, 0, right_rec_count);

  AlignOffset<Key>(offset);
  merged_node->SetSortedCount(rec_count);
  merged_node->SetStatus(StatusWord{rec_count, kPageSize - offset});

  return merged_node;
}

template <class Key, class Payload, class Compare>
constexpr Node<Key, Payload, Compare> *
CreateNewRoot(  //
    const Node<Key, Payload, Compare> *left_child,
    const Node<Key, Payload, Compare> *right_child)
{
  auto offset = kPageSize;
  auto new_root = Node<Key, Payload, Compare>::CreateEmptyNode(false);

  // insert children
  _InsertChild(new_root, left_child, 0, offset);
  _InsertChild(new_root, right_child, 1, offset);

  // set a new header
  AlignOffset<Key>(offset);
  new_root->SetSortedCount(2);
  new_root->SetStatus(StatusWord{2, kPageSize - offset});

  return new_root;
}

template <class Key, class Payload, class Compare>
constexpr Node<Key, Payload, Compare> *
NewParentForSplit(  //
    const Node<Key, Payload, Compare> *old_parent,
    const Node<Key, Payload, Compare> *left_node,
    const Node<Key, Payload, Compare> *right_node,
    const size_t split_index)
{
  const auto rec_count = old_parent->GetSortedCount();
  auto new_parent = Node<Key, Payload, Compare>::CreateEmptyNode(false);
  auto offset = kPageSize;

  if (split_index > 0) {
    // copy left sorted records
    _CopySortedRecords(new_parent, 0, offset, old_parent, 0, split_index);
  }

  // insert split nodes
  _InsertChild(new_parent, left_node, split_index, offset);
  _InsertChild(new_parent, right_node, split_index + 1, offset, old_parent, split_index);

  if (split_index < rec_count - 1) {
    // copy right sorted records
    _CopySortedRecords(new_parent, split_index + 2, offset,  //
                       old_parent, split_index + 1, rec_count);
  }

  // set an updated header
  AlignOffset<Key>(offset);
  new_parent->SetSortedCount(rec_count + 1);
  new_parent->SetStatus(StatusWord{rec_count + 1, kPageSize - offset});

  return new_parent;
}

template <class Key, class Payload, class Compare>
constexpr Node<Key, Payload, Compare> *
NewParentForMerge(  //
    const Node<Key, Payload, Compare> *old_parent,
    const Node<Key, Payload, Compare> *merged_node,
    const size_t del_index)
{
  const auto rec_count = old_parent->GetSortedCount();
  auto new_parent = Node<Key, Payload, Compare>::CreateEmptyNode(false);
  auto offset = kPageSize;

  if (del_index > 0) {
    // copy left sorted records
    _CopySortedRecords(new_parent, 0, offset, old_parent, 0, del_index);
  }

  // insert a merged node
  _InsertChild(new_parent, merged_node, del_index, offset, old_parent, del_index + 1);

  if (del_index < rec_count - 2) {
    // copy right sorted records
    _CopySortedRecords(new_parent, del_index + 1, offset,  //
                       old_parent, del_index + 2, rec_count);
  }

  AlignOffset<Key>(offset);
  new_parent->SetSortedCount(rec_count - 1);
  new_parent->SetStatus(StatusWord{rec_count - 1, kPageSize - offset});

  return new_parent;
}

}  // namespace dbgroup::index::bztree::internal
