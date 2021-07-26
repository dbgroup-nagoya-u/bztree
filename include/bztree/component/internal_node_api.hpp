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
using component::CallocNew;
using component::Metadata;
using component::Node;
using component::ReadMwCASField;
using component::StatusWord;

constexpr bool kLeafFlag = true;

constexpr bool kInternalFlag = false;

/*################################################################################################
 * Internal utility functions
 *##############################################################################################*/

template <class Key, class Payload, class Compare>
void
_SetChild(  //
    Node<Key, Payload, Compare> *node,
    const Key &key,
    const size_t key_length,
    const Node<Key, Payload, Compare> *child_addr,
    size_t &offset)
{
  node->SetPayload(offset, child_addr, kWordLength);
  if (key_length > 0) {
    node->SetKey(offset, key, key_length);
  }
}

template <class Key, class Payload, class Compare>
void
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
  target_node->SetMetadata(target_index, Metadata{offset, key_length, key_length + kWordLength});

  AlignOffset<Key>(offset);
}

template <class Key, class Payload, class Compare>
void
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

  AlignOffset<Key>(offset);
}

template <class Key, class Payload, class Compare>
void
_CopySortedRecords(  //
    Node<Key, Payload, Compare> *target_node,
    size_t record_count,
    size_t &offset,
    const Node<Key, Payload, Compare> *orig_node,
    const size_t begin_index,
    const size_t end_index)
{
  assert(end_index > 0);

  // insert metadata and records one by one
  for (size_t index = begin_index; index < end_index; ++index, ++record_count) {
    const auto meta = orig_node->GetMetadata(index);
    const auto key = orig_node->GetKey(meta);
    const auto child_addr =
        ReadMwCASField<Node<Key, Payload, Compare> *>(orig_node->GetPayloadAddr(meta));
    _SetChild(target_node, key, meta.GetKeyLength(), child_addr, offset);

    const auto new_meta = meta.UpdateOffset(offset);
    target_node->SetMetadata(record_count, new_meta);
    AlignOffset<Key>(offset);
  }
}

/*################################################################################################
 * Public utility functions
 *##############################################################################################*/

/**
 * @brief Get an index of the specified key by using binary search. If there is no
 * specified key, this returns the minimum metadata index that is greater than the
 * specified key
 *
 * @tparam Compare
 * @param key
 * @param comp
 * @return std::pair<KeyExistence, size_t>
 */
template <class Key, class Payload, class Compare>
size_t
SearchChildNode(  //
    const Node<Key, Payload, Compare> *node,
    const Key &key,
    const bool range_is_closed)
{
  const size_t sorted_count = node->GetSortedCount();

  int64_t begin_index = 0;
  int64_t end_index = sorted_count - 1;
  int64_t index = (begin_index + end_index) >> 1;

  while (begin_index <= end_index) {
    const auto meta = node->GetMetadataProtected(index);
    const auto index_key = node->GetKey(meta);

    if (meta.GetKeyLength() == 0 || Compare{}(key, index_key)) {
      // a target key is in a left side
      end_index = index - 1;
    } else if (Compare{}(index_key, key)) {
      // a target key is in a right side
      begin_index = index + 1;
    } else {
      // find an equivalent key
      if (!range_is_closed) ++index;
      return index;
    }

    index = (begin_index + end_index) >> 1;
  }

  return begin_index;
}

template <class Key, class Payload, class Compare>
Node<Key, Payload, Compare> *
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
Node<Key, Payload, Compare> *
CreateInitialRoot()
{
  auto root = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kInternalFlag);
  const auto leaf_node = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kLeafFlag);

  // set an inital leaf node
  auto offset = kPageSize;
  root->SetPayload(offset, leaf_node, kWordLength);
  root->SetMetadata(0, Metadata{offset, 0, kWordLength});

  // set a new header
  root->SetSortedCount(1);
  root->SetStatus(StatusWord{1, kWordLength});

  return root;
}

template <class Key, class Payload, class Compare>
std::pair<Node<Key, Payload, Compare> *, Node<Key, Payload, Compare> *>
Split(  //
    const Node<Key, Payload, Compare> *target_node,
    const size_t left_rec_count)
{
  const auto rec_count = target_node->GetSortedCount();
  const auto right_rec_count = rec_count - left_rec_count;

  // create a split left node
  auto left_node = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kInternalFlag);
  auto offset = kPageSize;
  _CopySortedRecords(left_node, 0, offset, target_node, 0, left_rec_count);

  left_node->SetSortedCount(left_rec_count);
  left_node->SetStatus(StatusWord{left_rec_count, kPageSize - offset});

  // create a split right node
  auto right_node = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kInternalFlag);
  offset = kPageSize;
  _CopySortedRecords(right_node, 0, offset, target_node, left_rec_count, rec_count);

  right_node->SetSortedCount(right_rec_count);
  right_node->SetStatus(StatusWord{right_rec_count, kPageSize - offset});

  return {left_node, right_node};
}

template <class Key, class Payload, class Compare>
Node<Key, Payload, Compare> *
Merge(  //
    const Node<Key, Payload, Compare> *left_node,
    const Node<Key, Payload, Compare> *right_node)
{
  const auto left_rec_count = left_node->GetSortedCount();
  const auto right_rec_count = right_node->GetSortedCount();
  const auto rec_count = left_rec_count + right_rec_count;

  // create a merged node
  auto merged_node = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kInternalFlag);
  auto offset = kPageSize;
  _CopySortedRecords(merged_node, 0, offset, left_node, 0, left_rec_count);
  _CopySortedRecords(merged_node, left_rec_count, offset, right_node, 0, right_rec_count);

  merged_node->SetSortedCount(rec_count);
  merged_node->SetStatus(StatusWord{rec_count, kPageSize - offset});

  return merged_node;
}

template <class Key, class Payload, class Compare>
Node<Key, Payload, Compare> *
CreateNewRoot(  //
    const Node<Key, Payload, Compare> *left_child,
    const Node<Key, Payload, Compare> *right_child)
{
  auto offset = kPageSize;
  auto new_root = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kInternalFlag);

  // insert children
  _InsertChild(new_root, left_child, 0, offset);
  _InsertChild(new_root, right_child, 1, offset);

  // set a new header
  new_root->SetSortedCount(2);
  new_root->SetStatus(StatusWord{2, kPageSize - offset});

  return new_root;
}

template <class Key, class Payload, class Compare>
Node<Key, Payload, Compare> *
NewParentForSplit(  //
    const Node<Key, Payload, Compare> *old_parent,
    const Node<Key, Payload, Compare> *left_node,
    const Node<Key, Payload, Compare> *right_node,
    const size_t split_index)
{
  const auto rec_count = old_parent->GetSortedCount();
  auto new_parent = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kInternalFlag);
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
  new_parent->SetSortedCount(rec_count + 1);
  new_parent->SetStatus(StatusWord{rec_count + 1, kPageSize - offset});

  return new_parent;
}

template <class Key, class Payload, class Compare>
Node<Key, Payload, Compare> *
NewParentForMerge(  //
    const Node<Key, Payload, Compare> *old_parent,
    const Node<Key, Payload, Compare> *merged_node,
    const size_t del_index)
{
  const auto rec_count = old_parent->GetSortedCount();
  auto new_parent = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kInternalFlag);
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

  new_parent->SetSortedCount(rec_count - 1);
  new_parent->SetStatus(StatusWord{rec_count - 1, kPageSize - offset});

  return new_parent;
}

}  // namespace dbgroup::index::bztree::internal
