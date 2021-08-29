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

/// a flag to indicate leaf nodes.
constexpr bool kLeafFlag = true;

/// a flag to indicate internal nodes.
constexpr bool kInternalFlag = false;

/*################################################################################################
 * Internal utility functions
 *##############################################################################################*/

/**
 * @brief Set a child node into a target internal node.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param node a target internal node.
 * @param key a target key.
 * @param key_length the length of a target key.
 * @param child_addr a pointer of a node to be set.
 * @param offset an offset for setting.
 */
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

/**
 * @brief Insert a child node into a target internal node.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param target_node a target internal node.
 * @param child_node a new child node to be inserted.
 * @param target_pos the position of a new child node for inserting.
 * @param offset an offest for inserting.
 */
template <class Key, class Payload, class Compare>
void
_InsertChild(  //
    Node<Key, Payload, Compare> *target_node,
    const Node<Key, Payload, Compare> *child_node,
    const size_t target_pos,
    size_t &offset)
{
  const auto meta = child_node->GetMetadata(child_node->GetSortedCount() - 1);
  const auto key = child_node->GetKey(meta);
  const auto key_length = meta.GetKeyLength();
  _SetChild(target_node, key, key_length, child_node, offset);
  target_node->SetMetadata(target_pos, Metadata{offset, key_length, key_length + kWordLength});

  AlignOffset<Key>(offset);
}

/**
 * @brief Copy a child node into a target internal node from an original one.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param target_node a target internal node.
 * @param child_node a new child node to be inserted.
 * @param target_node a target internal node.
 * @param offset an offest for inserting.
 * @param orig_node an original node to be copied.
 * @param orig_pos the position of a child node in an old internal node.
 */
template <class Key, class Payload, class Compare>
void
_CopyChild(  //
    Node<Key, Payload, Compare> *target_node,
    const Node<Key, Payload, Compare> *child_node,
    const size_t target_pos,
    size_t &offset,
    const Node<Key, Payload, Compare> *orig_node,
    const size_t orig_pos)
{
  const auto meta = orig_node->GetMetadata(orig_pos);
  const auto key = orig_node->GetKey(meta);
  const auto key_length = meta.GetKeyLength();
  _SetChild(target_node, key, key_length, child_node, offset);
  target_node->SetMetadata(target_pos, meta.UpdateOffset(offset));

  AlignOffset<Key>(offset);
}

/**
 * @brief Copy sorted records one by one.
 *
 * Note that this function cannot use memcpy because concurrent SMOs may modify payloads.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param target_node a target internal node.
 * @param record_count the number of records in a target node.
 * @param offset an offset of a target node.
 * @param orig_node an original internal node.
 * @param begin_id a begin id for copying.
 * @param end_id an end id for copying.
 */
template <class Key, class Payload, class Compare>
void
_CopySortedRecords(  //
    Node<Key, Payload, Compare> *target_node,
    size_t record_count,
    size_t &offset,
    const Node<Key, Payload, Compare> *orig_node,
    const size_t begin_id,
    const size_t end_id)
{
  assert(end_id > 0);

  // insert metadata and records one by one
  for (size_t index = begin_id; index < end_id; ++index, ++record_count) {
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
 * @brief Get the position of a specified key by using binary search. If there is no
 * specified key, this returns the minimum metadata index that is greater than the
 * specified key
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param node a target node.
 * @param key a target key.
 * @param range_is_closed a flag to indicate that a target key is included.
 * @return size_t: the position of a specified key.
 */
template <class Key, class Payload, class Compare>
size_t
SearchChildNode(  //
    const Node<Key, Payload, Compare> *node,
    const Key &key,
    const bool range_is_closed)
{
  const size_t sorted_count = node->GetSortedCount();

  int64_t begin_id = 0;
  int64_t end_id = sorted_count - 1;
  int64_t index = (begin_id + end_id) >> 1;

  while (begin_id <= end_id) {
    const auto meta = node->GetMetadataProtected(index);
    const auto index_key = node->GetKey(meta);

    if (meta.GetKeyLength() == 0 || Compare{}(key, index_key)) {
      // a target key is in a left side
      end_id = index - 1;
    } else if (Compare{}(index_key, key)) {
      // a target key is in a right side
      begin_id = index + 1;
    } else {
      // find an equivalent key
      if (!range_is_closed) ++index;
      begin_id = index;
      break;
    }

    index = (begin_id + end_id) >> 1;
  }

  return begin_id;
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

/**
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @return Node_t*: an initial root node for BzTree.
 */
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

/**
 * @brief Split a target internal node.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param orig_node an original node.
 * @param left_rec_count the number of records in a split left node.
 * @return std::pair<Node_t*, Node_t*>: split left/right nodes.
 */
template <class Key, class Payload, class Compare>
std::pair<Node<Key, Payload, Compare> *, Node<Key, Payload, Compare> *>
Split(  //
    const Node<Key, Payload, Compare> *orig_node,
    const size_t left_rec_count)
{
  const auto rec_count = orig_node->GetSortedCount();
  const auto right_rec_count = rec_count - left_rec_count;

  // create a split left node
  auto left_node = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kInternalFlag);
  auto offset = kPageSize;
  _CopySortedRecords(left_node, 0, offset, orig_node, 0, left_rec_count);

  left_node->SetSortedCount(left_rec_count);
  left_node->SetStatus(StatusWord{left_rec_count, kPageSize - offset});

  // create a split right node
  auto right_node = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kInternalFlag);
  offset = kPageSize;
  _CopySortedRecords(right_node, 0, offset, orig_node, left_rec_count, rec_count);

  right_node->SetSortedCount(right_rec_count);
  right_node->SetStatus(StatusWord{right_rec_count, kPageSize - offset});

  return {left_node, right_node};
}

/**
 * @brief Merge internal nodes.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param left_node a merged left node.
 * @param right_node a merged right node.
 * @return Node_t*: a merged node.
 */
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

/**
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param left_child a left child node.
 * @param right_child a right child node.
 * @return Node_t*: a new root node.
 */
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

/**
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param old_parent an old parent node.
 * @param left_node a split left node.
 * @param right_node a split right node.
 * @param split_pos the position of a split node.
 * @return Node_t*: a new parent node of a split node.
 */
template <class Key, class Payload, class Compare>
Node<Key, Payload, Compare> *
NewParentForSplit(  //
    const Node<Key, Payload, Compare> *old_parent,
    const Node<Key, Payload, Compare> *left_node,
    const Node<Key, Payload, Compare> *right_node,
    const size_t split_pos)
{
  const auto rec_count = old_parent->GetSortedCount();
  auto new_parent = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kInternalFlag);
  auto offset = kPageSize;

  if (split_pos > 0) {
    // copy left sorted records
    _CopySortedRecords(new_parent, 0, offset, old_parent, 0, split_pos);
  }

  // insert split nodes
  _InsertChild(new_parent, left_node, split_pos, offset);
  _CopyChild(new_parent, right_node, split_pos + 1, offset, old_parent, split_pos);

  if (split_pos < rec_count - 1) {
    // copy right sorted records
    _CopySortedRecords(new_parent, split_pos + 2, offset,  //
                       old_parent, split_pos + 1, rec_count);
  }

  // set an updated header
  new_parent->SetSortedCount(rec_count + 1);
  new_parent->SetStatus(StatusWord{rec_count + 1, kPageSize - offset});

  return new_parent;
}

/**
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 * @param old_parent an old parent node.
 * @param merged_node a merged node.
 * @param del_pos the position of a deleted node.
 * @return Node_t*: a new parent node of a merged node.
 */
template <class Key, class Payload, class Compare>
Node<Key, Payload, Compare> *
NewParentForMerge(  //
    const Node<Key, Payload, Compare> *old_parent,
    const Node<Key, Payload, Compare> *merged_node,
    const size_t del_pos)
{
  const auto rec_count = old_parent->GetSortedCount();
  auto new_parent = CallocNew<Node<Key, Payload, Compare>>(kPageSize, kInternalFlag);
  auto offset = kPageSize;

  if (del_pos > 0) {
    // copy left sorted records
    _CopySortedRecords(new_parent, 0, offset, old_parent, 0, del_pos);
  }

  // insert a merged node
  _CopyChild(new_parent, merged_node, del_pos, offset, old_parent, del_pos + 1);

  if (del_pos < rec_count - 2) {
    // copy right sorted records
    _CopySortedRecords(new_parent, del_pos + 1, offset,  //
                       old_parent, del_pos + 2, rec_count);
  }

  new_parent->SetSortedCount(rec_count - 1);
  new_parent->SetStatus(StatusWord{rec_count - 1, kPageSize - offset});

  return new_parent;
}

}  // namespace dbgroup::index::bztree::internal
