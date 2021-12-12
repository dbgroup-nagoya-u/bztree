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
using component::CanCASUpdate;
using component::Metadata;
using component::MwCASDescriptor;
using component::Node;
using component::StatusWord;

/// a flag to indicate leaf nodes.
constexpr bool kLeafFlag = true;

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

}  // namespace dbgroup::index::bztree::internal
