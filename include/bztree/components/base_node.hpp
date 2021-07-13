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

#include <algorithm>
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "metadata.hpp"
#include "mwcas/mwcas_descriptor.hpp"
#include "status_word.hpp"

namespace dbgroup::index::bztree
{
using dbgroup::atomic::mwcas::MwCASDescriptor;
using dbgroup::atomic::mwcas::ReadMwCASField;

template <class Key, class Payload, class Compare = std::less<Key>>
class alignas(kCacheLineSize) BaseNode
{
 private:
  /*################################################################################################
   * Internal variables
   *##############################################################################################*/

  uint64_t node_size_ : 32;

  uint64_t is_leaf_ : 1;

  uint64_t sorted_count_ : 16;

  uint64_t : 0;

  StatusWord status_;

  Metadata meta_array_[0];

  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  constexpr BaseNode(  //
      const bool is_leaf)
      : node_size_{kPageSize}, is_leaf_{is_leaf}, sorted_count_{0}, status_{}
  {
  }

 public:
  /*################################################################################################
   * Public enum and constants
   *##############################################################################################*/

  /**
   * @brief Return codes for functions in Base/Leaf/InternalNode.
   *
   */
  enum NodeReturnCode
  {
    kSuccess = 0,
    kKeyNotExist,
    kKeyExist,
    kScanInProgress,
    kFrozen,
    kNoSpace
  };

  enum KeyExistence
  {
    kExist = 0,
    kNotExist,
    kDeleted,
    kUncertain
  };

  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  ~BaseNode() = default;

  BaseNode(const BaseNode &) = delete;
  BaseNode &operator=(const BaseNode &) = delete;
  BaseNode(BaseNode &&) = delete;
  BaseNode &operator=(BaseNode &&) = delete;

  /*################################################################################################
   * Public builders
   *##############################################################################################*/

  constexpr static BaseNode *
  CreateEmptyNode(const bool is_leaf)
  {
    auto page = calloc(1, kPageSize);
    auto new_node = new (page) BaseNode{is_leaf};
    return new_node;
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr bool
  IsLeaf() const
  {
    return is_leaf_;
  }

  constexpr size_t
  GetSortedCount() const
  {
    return sorted_count_;
  }

  constexpr StatusWord
  GetStatusWord() const
  {
    return status_;
  }

  constexpr StatusWord
  GetStatusWordProtected() const
  {
    return ReadMwCASField<StatusWord>(&status_);
  }

  constexpr Metadata
  GetMetadata(const size_t index) const
  {
    return meta_array_[index];
  }

  constexpr Metadata
  GetMetadataProtected(const size_t index) const
  {
    return ReadMwCASField<Metadata>(&meta_array_[index]);
  }

  constexpr Key
  GetKey(const Metadata meta) const
  {
    return Cast<Key>(GetKeyAddr(meta));
  }

  constexpr void *
  GetKeyAddr(const Metadata meta) const
  {
    return ShiftAddress(this, meta.GetOffset());
  }

  constexpr void *
  GetPayloadAddr(const Metadata meta) const
  {
    return ShiftAddress(this, meta.GetOffset() + meta.GetKeyLength());
  }

  constexpr auto
  GetPayload(const Metadata meta) const
  {
    if constexpr (std::is_same_v<Payload, char *>) {
      const auto payload_length = meta.GetPayloadLength();
      auto payload = malloc(payload_length);
      memcpy(payload, this->GetPayloadAddr(meta), payload_length);
      return std::unique_ptr<char>(static_cast<char *>(payload));
    } else {
      return Cast<Payload>(this->GetPayloadAddr(meta));
    }
  }

  void
  SetSortedCount(const size_t sorted_count)
  {
    sorted_count_ = sorted_count;
  }

  void
  SetStatus(const StatusWord status)
  {
    status_ = status;
  }

  void
  SetMetadata(  //
      const size_t index,
      const Metadata new_meta)
  {
    meta_array_[index] = new_meta;
  }

  void
  SetMetadataByCAS(  //
      const size_t index,
      const Metadata new_meta)
  {
    CastAddress<std::atomic<Metadata> *>(meta_array_ + index)->store(new_meta, mo_relax);
  }

  constexpr size_t
  SetKey(  //
      size_t offset,
      const Key key,
      const size_t key_length)
  {
    if constexpr (std::is_same_v<Key, char *>) {
      offset -= key_length;
      memcpy(ShiftAddress(this, offset), key, key_length);
    } else {
      offset -= sizeof(Key);
      memcpy(ShiftAddress(this, offset), &key, sizeof(Key));
    }
    return offset;
  }

  template <class T>
  constexpr size_t
  SetPayload(  //
      size_t offset,
      const T payload,
      const size_t payload_length)
  {
    if constexpr (std::is_same_v<T, char *>) {
      offset -= payload_length;
      memcpy(ShiftAddress(this, offset), payload, payload_length);
    } else {
      offset -= sizeof(T);
      memcpy(ShiftAddress(this, offset), &payload, sizeof(T));
    }
    return offset;
  }

  void
  SetStatusForMwCAS(  //
      MwCASDescriptor &desc,
      const StatusWord old_status,
      const StatusWord new_status)
  {
    desc.AddMwCASTarget(&status_, old_status, new_status);
  }

  void
  SetMetadataForMwCAS(  //
      MwCASDescriptor &desc,
      const size_t index,
      const Metadata old_meta,
      const Metadata new_meta)
  {
    desc.AddMwCASTarget(meta_array_ + index, old_meta, new_meta);
  }

  void
  SetPayloadForMwCAS(  //
      MwCASDescriptor &desc,
      const Metadata meta,
      const Payload new_payload)
  {
    const auto old_payload = this->GetPayload(meta);
    desc.AddMwCASTarget(ShiftAddress(this, meta.GetOffset() + meta.GetKeyLength()),  //
                        old_payload, new_payload);
  }

  template <class T>
  void
  SetPayloadForMwCAS(  //
      MwCASDescriptor &desc,
      const Metadata meta,
      const T old_payload,
      const T new_payload)
  {
    desc.AddMwCASTarget(ShiftAddress(this, meta.GetOffset() + meta.GetKeyLength()),  //
                        old_payload, new_payload);
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @brief Freeze this node to prevent concurrent writes/SMOs.
   *
   * @param pmwcas_pool
   * @return BaseNode::NodeReturnCode
   * 1) `kSuccess` if the function successfully freeze this node, or
   * 2) `kFrozen` if this node is already frozen.
   */
  constexpr NodeReturnCode
  Freeze()
  {
    bool mwcas_success;
    do {
      const auto current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return NodeReturnCode::kFrozen;
      }

      MwCASDescriptor desc;
      SetStatusForMwCAS(desc, current_status, current_status.Freeze());
      mwcas_success = desc.MwCAS();
    } while (!mwcas_success);

    return NodeReturnCode::kSuccess;
  }

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
  constexpr std::pair<KeyExistence, size_t>
  SearchSortedMetadata(  //
      const Key &key,
      const bool range_is_closed) const
  {
    const int64_t sorted_count = GetSortedCount();

    int64_t begin_index = 0;
    int64_t end_index = sorted_count;
    int64_t index = end_index / 2;

    while (begin_index <= end_index && index < sorted_count) {
      const auto meta = GetMetadataProtected(index);
      const auto index_key = Cast<Key>(GetKeyAddr(meta));
      const auto index_key_length = meta.GetKeyLength();

      if (index_key_length == 0 || Compare{}(key, index_key)) {
        // a target key is in a left side
        end_index = index - 1;
      } else if (Compare{}(index_key, key)) {
        // a target key is in a right side
        begin_index = index + 1;
      } else {
        // find an equivalent key
        if (meta.IsVisible()) {
          return {KeyExistence::kExist, (range_is_closed) ? index : index + 1};
        } else {
          // there is no inserting nor corrupted record in a sorted region
          return {KeyExistence::kDeleted, index};
        }
      }
      index = (begin_index + end_index) / 2;
    }

    return {KeyExistence::kNotExist, begin_index};
  }
};

}  // namespace dbgroup::index::bztree
