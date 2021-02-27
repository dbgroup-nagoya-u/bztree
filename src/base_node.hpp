// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "metadata.hpp"
#include "mwcas/mwcas_descriptor.hpp"
#include "status_word.hpp"

namespace bztree
{
using dbgroup::atomic::MwCASDescriptor;
using dbgroup::atomic::ReadMwCASField;

class alignas(kWordLength) BaseNode
{
 protected:
  /*################################################################################################
   * Internal inherited variables
   *##############################################################################################*/

  uint64_t node_size_ : 32;

  uint64_t is_leaf_ : 1;

  uint64_t sorted_count_ : 16;

  uint64_t : 0;

  StatusWord status_;

  Metadata meta_array_[0];

  /*################################################################################################
   * Internally inherited constructors
   *##############################################################################################*/

  BaseNode(  //
      const size_t node_size,
      const bool is_leaf)
      : node_size_{node_size}, is_leaf_{is_leaf}, sorted_count_{0}, status_{}
  {
  }

  /*################################################################################################
   * Internally inherited getters/setters
   *##############################################################################################*/

  void
  SetMetadata(  //
      const size_t index,
      const Metadata new_meta)
  {
    meta_array_[index] = new_meta;
  }

  constexpr void *
  GetKeyAddr(const Metadata meta) const
  {
    const auto offset = meta.GetOffset();
    return ShiftAddress(this, offset);
  }

  constexpr void *
  GetPayloadAddr(const Metadata meta) const
  {
    const auto offset = meta.GetOffset() + meta.GetKeyLength();
    return ShiftAddress(this, offset);
  }

  void
  SetKey(  //
      const void *key,
      const size_t key_length,
      const size_t offset)
  {
    const auto key_ptr = ShiftAddress(this, offset);
    memcpy(key_ptr, key, key_length);
  }

  void
  SetPayload(  //
      const void *payload,
      const size_t payload_length,
      const size_t offset)
  {
    const auto payload_ptr = ShiftAddress(this, offset);
    memcpy(payload_ptr, payload, payload_length);
  }

  size_t
  CopyRecord(  //
      const void *key,
      const size_t key_length,
      const void *payload,
      const size_t payload_length,
      size_t offset)
  {
    offset -= payload_length;
    SetPayload(payload, payload_length, offset);
    if (key_length > 0) {
      offset -= key_length;
      SetKey(key, key_length, offset);
    }
    return offset;
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

  static BaseNode *
  CreateEmptyNode(  //
      const size_t node_size,
      const bool is_leaf)
  {
    assert((node_size % kWordLength) == 0);

    auto aligned_page = aligned_alloc(kWordLength, node_size);
    auto new_node = new (aligned_page) BaseNode{node_size, is_leaf};
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
  GetNodeSize() const
  {
    return node_size_;
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

  StatusWord
  GetStatusWordProtected() const
  {
    return ReadMwCASField<StatusWord>(&status_);
  }

  constexpr Metadata
  GetMetadata(const size_t index) const
  {
    return meta_array_[index];
  }

  std::pair<void *, size_t>
  GetKeyAndItsLength(const size_t index) const
  {
    const auto meta = GetMetadata(index);
    return {GetKeyAddr(meta), meta.GetKeyLength()};
  }

  constexpr BaseNode *
  GetPayloadAsNode(const size_t index) const
  {
    return CastPayload<BaseNode>(GetPayloadAddr(GetMetadata(index)));
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
      const size_t index,
      const void *old_payload,
      const void *new_payload)
  {
    desc.AddMwCASTarget(GetPayloadAddr(GetMetadata(index)),
                        reinterpret_cast<uintptr_t>(old_payload),
                        reinterpret_cast<uintptr_t>(new_payload));
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
  NodeReturnCode
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
  template <class Compare>
  std::pair<KeyExistence, size_t>
  SearchSortedMetadata(  //
      const void *key,
      const bool range_is_closed,
      const Compare &comp) const
  {
    const int64_t sorted_count = GetSortedCount();

    int64_t begin_index = 0;
    int64_t end_index = sorted_count;
    int64_t index = end_index / 2;

    while (begin_index <= end_index && index < sorted_count) {
      const auto meta = GetMetadata(index);
      const auto *index_key = GetKeyAddr(meta);
      const auto index_key_length = meta.GetKeyLength();

      if (index_key_length == 0 || comp(key, index_key)) {
        // a target key is in a left side
        end_index = index - 1;
      } else if (comp(index_key, key)) {
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

}  // namespace bztree
