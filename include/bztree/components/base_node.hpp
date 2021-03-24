// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

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

  void
  SetMetadataByCAS(  //
      const size_t index,
      const Metadata new_meta)
  {
    CastAddress<std::atomic<Metadata> *>(meta_array_ + index)->store(new_meta, mo_relax);
  }

  constexpr void *
  GetPayloadAddr(const Metadata meta) const
  {
    const auto offset = meta.GetOffset() + meta.GetKeyLength();
    return ShiftAddress(this, offset);
  }

  void
  SetKey(  //
      const Key &key,
      const size_t key_length,
      const size_t offset)
  {
    const auto key_ptr = ShiftAddress(this, offset);
    if constexpr (std::is_pointer_v<Key>) {
      memcpy(key_ptr, key, key_length);
    } else {
      memcpy(key_ptr, &key, key_length);
    }
  }

  void
  SetPayload(  //
      const Payload &payload,
      const size_t payload_length,
      const size_t offset)
  {
    const auto payload_ptr = ShiftAddress(this, offset);
    if constexpr (std::is_pointer_v<Payload>) {
      memcpy(payload_ptr, payload, payload_length);
    } else {
      memcpy(payload_ptr, &payload, payload_length);
    }
  }

  size_t
  SetRecord(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload,
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

  size_t
  CopyRecord(  //
      const BaseNode *original_node,
      const Metadata meta,
      size_t offset)
  {
    const auto total_length = meta.GetTotalLength();

    offset -= total_length;
    const auto dest = ShiftAddress(this, offset);
    const auto src = original_node->GetKeyAddr(meta);
    memcpy(dest, src, total_length);

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

    auto page = calloc(1, node_size);
    auto new_node = new (page) BaseNode{node_size, is_leaf};
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

  constexpr void *
  GetKeyAddr(const Metadata meta) const
  {
    const auto offset = meta.GetOffset();
    return ShiftAddress(this, offset);
  }

  std::pair<Key, size_t>
  GetKeyAndItsLength(const size_t index) const
  {
    const auto meta = GetMetadata(index);
    return {CastKey<Key>(GetKeyAddr(meta)), meta.GetKeyLength()};
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
  SetChildForMwCAS(  //
      MwCASDescriptor &desc,
      const size_t index,
      const void *old_addr,
      const void *new_addr)
  {
    desc.AddMwCASTarget(GetPayloadAddr(GetMetadata(index)), reinterpret_cast<uintptr_t>(old_addr),
                        reinterpret_cast<uintptr_t>(new_addr));
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
  std::pair<KeyExistence, size_t>
  SearchSortedMetadata(  //
      const Key &key,
      const bool range_is_closed) const
  {
    const int64_t sorted_count = GetSortedCount();

    int64_t begin_index = 0;
    int64_t end_index = sorted_count;
    int64_t index = end_index / 2;

    while (begin_index <= end_index && index < sorted_count) {
      const auto meta = GetMetadata(index);
      const auto index_key = CastKey<Key>(GetKeyAddr(meta));
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
