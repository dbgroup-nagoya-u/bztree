// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <mwcas/mwcas.h>
#include <pmwcas.h>

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "metadata.hpp"
#include "status_word.hpp"

namespace bztree
{
template <class Key, class Payload, template <typename> class Compare>
class alignas(kWordLength) BaseNode
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  uint64_t node_size_ : 32;

  bool is_leaf_ : 1;

  uint64_t sorted_count_ : 16;

  uint64_t : 0;

  StatusUnion status_;

  MetaUnion meta_array_[0];

  /*################################################################################################
   * Internal getter/setter for test use
   *##############################################################################################*/

  uint64_t
  GetHeadAddrForTest(void)
  {
    return reinterpret_cast<uint64_t>(reinterpret_cast<std::byte *>(this));
  }

  uint64_t
  GetStatusWordAddrForTest(void)
  {
    return reinterpret_cast<uint64_t>(reinterpret_cast<std::byte *>(&status_));
  }

  uint64_t
  GetMetadataArrayAddrForTest(void)
  {
    return reinterpret_cast<uint64_t>(reinterpret_cast<std::byte *>(meta_array_));
  }

 protected:
  /*################################################################################################
   * Internally inherited enum and constants
   *##############################################################################################*/

  enum KeyExistence
  {
    kExist = 0,
    kNotExist,
    kDeleted,
    kUncertain
  };

  // header length in bytes
  static constexpr size_t kHeaderLength = 2 * kWordLength;

  /*################################################################################################
   * Internally inherited constructors
   *##############################################################################################*/

  explicit BaseNode(const size_t node_size, const bool is_leaf)
  {
    // initialize header
    SetNodeSize(node_size);
    SetStatusWord(StatusWord{});
    SetIsLeaf(is_leaf);
    SetSortedCount(0);
  }

  /*################################################################################################
   * Internally inherited getters/setters
   *##############################################################################################*/

  constexpr std::byte *
  GetKeyPtr(const Metadata meta)
  {
    const auto offset = meta.GetOffset();
    return ShiftAddress(this, offset);
  }

  constexpr Key
  GetKey(const Metadata meta)
  {
    const auto key_ptr = GetKeyPtr(meta);
    return *static_cast<Key *>(static_cast<void *>(key_ptr));
  }

  constexpr std::byte *
  GetPayloadPtr(const Metadata meta)
  {
    const auto offset = meta.GetOffset() + meta.GetKeyLength();
    return ShiftAddress(this, offset);
  }

  void
  SetNodeSize(const size_t size)
  {
    node_size_ = size;
  }

  void
  SetStatusWord(const StatusWord status)
  {
    status_.word = status;
  }

  void
  SetIsLeaf(const bool is_leaf)
  {
    is_leaf_ = is_leaf;
  }

  void
  SetSortedCount(const size_t sorted_count)
  {
    sorted_count_ = sorted_count;
  }

  void
  SetMetadata(  //
      const size_t index,
      const Metadata new_meta)
  {
    (meta_array_ + index)->meta = new_meta;
  }

  void
  CopyKey(  //
      const std::byte *key,
      const size_t key_length,
      const size_t offset)
  {
    const auto key_ptr = ShiftAddress(this, offset);
    memcpy(key_ptr, key, key_length);
  }

  void
  CopyPayload(  //
      const std::byte *payload,
      const size_t payload_length,
      const size_t offset)
  {
    const auto payload_ptr = ShiftAddress(this, offset);
    memcpy(payload_ptr, payload, payload_length);
  }

  size_t
  CopyRecord(  //
      const std::byte *key,
      const size_t key_length,
      const std::byte *payload,
      const size_t payload_length,
      size_t offset)
  {
    offset -= payload_length;
    CopyPayload(payload, payload_length, offset);
    offset -= key_length;
    CopyKey(key, key_length, offset);
    return offset;
  }

  size_t
  SetRecord(  //
      const Key key,
      const size_t key_length,
      const Payload payload,
      const size_t payload_length,
      size_t offset)
  {
    const auto byte_key = CastToBytePtr(key);
    const auto byte_payload = CastToBytePtr(payload);
    return CopyRecord(byte_key, key_length, byte_payload, payload_length, offset);
  }

  /*################################################################################################
   * Internally inherited utility functions
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
  std::pair<KeyExistence, size_t>
  SearchSortedMetadata(  //
      const Key key,
      const bool range_is_closed,
      Compare<Key> comp)
  {
    // TODO(anyone) implement binary search
    const auto sorted_count = GetSortedCount();
    size_t index;
    for (index = 0; index < sorted_count; index++) {
      const auto meta = GetMetadata(index);
      const Key index_key = GetKey(meta);
      if (IsEqual(key, index_key, comp)) {
        if (meta.IsVisible()) {
          return {KeyExistence::kExist, (range_is_closed) ? index : index + 1};
        } else {
          // there is no inserting nor corrupted record in a sorted region
          return {KeyExistence::kDeleted, index};
        }
      } else if (comp(key, index_key)) {
        break;
      }
    }
    return {KeyExistence::kNotExist, index};
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

  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  BaseNode(const BaseNode &) = delete;
  BaseNode &operator=(const BaseNode &) = delete;
  BaseNode(BaseNode &&) = default;
  BaseNode &operator=(BaseNode &&) = default;
  ~BaseNode() = default;

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
    auto new_node = new (aligned_page) BaseNode<Key, Payload, Compare>{node_size, is_leaf};
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

  bool
  IsFrozen()
  {
    return GetStatusWordProtected().IsFrozen();
  }

  constexpr bool
  RecordIsVisible(const size_t index) const
  {
    return GetMetadata(index).IsVisible();
  }

  constexpr bool
  RecordIsDeleted(const size_t index) const
  {
    return GetMetadata(index).IsDeleted();
  }

  constexpr size_t
  GetNodeSize() const
  {
    return node_size_;
  }

  constexpr StatusWord
  GetStatusWord() const
  {
    return status_.word;
  }

  StatusWord
  GetStatusWordProtected()
  {
    auto status_addr = &status_.int_word;
    const auto protected_status =
        reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(status_addr)->GetValueProtected();
    return StatusUnion{protected_status}.word;
  }

  constexpr size_t
  GetRecordCount() const
  {
    return GetStatusWord().GetRecordCount();
  }

  constexpr size_t
  GetBlockSize() const
  {
    return GetStatusWord().GetBlockSize();
  }

  constexpr size_t
  GetDeletedSize() const
  {
    return GetStatusWord().GetDeletedSize();
  }

  constexpr size_t
  GetApproximateDataSize() const
  {
    return (kWordLength * GetStatusWord().GetRecordCount()) + GetStatusWord().GetBlockSize()
           - GetStatusWord().GetDeletedSize();
  }

  constexpr size_t
  GetSortedCount() const
  {
    return sorted_count_;
  }

  constexpr Metadata
  GetMetadata(const size_t index) const
  {
    return (meta_array_ + index)->meta;
  }

  Metadata
  GetMetadataProtected(const size_t index)
  {
    auto meta_addr = &((meta_array_ + index)->int_meta);
    auto protected_meta =
        reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(meta_addr)->GetValueProtected();
    return MetaUnion{protected_meta}.meta;
  }

  constexpr size_t
  GetKeyLength(const size_t index) const
  {
    return GetMetadata(index).GetKeyLength();
  }

  constexpr size_t
  GetPayloadLength(const size_t index) const
  {
    return GetMetadata(index).GetPayloadLength();
  }

  uint32_t
  SetStatusForMwCAS(  //
      StatusWord old_status,
      StatusWord new_status,
      pmwcas::Descriptor *descriptor)
  {
    auto status_addr = &status_.int_word;
    auto old_stat_int = CastToUint64(&old_status));
    auto new_stat_int = CastToUint64(&new_status));
    return descriptor->AddEntry(status_addr, old_stat_int, new_stat_int);
  }

  uint32_t
  SetMetadataForMwCAS(  //
      const size_t index,
      Metadata old_meta,
      Metadata new_meta,
      pmwcas::Descriptor *descriptor)
  {
    auto meta_addr = &((meta_array_ + index)->int_meta);
    auto old_meta_int = CastToUint64(&old_meta));
    auto new_meta_int = CastToUint64(&new_meta));
    return descriptor->AddEntry(meta_addr, old_meta_int, new_meta_int);
  }

  template <typename T>
  uint32_t
  SetPayloadForMwCAS(  //
      const size_t index,
      const T *old_payload,
      const T *new_payload,
      pmwcas::Descriptor *descriptor)
  {
    return descriptor->AddEntry(CastToUint64Ptr(GetPayloadPtr(GetMetadata(index))),
                                CastToUint64(old_payload), CastToUint64(new_payload));
  }

  /*################################################################################################
   * Public getter/setter for test use
   *##############################################################################################*/

  size_t
  GetStatusWordOffsetForTest(void)
  {
    return GetStatusWordAddrForTest() - GetHeadAddrForTest();
  }

  size_t
  GetMetadataOffsetForTest(void)
  {
    return GetMetadataArrayAddrForTest() - GetStatusWordAddrForTest();
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
  Freeze(pmwcas::DescriptorPool *pmwcas_pool)
  {
    pmwcas::Descriptor *pd;
    do {
      const auto current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return NodeReturnCode::kFrozen;
      }

      const auto new_status = current_status.Freeze();
      pd = pmwcas_pool->AllocateDescriptor();
      SetStatusForMwCAS(current_status, new_status, pd);
    } while (pd->MwCAS());

    return NodeReturnCode::kSuccess;
  }

  static size_t
  ComputeOccupiedSize(const std::vector<std::pair<std::byte *, Metadata>> &live_meta)
  {
    size_t block_size = 0;
    for (auto &&[key, meta] : live_meta) {
      block_size += meta.GetTotalLength();
    }
    block_size += kHeaderLength + (kWordLength * live_meta.size());

    return block_size;
  }
};

}  // namespace bztree
