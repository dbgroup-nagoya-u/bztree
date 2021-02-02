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
    return reinterpret_cast<uint64_t>(this);
  }

  uint64_t
  GetStatusWordAddrForTest(void)
  {
    return reinterpret_cast<uint64_t>(&status_);
  }

  uint64_t
  GetMetadataArrayAddrForTest(void)
  {
    return reinterpret_cast<uint64_t>(meta_array_);
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
    offset -= key_length;
    SetKey(key, key_length, offset);
    return offset;
  }

  /*################################################################################################
   * Internally inherited utility functions
   *##############################################################################################*/

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
    const auto protected_status = status_.target_field.GetValueProtected();
    return StatusUnion{protected_status}.word;
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

  constexpr void *
  GetPayloadAddr(const size_t index) const
  {
    return GetPayloadAddr(GetMetadata(index));
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
      const StatusWord old_status,
      const StatusWord new_status,
      pmwcas::Descriptor *descriptor)
  {
    auto status_addr = &status_.int_word;
    auto old_stat_int = StatusUnion{old_status}.int_word;
    auto new_stat_int = StatusUnion{new_status}.int_word;
    return descriptor->AddEntry(status_addr, old_stat_int, new_stat_int);
  }

  uint32_t
  SetMetadataForMwCAS(  //
      const size_t index,
      const Metadata old_meta,
      const Metadata new_meta,
      pmwcas::Descriptor *descriptor)
  {
    auto meta_addr = &((meta_array_ + index)->int_meta);
    auto old_meta_int = MetaUnion{old_meta}.int_meta;
    auto new_meta_int = MetaUnion{new_meta}.int_meta;
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
    return descriptor->AddEntry(static_cast<uint64_t *>(GetPayloadAddr(GetMetadata(index))),
                                PayloadUnion{old_payload}.int_payload,
                                PayloadUnion{new_payload}.int_payload);
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
    // TODO(anyone) implement binary search
    const auto sorted_count = GetSortedCount();
    size_t index;
    for (index = 0; index < sorted_count; index++) {
      const auto meta = GetMetadata(index);
      const void *index_key = GetKeyAddr(meta);
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

  static BaseNode *
  CreateNewRoot(  //
      const BaseNode *left_child,
      const BaseNode *right_child)
  {
    auto offset = left_child->GetNodeSize();
    auto new_root = new BaseNode{offset, false};

    // insert a left child node
    auto meta = left_child->GetMetadata(left_child->GetSortedCount() - 1);
    auto key = left_child->GetKeyAddr(meta);
    auto key_length = meta.GetKeyLength();
    auto node_addr = GetAddr(left_child);
    offset = new_root->CopyRecord(key, key_length, node_addr, kPointerLength, offset);
    auto new_meta = kInitMetadata.SetRecordInfo(offset, key_length, key_length + kPointerLength);
    new_root->SetMetadata(0, new_meta);

    // insert a right child node
    meta = right_child->GetMetadata(right_child->GetSortedCount() - 1);
    key = right_child->GetKeyAddr(meta);
    key_length = meta.GetKeyLength();
    node_addr = GetAddr(right_child);
    offset = new_root->CopyRecord(key, key_length, node_addr, kPointerLength, offset);
    new_meta = kInitMetadata.SetRecordInfo(offset, key_length, key_length + kPointerLength);
    new_root->SetMetadata(1, new_meta);

    // set a new header
    new_root->SetSortedCount(2);
    new_root->SetStatusWord(kInitStatusWord.AddRecordInfo(2, offset, 0));

    return new_root;
  }

  BaseNode *
  NewParentForSplit(  //
      const BaseNode *left_child,
      const BaseNode *right_child,
      const size_t split_index)
  {
    auto offset = left_child->GetNodeSize();
    auto new_parent = new BaseNode{offset, false};

    // copy child nodes with inserting new split ones
    auto record_count = GetSortedCount();
    for (size_t old_idx = 0, new_idx = 0; old_idx < record_count; ++old_idx, ++new_idx) {
      // prepare copying record and metadata
      const auto meta = GetMetadata(old_idx);
      const auto key = GetKeyAddr(meta);
      const auto key_length = meta.GetKeyLength();
      auto node_addr = GetPayloadAddr(meta);
      if (old_idx == split_index) {
        // prepare left child information
        const auto last_meta = left_child->GetMetadata(left_child->GetSortedCount() - 1);
        const auto new_key = left_child->GetKeyAddr(last_meta);
        const auto new_key_length = last_meta.GetKeyLength();
        const auto left_addr = GetAddr(left_child);
        // insert a split left child
        offset = new_parent->CopyRecord(new_key, new_key_length, left_addr, kPointerLength, offset);
        const auto total_length = new_key_length + kPointerLength;
        const auto left_meta = kInitMetadata.SetRecordInfo(offset, new_key_length, total_length);
        new_parent->SetMetadata(new_idx++, left_meta);
        // insert a split right child
        node_addr = GetAddr(right_child);
      }
      // copy a child node
      offset = new_parent->CopyRecord(key, key_length, node_addr, kPointerLength, offset);
      const auto new_meta = meta.UpdateOffset(offset);
      new_parent->SetMetadata(new_idx, new_meta);
    }

    // set a new header
    SetSortedCount(++record_count);
    SetStatusWord(kInitStatusWord.AddRecordInfo(record_count, offset, 0));

    return new_parent;
  }

  template <class Compare>
  BaseNode *
  NewParentForMerge(  //
      const BaseNode *merged_child,
      const size_t deleted_index,
      const Compare &comp)
  {
    auto offset = GetNodeSize();
    auto new_parent = new BaseNode{offset, false};

    // copy child nodes with deleting a merging target node
    auto record_count = GetSortedCount();
    for (size_t old_idx = 0, new_idx = 0; old_idx < record_count; ++old_idx, ++new_idx) {
      // prepare copying record and metadata
      auto meta = GetMetadata(old_idx);
      auto key = GetKeyAddr(meta);
      auto key_length = meta.GetKeyLength();
      auto node_addr = GetPayloadAddr(meta);
      if (old_idx == deleted_index) {
        // skip a deleted node and insert a merged node
        meta = GetMetadata(++old_idx);
        key = GetKeyAddr(meta);
        key_length = meta.GetKeyLength();
        node_addr = GetAddr(merged_child);
      }
      // copy a child node
      offset = new_parent->CopyRecord(key, key_length, node_addr, kPointerLength, offset);
      const auto new_meta = meta.UpdateOffset(offset);
      new_parent->SetMetadata(new_idx, new_meta);
    }

    // set a new header
    new_parent->SetSortedCount(--record_count);
    new_parent->SetStatusWord(kInitStatusWord.AddRecordInfo(record_count, offset, 0));

    return new_parent;
  }
};

}  // namespace bztree
