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

#include "metadata.hpp"
#include "status_word.hpp"

namespace bztree
{
class BaseNode
{
 private:
  /*################################################################################################
   * Internal enum and constants
   *##############################################################################################*/

  // byte offsets to acces node's elements
  static constexpr uint64_t kNodeSizeOffset = 0;
  static constexpr uint64_t kStatusWordOffset = 4;
  static constexpr uint64_t kIsLeafOffset = 12;
  static constexpr uint64_t kSortedCountOffset = 14;
  static constexpr uint64_t kRecordMetadataOffset = 16;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::unique_ptr<std::byte[]> page_;

  uint32_t *node_size_;

  uint64_t *status_word_;

  bool *is_leaf_;

  uint16_t *sorted_count_;

  uint64_t *metadata_array_;

 protected:
  /*################################################################################################
   * Internally inherited enum and constants
   *##############################################################################################*/

  enum KeyExistence
  {
    kExist = 0,
    kNotExist,
    kDeleted
  };

  // header length in bytes
  static constexpr size_t kHeaderLength = kRecordMetadataOffset;

  /*################################################################################################
   * Internally inherited getters/setters
   *##############################################################################################*/

  std::byte *
  GetKeyPtr(const uint64_t meta)
  {
    const auto offset = Metadata::GetOffset(meta);
    return ShiftAddress(page_.get(), offset);
  }

  std::byte *
  GetPayloadPtr(const uint64_t meta)
  {
    const auto offset = Metadata::GetOffset(meta) + Metadata::GetKeyLength(meta);
    return ShiftAddress(page_.get(), offset);
  }

  void
  SetNodeSize(const size_t size)
  {
    *node_size_ = size;
  }

  void
  SetStatusWord(const uint64_t status)
  {
    *status_word_ = status;
  }

  void
  SetIsLeaf(const bool is_leaf)
  {
    *is_leaf_ = is_leaf;
  }

  void
  SetSortedCount(const size_t sorted_count)
  {
    *sorted_count_ = sorted_count;
  }

  void
  SetMetadata(  //
      const size_t index,
      const uint64_t new_meta)
  {
    *(metadata_array_ + index) = new_meta;
  }

  void
  SetKey(  //
      const std::byte *key,
      const size_t key_length,
      const size_t offset)
  {
    const auto key_ptr = ShiftAddress(page_.get(), offset);
    memcpy(key_ptr, key, key_length);
  }

  void
  SetPayload(  //
      const std::byte *payload,
      const size_t payload_length,
      const size_t offset)
  {
    const auto payload_ptr = ShiftAddress(page_.get(), offset);
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
    SetPayload(payload, payload_length, offset);
    offset -= key_length;
    SetKey(key, key_length, offset);
    return offset;
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
  template <class Compare>
  std::pair<KeyExistence, size_t>
  SearchSortedMetadata(  //
      const std::byte *key,
      const bool range_is_closed,
      Compare comp)
  {
    // TODO(anyone) implement binary search
    const auto sorted_count = GetSortedCount();
    size_t index;
    for (index = 0; index < sorted_count; index++) {
      const auto meta = GetMetadata(index);
      const std::byte *index_key = GetKeyPtr(meta);
      if (IsEqual(key, index_key, comp)) {
        if (Metadata::IsVisible(meta)) {
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
    kConsolidationRequired,
    kSplitRequired,
    kMergeRequired,
    kSiblingHasNoSpace,
    kCASFailed
  };

  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  explicit BaseNode(const size_t node_size, const bool is_leaf)
  {
    // prepare a page region
    page_ = std::make_unique<std::byte[]>(node_size);

    // set page addresses
    node_size_ = reinterpret_cast<uint32_t *>(ShiftAddress(page_.get(), kNodeSizeOffset));
    status_word_ = reinterpret_cast<uint64_t *>(ShiftAddress(page_.get(), kStatusWordOffset));
    is_leaf_ = reinterpret_cast<bool *>(ShiftAddress(page_.get(), kIsLeafOffset));
    sorted_count_ = reinterpret_cast<uint16_t *>(ShiftAddress(page_.get(), kSortedCountOffset));
    metadata_array_ =
        reinterpret_cast<uint64_t *>(ShiftAddress(page_.get(), kRecordMetadataOffset));

    // initialize header
    SetNodeSize(node_size);
    SetStatusWord(0);
    SetIsLeaf(is_leaf);
    SetSortedCount(0);
  }

  BaseNode(const BaseNode &) = delete;
  BaseNode &operator=(const BaseNode &) = delete;
  BaseNode(BaseNode &&) = delete;
  BaseNode &operator=(BaseNode &&) = delete;
  virtual ~BaseNode() = default;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  bool
  IsLeaf()
  {
    return *is_leaf_;
  }

  bool
  IsFrozen()
  {
    return StatusWord::IsFrozen(GetStatusWordProtected());
  }

  bool
  RecordIsVisible(const size_t index)
  {
    return Metadata::IsVisible(GetMetadata(index));
  }

  bool
  RecordIsDeleted(const size_t index)
  {
    return Metadata::IsDeleted(GetMetadata(index));
  }

  size_t
  GetNodeSize()
  {
    return *node_size_;
  }

  uint64_t
  GetStatusWord()
  {
    return *status_word_;
  }

  uint64_t
  GetStatusWordProtected()
  {
    return reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(status_word_)->GetValueProtected();
  }

  size_t
  GetRecordCount()
  {
    return StatusWord::GetRecordCount(GetStatusWord());
  }

  size_t
  GetBlockSize()
  {
    return StatusWord::GetBlockSize(GetStatusWord());
  }

  size_t
  GetDeletedSize()
  {
    return StatusWord::GetDeletedSize(GetStatusWord());
  }

  size_t
  GetApproximateDataSize()
  {
    const auto status = GetStatusWord();
    return (Metadata::kMetadataByteLength * StatusWord::GetRecordCount(status))
           + StatusWord::GetBlockSize(status) - StatusWord::GetDeletedSize(status);
  }

  size_t
  GetSortedCount()
  {
    return *sorted_count_;
  }

  uint64_t
  GetMetadata(const size_t index)
  {
    return *(metadata_array_ + index);
  }

  uint64_t
  GetMetadataProtected(const size_t index)
  {
    return reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(metadata_array_ + index)
        ->GetValueProtected();
  }

  size_t
  GetKeyLength(const size_t index)
  {
    return Metadata::GetKeyLength(GetMetadata(index));
  }

  size_t
  GetPayloadLength(const size_t index)
  {
    return Metadata::GetPayloadLength(GetMetadata(index));
  }

  uint32_t
  SetStatusForMwCAS(  //
      const uint64_t old_status,
      const uint64_t new_status,
      pmwcas::Descriptor *descriptor)
  {
    return descriptor->AddEntry(status_word_, old_status, new_status);
  }

  uint32_t
  SetMetadataForMwCAS(  //
      const size_t index,
      const uint64_t old_meta,
      const uint64_t new_meta,
      pmwcas::Descriptor *descriptor)
  {
    return descriptor->AddEntry(metadata_array_ + index, old_meta, new_meta);
  }

  template <typename T>
  uint32_t
  SetPayloadForMwCAS(  //
      const size_t index,
      const T *old_payload,
      const T *new_payload,
      pmwcas::Descriptor *descriptor)
  {
    return descriptor->AddEntry(reinterpret_cast<uint64_t *>(GetPayloadPtr(GetMetadata(index))),
                                reinterpret_cast<uint64_t>(old_payload),
                                reinterpret_cast<uint64_t>(new_payload));
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
      if (StatusWord::IsFrozen(current_status)) {
        return NodeReturnCode::kFrozen;
      }

      const auto new_status = StatusWord::Freeze(current_status);
      pd = pmwcas_pool->AllocateDescriptor();
      SetStatusForMwCAS(current_status, new_status, pd);
    } while (pd->MwCAS());

    return NodeReturnCode::kSuccess;
  }

  // WIP
  void
  Dump()
  {
    std::cout << "size: " << node_size_ << ": " << GetNodeSize() << std::endl
              << "stat: " << status_word_ << ": " << GetStatusWord() << std::endl
              << "leaf: " << is_leaf_ << ": " << IsLeaf() << std::endl
              << "sort: " << sorted_count_ << ": " << GetSortedCount() << std::endl;
    for (size_t i = 0; i < 10; i++) {
      std::cout << "m[" << i << "]: " << metadata_array_ + i << ": " << GetMetadata(i) << std::endl;
    }
  }
};

}  // namespace bztree
