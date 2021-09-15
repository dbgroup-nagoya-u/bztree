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

#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <memory>
#include <utility>

#include "metadata.hpp"
#include "status_word.hpp"

namespace dbgroup::index::bztree::component
{
/**
 * @brief A class to represent nodes in BzTree.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 */
template <class Key, class Payload, class Compare>
class alignas(kCacheLineSize) Node
{
 private:
  /*################################################################################################
   * Internal variables
   *##############################################################################################*/

  /// the byte length of a node page.
  uint64_t node_size_ : 32;

  /// a flag to indicate whether this node is a leaf or internal node.
  uint64_t is_leaf_ : 1;

  /// the number of sorted records.
  uint64_t sorted_count_ : 16;

  /// a black block for alignment.
  uint64_t : 0;

  /// a status word.
  StatusWord status_;

  /// the head of a metadata array.
  Metadata meta_array_[0];

 public:
  /*################################################################################################
   * Public constants
   *##############################################################################################*/

  /// the maximum number of records in a node
  static constexpr size_t kMaxRecordNum = GetMaxRecordNum<Key, Payload>();

  /*################################################################################################
   * Public structs to cpmare key & metadata pairs
   *##############################################################################################*/

  /**
   * @brief A class to sort metadata.
   *
   */
  struct MetaRecord {
    /// a target metadata.
    Metadata meta{};

    /// a target key.
    Key key{};

    /**
     * @brief An operator for less than comparison.
     *
     */
    constexpr bool
    operator<(const MetaRecord &obj) const
    {
      return Compare{}(this->key, obj.key);
    }

    /**
     * @brief An operator to check equality.
     *
     */
    constexpr bool
    operator==(const MetaRecord &obj) const
    {
      return !Compare{}(this->key, obj.key) && !Compare{}(obj.key, this->key);
    }

    /**
     * @brief An operator to check inequality.
     *
     */
    constexpr bool
    operator!=(const MetaRecord &obj) const
    {
      return Compare{}(this->key, obj.key) || Compare{}(obj.key, this->key);
    }
  };

  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new node object.
   *
   * @param is_leaf a flag to indicate whether a leaf node is constructed.
   */
  explicit Node(const bool is_leaf)
      : node_size_{kPageSize}, is_leaf_{is_leaf}, sorted_count_{0}, status_{}
  {
  }

  /**
   * @brief Destroy the node object.
   *
   */
  ~Node() = default;

  Node(const Node &) = delete;
  Node &operator=(const Node &) = delete;
  Node(Node &&) = delete;
  Node &operator=(Node &&) = delete;

  /*################################################################################################
   * new/delete definitions
   *##############################################################################################*/

  static void *operator new(std::size_t) { return calloc(1UL, kPageSize); }

  static void
  operator delete(void *p) noexcept
  {
    free(p);
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @retval true if this is a leaf node.
   * @retval false if this is an internal node.
   */
  constexpr bool
  IsLeaf() const
  {
    return is_leaf_;
  }

  /**
   * @return size_t: the number of sorted records.
   */
  constexpr size_t
  GetSortedCount() const
  {
    return sorted_count_;
  }

  /**
   * @brief Read a status word without MwCAS read protection.
   *
   * This function requires some method of protection (e.g., locks) to read a status
   * word safely.
   *
   * @return StatusWord: a status word.
   */
  constexpr StatusWord
  GetStatusWord() const
  {
    return status_;
  }

  /**
   * @brief Read a status word with MwCAS read protection.
   *
   * This function uses a MwCAS read operation internally, and so it is guaranteed that
   * a read status word is valid.
   *
   * @return StatusWord: a status word.
   */
  StatusWord
  GetStatusWordProtected() const
  {
    return ReadMwCASField<StatusWord>(&status_);
  }

  /**
   * @brief Read metadata without MwCAS read protection.
   *
   * This function requires some method of protection (e.g., locks) to read metadata
   * safely.
   *
   * @return Metadata: metadata.
   */
  constexpr Metadata
  GetMetadata(const size_t position) const
  {
    return meta_array_[position];
  }

  /**
   * @brief Read metadata with MwCAS read protection.
   *
   * This function uses a MwCAS read operation internally, and so it is guaranteed that
   * read metadata is valid.
   *
   * @return Metadata: metadata.
   */
  Metadata
  GetMetadataProtected(const size_t position) const
  {
    return ReadMwCASField<Metadata>(&meta_array_[position]);
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return auto: an address of a target key.
   */
  constexpr auto
  GetKeyAddr(const Metadata meta) const
  {
    if constexpr (std::is_pointer_v<Key>) {
      return Cast<Key>(ShiftAddress(this, meta.GetOffset()));
    } else {
      return Cast<Key *>(ShiftAddress(this, meta.GetOffset()));
    }
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return Key: a target key.
   */
  constexpr Key
  GetKey(const Metadata meta) const
  {
    if constexpr (std::is_pointer_v<Key>) {
      return GetKeyAddr(meta);
    } else {
      return *GetKeyAddr(meta);
    }
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return void*: an address of a target payload.
   */
  constexpr void *
  GetPayloadAddr(const Metadata meta) const
  {
    return ShiftAddress(this, meta.GetOffset() + meta.GetKeyLength());
  }

  /**
   * @brief Copy a target payload to a specified reference.
   *
   * @param meta metadata of a corresponding record.
   * @param out_payload a reference to be copied a target payload.
   */
  void
  CopyPayload(  //
      const Metadata meta,
      Payload &out_payload) const
  {
    if constexpr (IsVariableLengthData<Payload>()) {
      const auto payload_length = meta.GetPayloadLength();
      out_payload = reinterpret_cast<Payload>(::operator new(payload_length));
      memcpy(out_payload, this->GetPayloadAddr(meta), payload_length);
    } else if constexpr (CanCASUpdate<Payload>()) {
      out_payload = ReadMwCASField<Payload>(this->GetPayloadAddr(meta));
    } else {
      memcpy(&out_payload, this->GetPayloadAddr(meta), sizeof(Payload));
    }
  }

  /**
   * @brief Set the number of sorted records.
   *
   * @param sorted_count the number of sorted records.
   */
  constexpr void
  SetSortedCount(const size_t sorted_count)
  {
    sorted_count_ = sorted_count;
  }

  /**
   * @brief Set a status word.
   *
   * @param status a status word.
   */
  constexpr void
  SetStatus(const StatusWord status)
  {
    status_ = status;
  }

  /**
   * @brief Set metadata directly.
   *
   * This function requires some method of protection (e.g., locks) to set metadata
   * safely.
   *
   * @param position the position of metadata to be set.
   * @param new_meta metadata to be set.
   */
  constexpr void
  SetMetadata(  //
      const size_t position,
      const Metadata new_meta)
  {
    meta_array_[position] = new_meta;
  }

  /**
   * @brief Set metadata by using a CAS operation.
   *
   * @param position the position of metadata to be set.
   * @param new_meta metadata to be set.
   */
  void
  SetMetadataByCAS(  //
      const size_t position,
      const Metadata new_meta)
  {
    reinterpret_cast<std::atomic<Metadata> *>(meta_array_ + position)->store(new_meta, mo_relax);
  }

  /**
   * @brief Set a target key.
   *
   * @param offset an offset to set a target key.
   * @param key a target key to be set.
   * @param key_length the length of a target key.
   */
  void
  SetKey(  //
      size_t &offset,
      const Key &key,
      const size_t key_length)
  {
    if constexpr (IsVariableLengthData<Key>()) {
      offset -= key_length;
      memcpy(ShiftAddress(this, offset), key, key_length);
    } else {
      offset -= sizeof(Key);
      memcpy(ShiftAddress(this, offset), &key, sizeof(Key));
    }
  }

  /**
   * @brief Set a target payload directly.
   *
   * @tparam T a class of a target payload.
   * @param offset an offset to set a target payload.
   * @param payload a target payload to be set.
   * @param payload_length the length of a target payload.
   */
  template <class T>
  void
  SetPayload(  //
      size_t &offset,
      const T &payload,
      const size_t payload_length)
  {
    if constexpr (IsVariableLengthData<T>()) {
      offset -= payload_length;
      memcpy(ShiftAddress(this, offset), payload, payload_length);
    } else {
      offset -= sizeof(T);
      memcpy(ShiftAddress(this, offset), &payload, sizeof(T));
    }
  }

  /**
   * @brief Set an old/new status word pair to a MwCAS target.
   *
   * @param desc a target MwCAS descriptor.
   * @param old_status an old status word for MwCAS.
   * @param new_status a new status word for MwCAS.
   */
  constexpr void
  SetStatusForMwCAS(  //
      MwCASDescriptor &desc,
      const StatusWord old_status,
      const StatusWord new_status)
  {
    desc.AddMwCASTarget(&status_, old_status, new_status);
  }

  /**
   * @brief Set an old/new metadata pair to a MwCAS target.
   *
   * @param desc a target MwCAS descriptor.
   * @param position the position of target metadata.
   * @param old_meta old metadata for MwCAS.
   * @param new_meta new metadata for MwCAS.
   */
  constexpr void
  SetMetadataForMwCAS(  //
      MwCASDescriptor &desc,
      const size_t position,
      const Metadata old_meta,
      const Metadata new_meta)
  {
    desc.AddMwCASTarget(meta_array_ + position, old_meta, new_meta);
  }

  /**
   * @brief Set an old/new payload pair to a MwCAS target.
   *
   * An old payload is copied from a node by using specified metadata.
   *
   * @param desc a target MwCAS descriptor.
   * @param meta metadata of a target record.
   * @param new_payload a new payload for MwCAS.
   */
  void
  SetPayloadForMwCAS(  //
      MwCASDescriptor &desc,
      const Metadata meta,
      const Payload &new_payload)
  {
    static_assert(CanCASUpdate<Payload>());

    Payload old_payload{};
    this->CopyPayload(meta, old_payload);
    desc.AddMwCASTarget(ShiftAddress(this, meta.GetOffset() + meta.GetKeyLength()),  //
                        old_payload, new_payload);
  }

  /**
   * @brief Set an old/new payload pair to a MwCAS target.
   *
   * @tparam T a class of a target payload.
   * @param desc a target MwCAS descriptor.
   * @param meta metadata of a target record.
   * @param old_payload an old payload for MwCAS.
   * @param new_payload a new payload for MwCAS.
   */
  template <class T>
  constexpr void
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
   * @brief Freeze this node for SMOs.
   *
   * @retval kSuccess if a node has become frozen.
   * @retval kFrozen if a node is already frozen.
   */
  NodeReturnCode
  Freeze()
  {
    while (true) {
      const auto current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) return NodeReturnCode::kFrozen;

      MwCASDescriptor desc;
      SetStatusForMwCAS(desc, current_status, current_status.Freeze());
      if (desc.MwCAS()) break;
    }

    return NodeReturnCode::kSuccess;
  }
};

}  // namespace dbgroup::index::bztree::component
