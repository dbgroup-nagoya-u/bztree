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
#include <string.h>

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
   * Internal classes
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
   * Internal utility functions
   *##############################################################################################*/

  /**
   * @brief Align a specified block size if needed.
   *
   * @param block_size a target block size.
   * @return size_t: an aligned block size.
   */
  static constexpr auto
  GetAlignedSize(const size_t block_size)  //
      -> size_t
  {
    if constexpr (CanCASUpdate<Payload>()) {
      if constexpr (IsVariableLengthData<Key>()) {
        const auto align_size = block_size & (kWordLength - 1);
        if (align_size > 0) {
          return block_size + (kWordLength - align_size);
        }
      } else if constexpr (sizeof(Key) % kWordLength != 0) {
        constexpr auto kAlignedSize = sizeof(Key) - (sizeof(Key) % kWordLength);
        return block_size + kAlignedSize;
      }
    }
    return block_size;
  }

  /**
   * @brief Check a target node requires consolidation.
   *
   * @param status a current status word of a target node.
   * @param block_size a current block size of a target node.
   * @retval true if a target node has sufficient space for inserting a new record.
   * @retval false if a target node requires consolidation.
   */
  constexpr auto
  HasSpace(  //
      const StatusWord status,
      const size_t block_size) const  //
      -> bool
  {
    return status.GetRecordCount() - GetSortedCount() < kMaxUnsortedRecNum
           && status.GetOccupiedSize() + block_size <= kPageSize - kWordLength
           && status.GetDeletedSize() < kMaxDeletedSpaceSize;
  }

  /**
   * @brief Sort unsorted metadata by using inserting sort for SMOs.
   *
   * @tparam Key a target key class.
   * @tparam Payload a target payload class.
   * @tparam Compare a comparetor class for keys.
   * @param node a target leaf node.
   * @param arr a result array of sorted metadata.
   * @param count the number of metadata.
   */
  auto
  SortNewRecords() const  //
      -> std::pair<size_t, std::array<MetaRecord, kMaxUnsortedRecNum>>
  {
    const int64_t rec_count = GetStatusWordProtected().GetRecordCount();
    const int64_t sorted_count = GetSortedCount();

    std::array<MetaRecord, kMaxUnsortedRecNum> arr;
    size_t count = 0;

    // sort unsorted records by insertion sort
    for (int64_t index = rec_count - 1; index >= sorted_count; --index) {
      const auto meta = GetMetadataProtected(index);
      if (!meta.IsInProgress()) {
        if (count == 0) {
          // insert a first record
          arr[0] = MetaRecord{meta, GetKey(meta)};
          ++count;
          continue;
        }

        // insert a new record into an appropiate position
        MetaRecord target{meta, GetKey(meta)};
        const auto ins_iter = std::lower_bound(arr.begin(), arr.begin() + count, target);
        if (*ins_iter != target) {
          const size_t ins_id = std::distance(arr.begin(), ins_iter);
          if (ins_id < count) {
            // shift upper records
            memmove(&(arr[ins_id + 1]), &(arr[ins_id]), sizeof(MetaRecord) * (count - ins_id));
          }

          // insert a new record
          arr[ins_id] = std::move(target);
          ++count;
        }
      }
    }

    return {count, arr};
  }

  /*################################################################################################
   * Internal variables
   *##############################################################################################*/

  /// the byte length of a node page.
  uint64_t node_size_ : 32;

  /// the number of sorted records.
  uint64_t sorted_count_ : 16;

  /// a flag to indicate whether this node is a leaf or internal node.
  uint64_t is_leaf_ : 1;

  /// a flag to indicate whether this node is a right end node.
  uint64_t is_right_end_ : 1;

  /// a black block for alignment.
  uint64_t : 0;

  /// a status word.
  StatusWord status_;

  /// the head of a metadata array.
  Metadata meta_array_[(kPageSize - kHeaderLength) / sizeof(Metadata)];

 public:
  /*################################################################################################
   * Public constants
   *##############################################################################################*/

  /// the maximum number of records in a node
  static constexpr size_t kMaxRecordNum = GetMaxRecordNum<Key, Payload>();

  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new node object.
   *
   * @param is_leaf a flag to indicate whether a leaf node is constructed.
   */
  explicit Node(const bool is_leaf)
      : node_size_{kPageSize}, sorted_count_{0}, is_leaf_{is_leaf}, is_right_end_{false}, status_{}
  {
  }

  /**
   * @brief Destroy the node object.
   *
   */
  ~Node()
  {
    for (size_t i = 0; i < sizeof(meta_array_) / sizeof(Metadata); ++i) {
      meta_array_[i] = Metadata{};
    }
  }

  Node(const Node &) = delete;
  Node &operator=(const Node &) = delete;
  Node(Node &&) = delete;
  Node &operator=(Node &&) = delete;

  /*################################################################################################
   * new/delete definitions
   *##############################################################################################*/

  static void *
  operator new(std::size_t)
  {
    return calloc(1UL, kPageSize);
  }

  static void *
  operator new(std::size_t, void *where)
  {
    return where;
  }

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
   * @retval true if this node has a next sibling node.
   * @retval false otherwise.
   */
  constexpr bool
  HasNext() const
  {
    return !static_cast<bool>(is_right_end_);
  }

  /**
   * @return size_t: the number of sorted records.
   */
  constexpr size_t
  GetSortedCount() const
  {
    return sorted_count_;
  }

  auto
  GetUsedSize() const  //
      -> size_t
  {
    return GetStatusWordProtected().GetOccupiedSize();
  }

  auto
  GetFreeSpaceSize() const  //
      -> size_t
  {
    const auto status = GetStatusWordProtected();
    return kPageSize - status.GetOccupiedSize();
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
    return MwCASDescriptor::Read<StatusWord>(&status_);
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
    return MwCASDescriptor::Read<Metadata>(&meta_array_[position]);
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
      out_payload = MwCASDescriptor::Read<Payload>(this->GetPayloadAddr(meta));
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
   * @brief Set a right-end flag to this node.
   *
   */
  void
  SetRightEndFlag(const bool is_right_end = true)
  {
    is_right_end_ = is_right_end;
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

  template <class T>
  auto
  SetRecord(  //
      const Key &key,
      const size_t key_length,
      const T &payload,
      const size_t payload_length,
      size_t offset)  //
      -> size_t
  {
    SetPayload(offset, payload, payload_length);
    SetKey(offset, key, key_length);

    if constexpr (CanCASUpdate<T>()) {
      AlignOffset<Key>(offset);
    }

    return offset;
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
   * @brief Get the position of a specified key by using binary search. If there is no
   * specified key, this returns the minimum metadata index that is greater than the
   * specified key
   *
   * @param key a target key.
   * @param range_is_closed a flag to indicate that a target key is included.
   * @return size_t: the position of a specified key.
   */
  auto
  SearchChild(  //
      const Key &key,
      const bool range_is_closed) const  //
      -> size_t
  {
    int64_t begin_pos = 0;
    int64_t end_pos = GetSortedCount() - 2;
    while (begin_pos <= end_pos) {
      size_t pos = (begin_pos + end_pos) >> 1;

      const auto meta = GetMetadata(pos);
      const auto index_key = GetKey(meta);

      if (Compare{}(key, index_key)) {  // a target key is in a left side
        end_pos = pos - 1;
      } else if (Compare{}(index_key, key)) {  // a target key is in a right side
        begin_pos = pos + 1;
      } else if (range_is_closed) {  // find an equivalent key
        begin_pos = pos;
        break;
      } else {  // find an equivalent key, but the range is open
        begin_pos = pos + 1;
        break;
      }
    }

    return begin_pos;
  }

  /**
   * @brief Get the position of a specified key by using binary search. If there is no
   * specified key, this returns the minimum metadata index that is greater than the
   * specified key
   *
   * @param node a target node.
   * @param key a target key.
   * @return std::pair<KeyExistence, int64_t>: a pair of key existence and a key position.
   */
  auto
  SearchSortedRecord(const Key &key) const  //
      -> std::pair<KeyExistence, size_t>
  {
    int64_t begin_pos = 0;
    int64_t end_pos = GetSortedCount() - 1;
    while (begin_pos <= end_pos) {
      size_t pos = (begin_pos + end_pos) >> 1;

      const auto meta = GetMetadataProtected(pos);
      const auto index_key = GetKey(meta);

      if (Compare{}(key, index_key)) {  // a target key is in a left side
        end_pos = pos - 1;
      } else if (Compare{}(index_key, key)) {  // a target key is in a right side
        begin_pos = pos + 1;
      } else if (meta.IsVisible()) {  // find an equivalent key
        return {kExist, pos};
      } else {  // find an equivalent key, but it is deleted
        return {kDeleted, pos};
      }
    }

    return {kNotExist, begin_pos};
  }

  auto
  SearchUnsortedRecord(  //
      const Key &key,
      const size_t begin_pos) const  //
      -> std::pair<KeyExistence, size_t>
  {
    const int64_t sorted_count = GetSortedCount();

    // perform a linear search in revese order
    for (int64_t pos = begin_pos; pos >= sorted_count; --pos) {
      const auto meta = GetMetadataProtected(pos);
      if (meta.IsInProgress()) return {kUncertain, pos};

      const auto target_key = GetKey(meta);
      if (IsEqual<Compare>(key, target_key)) {
        if (meta.IsVisible()) return {kExist, pos};
        return {kDeleted, pos};
      }
    }

    return {kNotExist, 0};
  }

  /**
   * @brief Check uniqueness of a target key in a specifeid node.
   *
   * @param key a target key.
   * @param rec_count the total number of records in a node.
   * @return std::pair<KeyExistence, int64_t>: a pair of key existence and a key position.
   */
  auto
  CheckUniqueness(  //
      const Key &key,
      const int64_t rec_count) const  //
      -> std::pair<KeyExistence, size_t>
  {
    KeyExistence rc;
    size_t pos;

    if constexpr (CanCASUpdate<Payload>()) {
      std::tie(rc, pos) = SearchSortedRecord(key);
      if (rc == kNotExist || rc == kDeleted) {
        // a new record may be inserted in an unsorted region
        std::tie(rc, pos) = SearchUnsortedRecord(key, rec_count - 1);
      }
    } else {
      std::tie(rc, pos) = SearchUnsortedRecord(key, rec_count - 1);
      if (rc == kNotExist) {
        // a record may be in a sorted region
        std::tie(rc, pos) = SearchSortedRecord(key);
      }
    }

    return {rc, pos};
  }

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

  auto
  InsertChild(  //
      const Node *orig_node,
      const Metadata orig_meta,
      const Node *child_node,
      const size_t rec_count,
      size_t offset)  //
      -> size_t
  {
    const auto key = orig_node->GetKey(orig_meta);
    const auto key_len = orig_meta.GetKeyLength();
    offset = SetRecord(key, key_len, child_node, kWordLength, offset);
    SetMetadata(rec_count, Metadata{offset, key_len, key_len + kWordLength});

    return offset;
  }

  template <class T>
  auto
  CopyRecordFrom(  //
      const Node *orig_node,
      const Metadata target_meta,
      const size_t rec_count,
      size_t offset)  //
      -> size_t
  {
    // copy a record from the given node
    const auto total_length = target_meta.GetTotalLength();
    offset -= total_length;
    memcpy(ShiftAddress(this, offset), orig_node->GetKeyAddr(target_meta), total_length);

    // set new metadata
    SetMetadata(rec_count, target_meta.UpdateOffset(offset));

    if constexpr (CanCASUpdate<T>()) {
      AlignOffset<Key>(offset);
    }

    return offset;
  }

  template <class T>
  auto
  CopyRecordsFrom(  //
      const Node *orig_node,
      const size_t begin_pos,
      const size_t end_pos,
      size_t rec_count,
      size_t offset)  //
      -> size_t
  {
    // copy records from the given node
    for (size_t i = begin_pos; i < end_pos; ++i) {
      const auto target_meta = orig_node->GetMetadata(i);
      offset = CopyRecordFrom(orig_node, target_meta, rec_count++, offset);
    }

    return offset;
  }

  /*################################################################################################
   * Leaf read operations
   *##############################################################################################*/

  /**
   * @brief Read a payload of a specified key if it exists.
   *
   * @param key a target key.
   * @param out_payload a reference to be stored a target payload.
   * @retval kSuccess if a key exists.
   * @retval kKeyNotExist if a key does not exist.
   */
  auto
  Read(  //
      const Key &key,
      Payload &out_payload) const  //
      -> NodeReturnCode
  {
    const auto status = GetStatusWordProtected();
    const auto [rc, pos] = CheckUniqueness(key, status.GetRecordCount());
    if (rc == kNotExist || rc == kDeleted) {
      return NodeReturnCode::kKeyNotExist;
    }

    const auto meta = GetMetadataProtected(pos);
    CopyPayload(meta, out_payload);
    return NodeReturnCode::kSuccess;
  }

  // /**
  //  * @brief Perform a range scan with specified keys.
  //  *
  //  * If a begin/end key is nullptr, it is treated as negative or positive infinite.
  //  *
  //  * @param begin_k the pointer of a begin key of a range scan.
  //  * @param begin_closed a flag to indicate whether the begin side of a range is closed.
  //  * @param end_k the pointer of an end key of a range scan.
  //  * @param end_closed a flag to indicate whether the end side of a range is closed.
  //  * @param page a page to copy target keys/payloads. This argument is used internally.
  //  * @retval true if scanning finishes.
  //  * @retval false if scanning is in progress.
  //  */
  // template <class Key, class Payload, class Compare>
  // bool
  // Scan(  //
  //     const Key *begin_k,
  //     const bool begin_closed,
  //     const Key *end_k,
  //     const bool end_closed,
  //     RecordPage<Key, Payload> *page)
  // {
  //   // sort records in an unsorted region
  //   NewSortedMeta<Key, Payload, Compare> new_records;
  //   size_t new_rec_num = 0;
  //   _SortUnsortedRecords(node, begin_k, begin_closed, end_k, end_closed, new_records,
  //   new_rec_num);

  //   // sort all records by merge sort
  //   MetaArray<Key, Payload, Compare> metadata;
  //   size_t count = 0;
  //   const auto scan_finished = _MergeSortedRecords(node, new_records, new_rec_num, begin_k,
  //                                                  begin_closed, end_k, end_closed, metadata,
  //                                                  count);

  //   // copy scan results to a page for returning
  //   std::byte *cur_addr = reinterpret_cast<std::byte *>(page) + component::kHeaderLength;
  //   for (size_t i = 0; i < count; ++i) {
  //     const Metadata meta = metadata[i];
  //     if constexpr (IsVariableLengthData<Key>()) {
  //       *(reinterpret_cast<uint32_t *>(cur_addr)) = meta.GetKeyLength();
  //       cur_addr += sizeof(uint32_t);
  //     }
  //     if constexpr (IsVariableLengthData<Payload>()) {
  //       *(reinterpret_cast<uint32_t *>(cur_addr)) = meta.GetPayloadLength();
  //       cur_addr += sizeof(uint32_t);
  //     }
  //     if constexpr (CanCASUpdate<Payload>()) {
  //       if constexpr (IsVariableLengthData<Key>()) {
  //         memcpy(cur_addr, GetKeyAddr(meta), meta.GetKeyLength());
  //         cur_addr += meta.GetKeyLength();
  //       } else {
  //         memcpy(cur_addr, GetKeyAddr(meta), sizeof(Key));
  //         cur_addr += sizeof(Key);
  //       }
  //       *(reinterpret_cast<Payload *>(cur_addr)) =
  //           MwCASDescriptor::Read<Payload>(GetPayloadAddr(meta));
  //       cur_addr += kWordLength;
  //     } else {
  //       memcpy(cur_addr, GetKeyAddr(meta), meta.GetTotalLength());
  //       cur_addr += meta.GetTotalLength();
  //     }
  //   }
  //   page->SetEndAddress(cur_addr);
  //   if (count > 0) {
  //     page->SetLastKeyAddress(cur_addr - metadata[count - 1].GetTotalLength());
  //   }

  //   return scan_finished;
  // }

  /*################################################################################################
   * Write operations
   *##############################################################################################*/

  /**
   * @brief Write (i.e., upsert) a specified kay/payload pair.
   *
   * If a specified key does not exist in a leaf node, this function performs an insert
   * operation. If a specified key has been already inserted, this function perfroms an
   * update operation.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_length the length of a target key.
   * @param payload_length the length of a target payload.
   * @retval kSuccess if a key/payload pair is written.
   * @retval kFrozen if a target node is frozen.
   * @retval kNeedConsolidation if a target node requires consolidation.
   */
  auto
  Write(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload,
      const size_t payload_length)  //
      -> NodeReturnCode
  {
    // variables and constants shared in Phase 1 & 2
    const auto total_length = key_length + payload_length;
    const auto block_size = GetAlignedSize(total_length);
    const auto in_progress_meta = Metadata{epoch, key_length, total_length, true};
    StatusWord cur_status;
    size_t rec_count;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to write a record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = GetStatusWordProtected();
      if (cur_status.IsFrozen()) return NodeReturnCode::kFrozen;
      if (!HasSpace(cur_status, block_size)) return NodeReturnCode::kNeedConsolidation;

      rec_count = cur_status.GetRecordCount();
      if constexpr (CanCASUpdate<Payload>()) {
        // check whether a node includes a target key
        const auto [rc, target_pos] = SearchSortedRecord(key);
        if (rc == kExist) {
          const auto target_meta = GetMetadataProtected(target_pos);

          // update a record directly
          MwCASDescriptor desc{};
          SetStatusForMwCAS(desc, cur_status, cur_status);
          SetMetadataForMwCAS(desc, target_pos, target_meta, target_meta);
          SetPayloadForMwCAS(desc, target_meta, payload);
          if (desc.MwCAS()) return NodeReturnCode::kSuccess;
          continue;
        }
      }

      // prepare new status for MwCAS
      const auto new_status = cur_status.Add(1, block_size);

      // perform MwCAS to reserve space
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, cur_status, new_status);
      SetMetadataForMwCAS(desc, rec_count, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: write a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - cur_status.GetBlockSize();
    SetPayload(offset, payload, payload_length);
    SetKey(offset, key, key_length);

    // prepare record metadata for MwCAS
    const auto inserted_meta = in_progress_meta.MakeVisible(offset);

    // check conflicts (concurrent SMOs)
    while (true) {
      const auto status = GetStatusWordProtected();
      if (status.IsFrozen()) {
        return NodeReturnCode::kFrozen;
      }

      // perform MwCAS to complete a write
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, status, status);
      SetMetadataForMwCAS(desc, rec_count, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
    }

    return NodeReturnCode::kSuccess;
  }

  /**
   * @brief Insert a specified kay/payload pair.
   *
   * This function performs a uniqueness check in its processing. If a specified key does
   * not exist, this function insert a target payload into a target leaf node. If a
   * specified key exists in a target leaf node, this function does nothing and returns
   * kKeyExist as a return code.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_length the length of a target key.
   * @param payload_length the length of a target payload.
   * @retval kSuccess if a key/payload pair is written.
   * @retval kKeyExist if a specified key exists.
   * @retval kFrozen if a target node is frozen.
   * @retval kNeedConsolidation if a target node requires consolidation.
   */
  auto
  Insert(  //
      Node<Key, Payload, Compare> *node,
      const Key &key,
      const size_t key_length,
      const Payload &payload,
      const size_t payload_length)  //
      -> NodeReturnCode
  {
    // variables and constants shared in Phase 1 & 2
    const auto total_length = key_length + payload_length;
    const auto block_size = GetAlignedSize(total_length);
    const auto in_progress_meta = Metadata{epoch, key_length, total_length, true};
    StatusWord cur_status;
    size_t rec_count;
    KeyExistence rc;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = GetStatusWordProtected();
      if (cur_status.IsFrozen()) return NodeReturnCode::kFrozen;
      if (!HasSpace(cur_status, block_size)) return NodeReturnCode::kNeedConsolidation;

      // check uniqueness
      rec_count = cur_status.GetRecordCount();
      rc = CheckUniqueness(key, rec_count).first;
      if (rc == kExist) return NodeReturnCode::kKeyExist;

      // prepare new status for MwCAS
      const auto new_status = cur_status.Add(1, block_size);

      // perform MwCAS to reserve space
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, cur_status, new_status);
      SetMetadataForMwCAS(desc, rec_count, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - cur_status.GetBlockSize();
    SetPayload(offset, payload, payload_length);
    SetKey(offset, key, key_length);

    // prepare record metadata for MwCAS
    const auto inserted_meta = in_progress_meta.MakeVisible(offset);

    while (true) {
      // check concurrent SMOs
      const auto status = GetStatusWordProtected();
      if (status.IsFrozen()) {
        // delete an inserted record
        SetMetadataByCAS(rec_count, in_progress_meta.UpdateOffset(0));
        return NodeReturnCode::kFrozen;
      }

      // recheck uniqueness if required
      if (rc == kUncertain) {
        rc = CheckUniqueness(key, rec_count).first;
        if (rc == kExist) {
          // delete an inserted record
          SetMetadataByCAS(rec_count, in_progress_meta.UpdateOffset(0));
          return NodeReturnCode::kKeyExist;
        } else if (rc == kUncertain) {
          // retry if there are still uncertain records
          continue;
        }
      }

      // perform MwCAS to complete an insert
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, status, status);
      SetMetadataForMwCAS(desc, rec_count, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
    }

    return NodeReturnCode::kSuccess;
  }

  /**
   * @brief Update a target kay with a specified payload.
   *
   * This function performs a uniqueness check in its processing. If a specified key
   * exist, this function update a target payload. If a specified key does not exist in
   * a target leaf node, this function does nothing and returns kKeyNotExist as a return
   * code.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_length the length of a target key.
   * @param payload_length the length of a target payload.
   * @retval kSuccess if a key/payload pair is written.
   * @retval kKeyNotExist if a specified key does not exist.
   * @retval kFrozen if a target node is frozen.
   * @retval kNeedConsolidation if a target node requires consolidation.
   */
  auto
  Update(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload,
      const size_t payload_length)  //
      -> NodeReturnCode
  {
    // variables and constants shared in Phase 1 & 2
    const auto total_length = key_length + payload_length;
    const auto block_size = GetAlignedSize(total_length);
    const auto in_progress_meta = Metadata{epoch, key_length, total_length, true};
    StatusWord cur_status;
    size_t rec_count, target_index = 0;
    KeyExistence rc;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = GetStatusWordProtected();
      if (cur_status.IsFrozen()) return NodeReturnCode::kFrozen;
      if (!HasSpace(cur_status, block_size)) return NodeReturnCode::kNeedConsolidation;

      // check whether a node includes a target key
      rec_count = cur_status.GetRecordCount();
      std::tie(rc, target_index) = CheckUniqueness(key, rec_count);
      if (rc == kNotExist || rc == kDeleted) return NodeReturnCode::kKeyNotExist;

      if constexpr (CanCASUpdate<Payload>()) {
        if (rc == kExist && target_index < GetSortedCount()) {
          const auto target_meta = GetMetadataProtected(target_index);

          // update a record directly
          MwCASDescriptor desc{};
          SetStatusForMwCAS(desc, cur_status, cur_status);
          SetMetadataForMwCAS(desc, target_index, target_meta, target_meta);
          SetPayloadForMwCAS(desc, target_meta, payload);
          if (desc.MwCAS()) return NodeReturnCode::kSuccess;
          continue;
        }
      }

      // prepare new status for MwCAS
      const auto target_meta = GetMetadataProtected(target_index);
      const auto deleted_size = kWordLength + GetAlignedSize(target_meta.GetTotalLength());
      const auto new_status = cur_status.Add(1, block_size).Delete(deleted_size);

      // perform MwCAS to reserve space
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, cur_status, new_status);
      SetMetadataForMwCAS(desc, rec_count, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - cur_status.GetBlockSize();
    SetPayload(offset, payload, payload_length);
    SetKey(offset, key, key_length);

    // prepare record metadata for MwCAS
    const auto inserted_meta = in_progress_meta.MakeVisible(offset);

    while (true) {
      // check conflicts (concurrent SMOs)
      const auto status = GetStatusWordProtected();
      if (status.IsFrozen()) return NodeReturnCode::kFrozen;

      // recheck uniqueness if required
      if (rc == kUncertain) {
        rc = CheckUniqueness(key, rec_count).first;
        if (rc == kNotExist || rc == kDeleted) {
          // delete an inserted record
          SetMetadataByCAS(rec_count, in_progress_meta.UpdateOffset(0));
          return NodeReturnCode::kKeyNotExist;
        } else if (rc == kUncertain) {
          continue;
        }
      }

      // perform MwCAS to complete an update
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, status, status);
      SetMetadataForMwCAS(desc, rec_count, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
    }

    return NodeReturnCode::kSuccess;
  }

  /**
   * @brief Delete a target kay from the index.
   *
   * This function performs a uniqueness check in its processing. If a specified key
   * exist, this function deletes it. If a specified key does not exist in a leaf node,
   * this function does nothing and returns kKeyNotExist as a return code.
   *
   * Note that if a target key is binary data, it is required to specify its length in
   * bytes.
   *
   * @param key a target key to be written.
   * @param key_length the length of a target key.
   * @retval kSuccess if a key/payload pair is written.
   * @retval kKeyNotExist if a specified key does not exist.
   * @retval kFrozen if a target node is frozen.
   * @retval kNeedConsolidation if a target node requires consolidation.
   */
  auto
  Delete(  //
      const Key &key,
      const size_t key_length)  //
      -> NodeReturnCode
  {
    // variables and constants
    const auto in_progress_meta = Metadata{epoch, key_length, key_length, true};
    StatusWord cur_status;
    size_t rec_count, target_index = 0;
    KeyExistence rc;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a null record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = GetStatusWordProtected();
      if (cur_status.IsFrozen()) return NodeReturnCode::kFrozen;
      if (!HasSpace(cur_status, key_length)) return NodeReturnCode::kNeedConsolidation;

      // check whether a node includes a target key
      rec_count = cur_status.GetRecordCount();
      std::tie(rc, target_index) = CheckUniqueness(key, rec_count);
      if (rc == kNotExist || rc == kDeleted) return NodeReturnCode::kKeyNotExist;

      if constexpr (CanCASUpdate<Payload>()) {
        if (rc == kExist && target_index < GetSortedCount()) {
          const auto target_meta = GetMetadataProtected(target_index);
          const auto deleted_meta = target_meta.Delete();
          const auto deleted_size = kWordLength + GetAlignedSize(target_meta.GetTotalLength());
          const auto new_status = cur_status.Delete(deleted_size);

          // delete a record directly
          MwCASDescriptor desc{};
          SetStatusForMwCAS(desc, cur_status, new_status);
          SetMetadataForMwCAS(desc, target_index, target_meta, deleted_meta);
          if (desc.MwCAS()) return NodeReturnCode::kSuccess;
          continue;
        }
      }

      // prepare new status for MwCAS
      const auto target_meta = GetMetadataProtected(target_index);
      const auto deleted_size =
          (2 * kWordLength) + GetAlignedSize(target_meta.GetTotalLength()) + key_length;
      const auto new_status = cur_status.Add(1, key_length).Delete(deleted_size);

      // perform MwCAS to reserve space
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, cur_status, new_status);
      SetMetadataForMwCAS(desc, rec_count, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a null record
    auto offset = kPageSize - cur_status.GetBlockSize();
    SetKey(offset, key, key_length);

    // prepare record metadata for MwCAS
    const auto deleted_meta = in_progress_meta.MakeInvisible(offset);

    while (true) {
      // check concurrent SMOs
      const auto status = GetStatusWordProtected();
      if (status.IsFrozen()) {
        // delete an inserted record
        SetMetadataByCAS(rec_count, in_progress_meta.UpdateOffset(0));
        return NodeReturnCode::kFrozen;
      }

      // recheck uniqueness if required
      if (rc == kUncertain) {
        rc = CheckUniqueness(key, rec_count).first;
        if (rc == kNotExist || rc == kDeleted) {
          // delete an inserted record
          SetMetadataByCAS(rec_count, in_progress_meta.UpdateOffset(0));
          return NodeReturnCode::kKeyNotExist;
        } else if (rc == kUncertain) {
          // retry if there are still uncertain records
          continue;
        }
      }

      // perform MwCAS to complete an insert
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, status, status);
      SetMetadataForMwCAS(desc, rec_count, in_progress_meta, deleted_meta);
      if (desc.MwCAS()) break;
    }

    return NodeReturnCode::kSuccess;
  }

  /*################################################################################################
   * Public structure modification operations
   *##############################################################################################*/

  /**
   * @brief Consolidate a target leaf node.
   *
   * @param new_node a consolidated node.
   * @param old_node an original node.
   */
  static void
  Consolidate(  //
      const Node *old_node,
      Node *new_node)
  {
    if (!old_node->HasNext()) {
      new_node->SetRightEndFlag();
    }

    // sort records in an unsorted region
    const auto [new_rec_num, records] = SortNewRecords(old_node);

    // perform merge-sort to consolidate a node
    const auto sorted_count = old_node->GetSortedCount();
    size_t offset = kPageSize;
    size_t rec_count = 0;

    size_t j = 0;
    for (size_t i = 0; i < sorted_count; ++i) {
      const auto meta = GetMetadataProtected(i);
      const auto key = GetKey(meta);

      // copy new records
      for (; j < new_rec_num; ++j) {
        auto [target_meta, target_key] = records[j];
        if (Compare{}(key, target_key)) break;

        // check a new record is active
        if (target_meta.IsVisible()) {
          offset = new_node->CopyRecordFrom<Payload>(old_node, target_meta, rec_count++, offset);
        }
      }

      // check a new record is updated one
      if (j < new_rec_num && IsEqual(key, records[j].key)) {
        const auto target_meta = records[j++].meta;
        if (target_meta.IsVisible()) {
          offset = new_node->CopyRecordFrom<Payload>(old_node, target_meta, rec_count++, offset);
        }
      } else if (meta.IsVisible()) {
        offset = new_node->CopyRecordFrom<Payload>(old_node, meta, rec_count++, offset);
      }
    }

    // move remaining new records
    for (; j < new_rec_num; ++j) {
      const auto target_meta = records[j++].meta;
      if (target_meta.IsVisible()) {
        offset = new_node->CopyRecordFrom<Payload>(old_node, target_meta, rec_count++, offset);
      }
    }

    // set header information
    new_node->SetSortedCount(rec_count);
    new_node->SetStatus(StatusWord{rec_count, kPageSize - offset});
  }

  template <class T>
  static void
  Split(  //
      Node *node,
      Node *l_node,
      Node *r_node)
  {
    // set a right-end flag if needed
    if (!node->HasNext()) {
      l_node->SetRightEndFlag(false);
      r_node->SetRightEndFlag(true);
    }

    // copy records to a left node
    const auto rec_count = node->GetSortedCount();
    const size_t l_count = rec_count / 2;
    size_t l_offset;
    if constexpr (std::is_same_v<T, Node *>) {
      l_offset = l_node->CopyRecordsFrom<T>(node, 0, l_count, 0, kPageSize);
    } else {  // when splitting a leaf node, only update its offset
      l_offset = l_node->GetMetadata(l_count - 1).GetOffset();
    }
    l_node->SetSortedCount(l_count);
    l_node->SetStatus(StatusWord{l_count, kPageSize - l_offset});

    // copy records to a right node
    const auto r_offset = r_node->CopyRecordsFrom<T>(node, l_count, rec_count, 0, kPageSize);
    const auto r_count = rec_count - l_count;
    r_node->SetSortedCount(r_count);
    r_node->SetStatus(StatusWord{r_count, kPageSize - r_offset});
  }

  static void
  CreateSplitParent(  //
      const Node *old_node,
      Node *new_node,
      const Node *l_child,
      const Node *r_child,
      const size_t l_pos)
  {
    // set a right-end flag if needed
    if (!old_node->HasNext()) {
      new_node->SetRightEndFlag(true);
    }

    // copy lower records
    auto offset = new_node->CopyRecordsFrom<Node *>(old_node, 0, l_pos, 0, kPageSize);

    // insert split nodes
    const auto l_meta = l_child->GetMetadata(l_child->GetSortedCount() - 1);
    offset = new_node->InsertChild(l_child, l_meta, l_child, l_pos, offset);
    const auto r_meta = old_node->GetMetadata(l_pos);
    const auto r_pos = l_pos + 1;
    offset = new_node->InsertChild(old_node, r_meta, r_child, r_pos, offset);

    // copy upper records
    auto rec_count = old_node->GetSortedCount();
    offset = new_node->CopyRecordsFrom<Node *>(old_node, r_pos, rec_count, r_pos + 1, offset);

    // set an updated header
    new_node->SetSortedCount(++rec_count);
    new_node->SetStatus(StatusWord{rec_count, kPageSize - offset});
  }

  template <class T>
  static void
  Merge(  //
      Node *l_node,
      Node *r_node,
      Node *merged_node)
  {
    // set a right-end flag if needed
    if (!r_node->HasNext()) {
      merged_node->SetRightEndFlag();
    }

    // copy records in left/right nodes
    const auto l_count = l_node->GetSortedCount();
    size_t offset;
    if constexpr (std::is_same_v<T, Node *>) {
      offset = merged_node->CopyRecordsFrom<T>(l_node, 0, l_count, 0, kPageSize);
    } else {  // when merging a leaf node, only update its offset
      offset = merged_node->GetMetadata(l_count - 1).GetOffset();
    }
    const auto r_count = r_node->GetSortedCount();
    offset = merged_node->CopyRecordsFrom<T>(r_node, 0, r_count, l_count, offset);

    // create a merged node
    const auto rec_count = l_count + r_count;
    merged_node->SetSortedCount(rec_count);
    merged_node->SetStatus(StatusWord{rec_count, kPageSize - offset});
  }

  static void
  CreateMergeParent(  //
      const Node *old_node,
      Node *new_node,
      const Node *merged_child,
      const size_t position)
  {
    // set a right-end flag if needed
    if (!old_node->HasNext()) {
      new_node->SetRightEndFlag(true);
    }

    // copy lower records
    auto offset = new_node->CopyRecordsFrom<Node *>(old_node, 0, position, 0, kPageSize);

    // insert a merged node
    const auto r_pos = position + 1;
    const auto meta = old_node->GetMetadata(r_pos);
    offset = new_node->InsertChild(old_node, meta, merged_child, position, offset);

    // copy upper records
    auto rec_count = old_node->GetSortedCount();
    offset = new_node->CopyRecordsFrom<Node *>(old_node, r_pos + 1, rec_count, r_pos, offset);

    // set an updated header
    new_node->SetSortedCount(--rec_count);
    new_node->SetStatus(StatusWord{rec_count, kPageSize - offset});
  }
};

}  // namespace dbgroup::index::bztree::component
