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

#ifndef BZTREE_COMPONENT_NODE_HPP
#define BZTREE_COMPONENT_NODE_HPP

#include <atomic>
#include <cstdlib>
#include <functional>
#include <memory>
#include <tuple>
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
template <class Key, class Compare>
class Node
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new node object.
   *
   * @param is_leaf a flag to indicate whether a leaf node is constructed.
   * @param block_size an initial block size to align records.
   */
  Node(  //
      const bool is_leaf,
      const size_t block_size)
      : node_size_{kHeaderLength},
        sorted_count_{0},
        is_leaf_{static_cast<uint64_t>(is_leaf)},
        do_splitting_{0},
        status_{0, block_size}
  {
  }

  Node(const Node &) = delete;
  Node &operator=(const Node &) = delete;
  Node(Node &&) = delete;
  Node &operator=(Node &&) = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the node object.
   *
   */
  ~Node()
  {
    // fill a metadata region with zeros
    for (size_t i = 0; i < GetMaxRecordNum(); ++i) {
      auto *dummy_p = reinterpret_cast<size_t *>(&meta_array_[i]);
      *dummy_p = 0UL;
    }
  }

  /*####################################################################################
   * new/delete operators
   *##################################################################################*/

  static auto
  operator new([[maybe_unused]] std::size_t n)  //
      -> void *
  {
    return calloc(1UL, kPageSize);
  }

  static auto
  operator new([[maybe_unused]] std::size_t n, void *where)  //
      -> void *
  {
    return where;
  }

  static void
  operator delete(void *p) noexcept
  {
    free(p);
  }

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @retval true if this is a leaf node.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  IsLeaf() const  //
      -> bool
  {
    return is_leaf_;
  }

  /**
   * @return the number of sorted records.
   */
  [[nodiscard]] constexpr auto
  GetSortedCount() const  //
      -> size_t
  {
    return sorted_count_;
  }

  /**
   * @brief Read a status word without MwCAS read protection.
   *
   * This function assumes that there are no other threads modify this node
   * concurrently.
   *
   * @return a status word.
   */
  [[nodiscard]] constexpr auto
  GetStatusWord() const  //
      -> StatusWord
  {
    return status_;
  }

  /**
   * @brief Read a status word with MwCAS read protection.
   *
   * This function uses a MwCAS read operation internally, and so it is guaranteed that
   * a read status word is valid in multi-threading.
   *
   * @return a status word.
   */
  [[nodiscard]] auto
  GetStatusWordProtected() const  //
      -> StatusWord
  {
    return MwCASDescriptor::Read<StatusWord>(&status_);
  }

  /**
   * @return the metadata fo a highest key.
   */
  [[nodiscard]] constexpr auto
  GetHighMeta() const  //
      -> Metadata
  {
    return high_meta_;
  }

  /**
   * @brief Read metadata without MwCAS read protection.
   *
   * @return metadata.
   */
  [[nodiscard]] constexpr auto
  GetMetadata(const size_t position) const  //
      -> Metadata
  {
    return meta_array_[position];
  }

  /**
   * @brief Copy and return a highest key for scanning.
   *
   * NOTE: this function does not check the existence of a highest key.
   * NOTE: this function allocates memory dynamically for variable-length keys, so it
   * must be released by the caller.
   *
   * @return the highest key in this node.
   */
  [[nodiscard]] auto
  GetHighKey() const  //
      -> Key
  {
    Key high_key{};
    if constexpr (IsVariableLengthData<Key>()) {
      const auto key_len = high_meta_.GetKeyLength();
      high_key = reinterpret_cast<Key>(::operator new(key_len));
      memcpy(high_key, GetKeyAddr(high_meta_), key_len);
    } else {
      memcpy(&high_key, GetKeyAddr(high_meta_), sizeof(Key));
    }

    return high_key;
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return a key in a target record.
   */
  [[nodiscard]] auto
  GetKey(const Metadata meta) const  //
      -> Key
  {
    if constexpr (IsVariableLengthData<Key>()) {
      return reinterpret_cast<Key>(GetKeyAddr(meta));
    } else {
      Key key{};
      memcpy(&key, GetKeyAddr(meta), sizeof(Key));
      return key;
    }
  }

  /**
   * @tparam Payload a class of payload.
   * @param meta metadata of a corresponding record.
   * @return a payload in a target record.
   */
  template <class Payload>
  [[nodiscard]] auto
  GetPayload(const Metadata meta) const  //
      -> Payload
  {
    Payload payload{};
    memcpy(&payload, GetPayloadAddr(meta), sizeof(Payload));
    return payload;
  }

  /**
   * @brief Get a child node with MwCAS's read protection.
   *
   * @param position the position of a child node.
   * @return a child node.
   */
  [[nodiscard]] auto
  GetChild(const size_t position) const  //
      -> Node *
  {
    auto *child = MwCASDescriptor::Read<Node *>(GetPayloadAddr(meta_array_[position]));
    std::atomic_thread_fence(std::memory_order_acquire);

    return child;
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
   * @brief Set old/new children nodes as a MwCAS target.
   *
   * @param desc a target MwCAS descriptor.
   * @param pos the position of a target child node.
   * @param old_child an expected child node.
   * @param new_child a desired child node.
   */
  constexpr void
  SetChildForMwCAS(  //
      MwCASDescriptor &desc,
      const size_t pos,
      const Node *old_child,
      const Node *new_child)
  {
    desc.AddMwCASTarget(GetPayloadAddr(meta_array_[pos]), old_child, new_child);
  }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  /**
   * @brief Get the position of a specified key by using binary search. If there is no
   * specified key, this returns the minimum metadata index that is greater than the
   * specified key
   *
   * @param key a target key.
   * @param range_is_closed a flag to indicate that a target key is included.
   * @return the position of a specified key.
   */
  [[nodiscard]] auto
  Search(  //
      const Key &key,
      const bool range_is_closed) const  //
      -> size_t
  {
    int64_t begin_pos = 0;
    int64_t end_pos = sorted_count_ - 2;
    while (begin_pos <= end_pos) {
      size_t pos = (begin_pos + end_pos) >> 1UL;  // NOLINT

      const auto &index_key = GetKey(meta_array_[pos]);

      if (Compare{}(key, index_key)) {  // a target key is in a left side
        end_pos = pos - 1;
      } else if (Compare{}(index_key, key)) {  // a target key is in a right side
        begin_pos = pos + 1;
      } else {  // find an equivalent key
        if (!range_is_closed) ++pos;
        begin_pos = pos;
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
   * @param key a target key.
   * @return a pair of key existence and a key position.
   */
  [[nodiscard]] auto
  SearchSortedRecord(const Key &key) const  //
      -> std::pair<KeyExistence, size_t>
  {
    int64_t begin_pos = 0;
    int64_t end_pos = sorted_count_ - 1;
    while (begin_pos <= end_pos) {
      size_t pos = (begin_pos + end_pos) >> 1UL;  // NOLINT

      const auto meta = GetMetadataProtected(pos);
      const auto &index_key = GetKey(meta);

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

  /**
   * @brief Freeze this node for SMOs.
   *
   * @retval kSuccess if a node has become frozen.
   * @retval kFrozen if a node has been already frozen.
   */
  auto
  Freeze()  //
      -> NodeRC
  {
    while (true) {
      const auto current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) return kFrozen;

      MwCASDescriptor desc;
      SetStatusForMwCAS(desc, current_status, current_status.Freeze());
      if (desc.MwCAS()) break;
      SPINLOCK_HINT
    }

    return kSuccess;
  }

  /**
   * @brief Unfreeze this node for accepting other modification.
   *
   */
  void
  Unfreeze()
  {
    auto *atomic_stat = reinterpret_cast<std::atomic<StatusWord> *>(&status_);
    atomic_stat->store(status_.Unfreeze(), std::memory_order_relaxed);
  }

  /*####################################################################################
   * Leaf read operations
   *##################################################################################*/

  /**
   * @brief Read a payload of a specified key if it exists.
   *
   * @tparam Payload a class of payload.
   * @param key a target key.
   * @param out_payload a reference to be stored a target payload.
   * @retval kSuccess if a key exists.
   * @retval kKeyNotExist if a key does not exist.
   */
  template <class Payload>
  auto
  Read(  //
      const Key &key,
      Payload &out_payload) const  //
      -> NodeRC
  {
    // check whether there is a given key in this node
    KeyExistence rc{};
    size_t pos{};
    if constexpr (CanCASUpdate<Payload>()) {
      std::tie(rc, pos) = SearchSortedRecord(key);
      if (rc == kNotExist || rc == kDeleted) {
        // a new record may be inserted in an unsorted region
        const auto status = GetStatusWordProtected();
        std::tie(rc, pos) = SearchUnsortedRecord(key, status.GetRecordCount() - 1);
      }
    } else {
      const auto status = GetStatusWordProtected();
      std::tie(rc, pos) = CheckUniqueness<Payload>(key, status.GetRecordCount() - 1);
    }
    if (rc == kNotExist || rc == kDeleted) return kKeyNotExist;

    // copy a written payload to a given address
    const auto meta = GetMetadataProtected(pos);
    if constexpr (CanCASUpdate<Payload>()) {
      out_payload = MwCASDescriptor::Read<Payload>(GetPayloadAddr(meta));
    } else {
      memcpy(&out_payload, GetPayloadAddr(meta), sizeof(Payload));
    }

    return kSuccess;
  }

  /*####################################################################################
   * Public APIs for scanning
   *##################################################################################*/

  /**
   * @brief Initialize header information for scanning.
   *
   */
  void
  InitForScanning()
  {
    node_size_ = 0;
    sorted_count_ = 0;
    do_splitting_ = 0;
  }

  /**
   * @brief Get the end position of records for scanning and check it has been finished.
   *
   * @param end_key a pair of a target key and its closed/open-interval flag.
   * @retval 1st: true if this node is end of scanning.
   * @retval 2nd: the end position for scanning.
   */
  [[nodiscard]] auto
  SearchEndPositionFor(const std::optional<std::pair<const Key &, bool>> &end_key) const  //
      -> std::pair<bool, size_t>
  {
    const auto is_end = IsRightmostOf(end_key);
    size_t end_pos{};
    if (is_end && end_key) {
      const auto &[e_key, e_closed] = *end_key;
      const auto [rc, pos] = SearchSortedRecord(e_key);
      end_pos = (rc == kExist && e_closed) ? pos + 1 : pos;
    } else {
      end_pos = sorted_count_;
    }

    return {is_end, end_pos};
  }

  /*####################################################################################
   * Leaf write operations
   *##################################################################################*/

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
   * @tparam Payload a class of payload.
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_length the length of a target key.
   * @param payload_length the length of a target payload.
   * @retval kSuccess if a key/payload pair is written.
   * @retval kFrozen if a target node is frozen.
   * @retval kNeedConsolidation if a target node requires consolidation.
   */
  template <class Payload>
  auto
  Write(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload)  //
      -> NodeRC
  {
    // variables and constants shared in Phase 1 & 2
    const auto [key_len, rec_len] = Align<Key, Payload>(key_length);
    const auto in_progress_meta = Metadata{key_len, rec_len};
    StatusWord cur_status;
    size_t target_pos{};

    /*----------------------------------------------------------------------------------
     * Phase 1: reserve free space to write a record
     *--------------------------------------------------------------------------------*/
    while (true) {
      cur_status = GetStatusWordProtected();
      if (cur_status.IsFrozen()) return kFrozen;

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
          if (desc.MwCAS()) return kSuccess;
          continue;
        }
      }

      // prepare new status for MwCAS
      const auto new_status = cur_status.Add(rec_len);
      if (new_status.NeedConsolidation(sorted_count_)) return kNeedConsolidation;
      target_pos = cur_status.GetRecordCount();

      // perform MwCAS to reserve space
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, cur_status, new_status);
      SetMetadataForMwCAS(desc, target_pos, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
      SPINLOCK_HINT
    }

    /*----------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - cur_status.GetBlockSize();
    offset = SetPayload(offset, payload);
    offset = SetKey(offset, key, key_len);
    std::atomic_thread_fence(std::memory_order_release);

    // prepare record metadata for MwCAS
    const auto inserted_meta = in_progress_meta.Commit(offset);

    // check conflicts (concurrent SMOs)
    while (true) {
      const auto status = GetStatusWordProtected();
      if (status.IsFrozen()) return kFrozen;

      // perform MwCAS to complete a write
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, status, status);
      SetMetadataForMwCAS(desc, target_pos, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
      SPINLOCK_HINT
    }

    return kSuccess;
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
   * @tparam Payload a class of payload.
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_length the length of a target key.
   * @param payload_length the length of a target payload.
   * @retval kSuccess if a key/payload pair is written.
   * @retval kKeyExist if a specified key exists.
   * @retval kFrozen if a target node is frozen.
   * @retval kNeedConsolidation if a target node requires consolidation.
   */
  template <class Payload>
  auto
  Insert(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload)  //
      -> NodeRC
  {
    // variables and constants shared in Phase 1 & 2
    const auto [key_len, rec_len] = Align<Key, Payload>(key_length);
    const auto in_progress_meta = Metadata{key_len, rec_len};
    StatusWord cur_status;
    size_t target_pos{};
    size_t recheck_pos{};
    KeyExistence rc{};

    /*----------------------------------------------------------------------------------
     * Phase 1: reserve free space to write a record
     *--------------------------------------------------------------------------------*/
    while (true) {
      cur_status = GetStatusWordProtected();
      if (cur_status.IsFrozen()) return kFrozen;

      // check uniqueness
      target_pos = cur_status.GetRecordCount();
      std::tie(rc, recheck_pos) = CheckUniqueness<Payload>(key, target_pos - 1);
      if (rc == kExist) return kKeyExist;

      // prepare new status for MwCAS
      const auto new_status = cur_status.Add(rec_len);
      if (new_status.NeedConsolidation(sorted_count_)) return kNeedConsolidation;

      // perform MwCAS to reserve space
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, cur_status, new_status);
      SetMetadataForMwCAS(desc, target_pos, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - cur_status.GetBlockSize();
    offset = SetPayload(offset, payload);
    offset = SetKey(offset, key, key_len);
    std::atomic_thread_fence(std::memory_order_release);

    // prepare record metadata for MwCAS
    const auto inserted_meta = in_progress_meta.Commit(offset);

    // check concurrent SMOs
    while (true) {
      const auto status = GetStatusWordProtected();
      if (status.IsFrozen()) return kFrozen;

      // recheck uniqueness if required
      if (rc == kUncertain) {
        std::tie(rc, recheck_pos) = CheckUniqueness<Payload>(key, recheck_pos);
        if (rc == kUncertain) continue;
        if (rc == kExist) {
          // delete an inserted record
          SetMetadata(target_pos, in_progress_meta.Delete());
          return kKeyExist;
        }
      }

      // perform MwCAS to complete an insert
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, status, status);
      SetMetadataForMwCAS(desc, target_pos, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
      SPINLOCK_HINT
    }

    return kSuccess;
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
   * @tparam Payload a class of payload.
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_length the length of a target key.
   * @param payload_length the length of a target payload.
   * @retval kSuccess if a key/payload pair is written.
   * @retval kKeyNotExist if a specified key does not exist.
   * @retval kFrozen if a target node is frozen.
   * @retval kNeedConsolidation if a target node requires consolidation.
   */
  template <class Payload>
  auto
  Update(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload)  //
      -> NodeRC
  {
    // variables and constants shared in Phase 1 & 2
    const auto [key_len, rec_len] = Align<Key, Payload>(key_length);
    const auto deleted_size = kWordSize + rec_len;
    const auto in_progress_meta = Metadata{key_len, rec_len};
    StatusWord cur_status;
    size_t target_pos{};
    size_t exist_pos{};
    KeyExistence rc{};

    /*----------------------------------------------------------------------------------
     * Phase 1: reserve free space to write a record
     *--------------------------------------------------------------------------------*/
    while (true) {
      cur_status = GetStatusWordProtected();
      if (cur_status.IsFrozen()) return kFrozen;

      // check whether a node includes a target key
      target_pos = cur_status.GetRecordCount();
      std::tie(rc, exist_pos) = CheckUniqueness<Payload>(key, target_pos - 1);
      if (rc == kNotExist || rc == kDeleted) return kKeyNotExist;

      if constexpr (CanCASUpdate<Payload>()) {
        if (rc == kExist && exist_pos < sorted_count_) {
          const auto meta = GetMetadataProtected(exist_pos);

          // update a record directly
          MwCASDescriptor desc{};
          SetStatusForMwCAS(desc, cur_status, cur_status);
          SetMetadataForMwCAS(desc, exist_pos, meta, meta);
          SetPayloadForMwCAS(desc, meta, payload);
          if (desc.MwCAS()) return kSuccess;
          continue;
        }
      }

      // prepare new status for MwCAS
      const auto new_status = cur_status.Add(rec_len).Delete(deleted_size);
      if (new_status.NeedConsolidation(sorted_count_)) return kNeedConsolidation;

      // perform MwCAS to reserve space
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, cur_status, new_status);
      SetMetadataForMwCAS(desc, target_pos, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - cur_status.GetBlockSize();
    offset = SetPayload(offset, payload);
    offset = SetKey(offset, key, key_len);
    std::atomic_thread_fence(std::memory_order_release);

    // prepare record metadata for MwCAS
    const auto inserted_meta = in_progress_meta.Commit(offset);

    // check conflicts (concurrent SMOs)
    while (true) {
      const auto status = GetStatusWordProtected();
      if (status.IsFrozen()) return kFrozen;

      // recheck uniqueness if required
      if (rc == kUncertain) {
        std::tie(rc, exist_pos) = CheckUniqueness<Payload>(key, exist_pos);
        if (rc == kUncertain) continue;
        if (rc == kNotExist || rc == kDeleted) {
          // delete an inserted record
          SetMetadata(target_pos, in_progress_meta.Delete());
          return kKeyNotExist;
        }
      }

      // perform MwCAS to complete an update
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, status, status);
      SetMetadataForMwCAS(desc, target_pos, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
      SPINLOCK_HINT
    }

    return kSuccess;
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
   * @tparam Payload a class of payload.
   * @param key a target key to be written.
   * @param key_length the length of a target key.
   * @retval kSuccess if a key/payload pair is written.
   * @retval kKeyNotExist if a specified key does not exist.
   * @retval kFrozen if a target node is frozen.
   * @retval kNeedConsolidation if a target node requires consolidation.
   */
  template <class Payload>
  auto
  Delete(  //
      const Key &key,
      const size_t key_len)  //
      -> NodeRC
  {
    // variables and constants
    const auto rec_len = Align<Key, Payload>(key_len).second - sizeof(Payload);
    const auto in_progress_meta = Metadata{key_len, rec_len};
    StatusWord cur_status;
    size_t target_pos{};
    size_t exist_pos{};
    KeyExistence rc{};

    /*----------------------------------------------------------------------------------
     * Phase 1: reserve free space to write a record
     *--------------------------------------------------------------------------------*/
    while (true) {
      cur_status = GetStatusWordProtected();
      if (cur_status.IsFrozen()) return kFrozen;

      // check whether a node includes a target key
      target_pos = cur_status.GetRecordCount();
      std::tie(rc, exist_pos) = CheckUniqueness<Payload>(key, target_pos - 1);
      if (rc == kNotExist || rc == kDeleted) return kKeyNotExist;

      const auto meta = GetMetadataProtected(exist_pos);
      if constexpr (CanCASUpdate<Payload>()) {
        if (rc == kExist && exist_pos < sorted_count_) {
          const auto deleted_meta = meta.Delete();
          const auto new_status = cur_status.Delete(kWordSize + meta.GetTotalLength());
          if (new_status.NeedConsolidation(sorted_count_)) return kNeedConsolidation;

          // delete a record directly
          MwCASDescriptor desc{};
          SetStatusForMwCAS(desc, cur_status, new_status);
          SetMetadataForMwCAS(desc, exist_pos, meta, deleted_meta);
          if (desc.MwCAS()) return kSuccess;
          continue;
        }
      }

      // prepare new status for MwCAS
      const auto deleted_size = (2 * kWordSize) + meta.GetTotalLength() + rec_len;
      const auto new_status = cur_status.Add(rec_len).Delete(deleted_size);
      if (new_status.NeedConsolidation(sorted_count_)) return kNeedConsolidation;

      // perform MwCAS to reserve space
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, cur_status, new_status);
      SetMetadataForMwCAS(desc, target_pos, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------*/

    // insert a null record
    auto offset = kPageSize - cur_status.GetBlockSize();
    offset = SetKey(offset, key, key_len);
    std::atomic_thread_fence(std::memory_order_release);

    // prepare record metadata for MwCAS
    const auto deleted_meta = in_progress_meta.Delete(offset);

    // check concurrent SMOs
    while (true) {
      const auto status = GetStatusWordProtected();
      if (status.IsFrozen()) return kFrozen;

      // recheck uniqueness if required
      if (rc == kUncertain) {
        std::tie(rc, exist_pos) = CheckUniqueness<Payload>(key, exist_pos);
        if (rc == kUncertain) continue;
        if (rc == kNotExist || rc == kDeleted) {
          // delete an inserted record
          SetMetadata(target_pos, in_progress_meta.Delete());
          return kKeyNotExist;
        }
      }

      // perform MwCAS to complete an insert
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, status, status);
      SetMetadataForMwCAS(desc, target_pos, in_progress_meta, deleted_meta);
      if (desc.MwCAS()) break;
      SPINLOCK_HINT
    }

    return kSuccess;
  }

  /*####################################################################################
   * Public structure modification operations
   *##################################################################################*/

  /**
   * @brief Consolidate a given node into this node.
   *
   * @tparam Payload a class of payload.
   * @param node an original node.
   */
  template <class Payload>
  void
  Consolidate(const Node *node)
  {
    auto offset = node->ConsolidateTo<Payload>(kPageSize, this);

    // set header information
    offset = CopyHighKeyFrom<Payload>(node, offset);
    status_ = StatusWord{sorted_count_, kPageSize - offset};
  }

  /**
   * @brief Split this node into two nodes.
   *
   * @tparam Payload a class of payload.
   * @param l_node a split left node.
   * @param r_node a split right node.
   */
  template <class Payload>
  void
  Split(  //
      Node *l_node,
      Node *r_node) const
  {
    // copy records to left/right nodes
    l_node->do_splitting_ = true;
    auto offset = kPageSize;
    for (size_t i = 0; i < sorted_count_; ++i) {
      offset = CopyRecord<Payload>(this, l_node, meta_array_[i], offset, r_node);
    }

    // set header information for a right node (left header is set via splitting)
    offset = r_node->CopyHighKeyFrom<Payload>(this, offset);
    r_node->status_ = StatusWord{r_node->sorted_count_, kPageSize - offset};
  }

  /**
   * @brief Initialize this node as a parent node of split nodes.
   *
   * @param old_node an old parent node.
   * @param l_child a split left child node.
   * @param r_child a split right child node.
   * @param l_pos the position of a left child node.
   * @retval true if this node needs recursive splitting
   * @retval false otherwise
   */
  auto
  InitAsSplitParent(  //
      const Node *old_node,
      const Node *l_child,
      const Node *r_child,
      const size_t l_pos)  //
      -> bool
  {
    // set a lowest/highest keys
    auto offset = CopyHighKeyFrom<Node *>(old_node, kPageSize);

    // copy lower records
    offset = CopyRecords<Node *>(old_node, this, 0, l_pos, offset);

    // insert split nodes
    const auto l_meta = l_child->meta_array_[l_child->sorted_count_ - 1];
    offset = InsertChild(l_child, l_meta, l_child, l_pos, offset);
    const auto r_meta = r_child->meta_array_[r_child->sorted_count_ - 1];
    const auto r_pos = l_pos + 1;
    offset = InsertChild(r_child, r_meta, r_child, r_pos, offset);
    sorted_count_ += 2;

    // copy upper records
    offset = CopyRecords<Node *>(old_node, this, r_pos, old_node->sorted_count_, offset);

    // set an updated header
    StatusWord stat{sorted_count_, kPageSize - offset};
    if (stat.NeedInternalSplit<Key>()) {
      status_ = stat.Freeze();
      return true;
    }
    status_ = stat;
    return false;
  }

  /**
   * @brief Initialize this node as a new root node.
   *
   * @param l_child a split left child node.
   * @param r_child a split right child node.
   */
  void
  InitAsRoot(  //
      const Node *l_child,
      const Node *r_child)
  {
    // insert initial children
    const auto l_meta = l_child->meta_array_[l_child->sorted_count_ - 1];
    auto offset = kPageSize;
    offset = InsertChild(l_child, l_meta, l_child, 0, offset);
    const auto r_meta = r_child->meta_array_[r_child->sorted_count_ - 1];
    offset = InsertChild(r_child, r_meta, r_child, 1, offset);

    // set an updated header
    sorted_count_ = 2;
    status_ = StatusWord{2, kPageSize - offset};
  }

  /**
   * @brief Merge given nodes into this node.
   *
   * @tparam Payload a class of payload.
   * @param l_node a left node to be merged.
   * @param r_node a right node to be merged.
   * @retval true if this node needs recursive merging
   * @retval false otherwise
   */
  template <class Payload>
  void
  Merge(  //
      const Node *l_node,
      const Node *r_node)
  {
    constexpr auto kIsInternal = std::is_same_v<Payload, Node *>;

    // copy records in left/right nodes
    auto offset = (kIsInternal)
                      ? CopyRecords<Payload>(l_node, this, 0, l_node->sorted_count_, kPageSize)
                      : l_node->ConsolidateTo<Payload>(kPageSize, this);
    offset = CopyRecords<Payload>(r_node, this, 0, r_node->sorted_count_, offset);

    // set header information
    offset = CopyHighKeyFrom<Payload>(r_node, offset);
    status_ = StatusWord{sorted_count_, kPageSize - offset};
  }

  /**
   * @brief Initialize this node as a parent node of merged nodes.
   *
   * @param old_node an old parent node.
   * @param merged_child a merged child node.
   * @param position the position of a merged child node.
   */
  auto
  InitAsMergeParent(  //
      const Node *old_node,
      const Node *merged_child,
      const size_t position)  //
      -> bool
  {
    // set a lowest/highest keys
    auto offset = CopyHighKeyFrom<Node *>(old_node, kPageSize);

    // copy lower records
    offset = CopyRecords<Node *>(old_node, this, 0, position, offset);

    // insert a merged node
    const auto meta = merged_child->meta_array_[merged_child->sorted_count_ - 1];
    offset = InsertChild(merged_child, meta, merged_child, position, offset);
    ++sorted_count_;

    // copy upper records
    const auto r_pos = position + 2;
    offset = CopyRecords<Node *>(old_node, this, r_pos, old_node->sorted_count_, offset);

    // set an updated header
    StatusWord stat{sorted_count_, kPageSize - offset};
    if (stat.NeedMerge()) {
      status_ = stat.Freeze();
      return true;
    }
    status_ = stat;
    return false;
  }

  /*####################################################################################
   * Public bulkload API
   *##################################################################################*/

  /**
   * @brief Create a leaf node with the maximum number of records for bulkloading.
   *
   * @tparam Payload a target payload class.
   * @param iter the begin position of target records.
   * @param iter_end the end position of target records.
   */
  template <class Payload>
  void
  Bulkload(  //
      typename std::vector<BulkloadEntry<Key, Payload>>::const_iterator &iter,
      const typename std::vector<BulkloadEntry<Key, Payload>>::const_iterator &iter_end)
  {
    // extract and insert entries for the leaf node
    size_t node_size = kHeaderLength;
    auto offset = kPageSize;
    while (iter < iter_end) {
      const auto key_length = iter->GetKeyLength();
      const auto [key_len, rec_len] = Align<Key, Payload>(key_length);

      // check whether the node has sufficent space
      node_size += rec_len + kWordSize;
      if (node_size > kPageSize - kMinFreeSpaceSize) break;

      // insert an entry to the leaf node
      auto tmp_offset = SetPayload(offset, iter->GetPayload());
      tmp_offset = SetKey(tmp_offset, iter->GetKey(), key_len);
      meta_array_[sorted_count_] = Metadata{tmp_offset, key_len, rec_len};
      offset -= rec_len;

      ++sorted_count_;
      ++iter;
    }

    // set a highest key
    const auto high_meta = meta_array_[sorted_count_ - 1];
    const auto high_key_len = high_meta.GetKeyLength();
    high_meta_ = Metadata{high_meta.GetOffset(), high_key_len, high_key_len};

    // create the header of the leaf node
    status_ = StatusWord{sorted_count_, kPageSize - offset};
  }

  /**
   * @brief Insert a child node directly.
   *
   * @param node a target child node.
   * @retval true if this node should be split.
   * @retval false otherwise.
   */
  auto
  LoadChildNode(Node *node)  //
      -> bool
  {
    // insert a new node
    const auto meta = node->meta_array_[node->sorted_count_ - 1];
    auto offset = kPageSize - status_.GetBlockSize();
    offset = InsertChild(node, meta, node, sorted_count_, offset);

    // update headers
    ++sorted_count_;
    status_ = StatusWord{sorted_count_, kPageSize - offset};

    return status_.NeedInternalSplit<Key>();
  }

  /**
   * @brief Delete the last (i.e., rightmost) child node from this.
   *
   */
  void
  RemoveLastNode()
  {
    // update only a header
    const auto meta = meta_array_[sorted_count_ - 1];
    const auto offset = kPageSize - (status_.GetBlockSize() - meta.GetTotalLength());
    --sorted_count_;
    status_ = StatusWord{sorted_count_, kPageSize - offset};
  }

 private:
  /*####################################################################################
   * Internal classes
   *##################################################################################*/

  /**
   * @brief A class to sort metadata.
   *
   */
  struct MetaKeyPair {
    Metadata meta{};
    Key key{};
  };

  /*####################################################################################
   * Internal getters
   *##################################################################################*/

  /**
   * @param end_key a pair of a target key and its closed/open-interval flag.
   * @retval true if this node is a rightmost node for the given key.
   * @retval false otherwise.
   */
  [[nodiscard]] auto
  IsRightmostOf(const std::optional<std::pair<const Key &, bool>> &end_key) const  //
      -> bool
  {
    if (high_meta_.GetKeyLength() == 0) return true;  // the rightmost node
    if (!end_key) return false;                       // perform full scan
    return !Compare{}(GetKey(high_meta_), end_key->first);
  }

  /**
   * @brief Read metadata with MwCAS read protection.
   *
   * This function uses a MwCAS read operation internally, and so it is guaranteed that
   * read metadata is valid.
   *
   * @return metadata.
   */
  [[nodiscard]] auto
  GetMetadataProtected(const size_t position) const  //
      -> Metadata
  {
    return MwCASDescriptor::Read<Metadata>(&meta_array_[position]);
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return an address of a target key.
   */
  [[nodiscard]] constexpr auto
  GetKeyAddr(const Metadata meta) const  //
      -> void *
  {
    return ShiftAddr(this, meta.GetOffset());
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return an address of a target payload.
   */
  [[nodiscard]] constexpr auto
  GetPayloadAddr(const Metadata meta) const  //
      -> void *
  {
    return ShiftAddr(this, meta.GetOffset() + meta.GetKeyLength());
  }

  /*####################################################################################
   * Internal setters
   *##################################################################################*/

  /**
   * @brief Set metadata atomically.
   *
   * @param pos the position of metadata to be set.
   * @param meta metadata to be set.
   */
  void
  SetMetadata(  //
      const size_t pos,
      const Metadata meta)
  {
    auto *atomic_meta = reinterpret_cast<std::atomic<Metadata> *>(meta_array_ + pos);
    atomic_meta->store(meta, std::memory_order_relaxed);
  }

  /**
   * @brief Set a target key directly.
   *
   * @tparam Key a class of targets.
   * @param offset an offset to set a target key.
   * @param key a target key to be set.
   * @param key_len the length of a target key.
   */
  auto
  SetKey(  //
      size_t offset,
      const Key &key,
      const size_t key_len)  //
      -> size_t
  {
    offset -= key_len;
    if constexpr (IsVariableLengthData<Key>()) {
      memcpy(ShiftAddr(this, offset), key, key_len);
    } else {
      memcpy(ShiftAddr(this, offset), &key, sizeof(Key));
    }

    return offset;
  }

  /**
   * @brief Set a target payload directly.
   *
   * @tparam Payload a class of targets.
   * @param offset an offset to set a target payload.
   * @param payload a target payload to be set.
   */
  template <class Payload>
  auto
  SetPayload(  //
      size_t offset,
      const Payload &payload)  //
      -> size_t
  {
    offset -= sizeof(Payload);
    memcpy(ShiftAddr(this, offset), &payload, sizeof(Payload));

    return offset;
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
   * @tparam Payload a class of payload.
   * @param desc a target MwCAS descriptor.
   * @param meta metadata of a target record.
   * @param new_payload a new payload for MwCAS.
   */
  template <class Payload>
  void
  SetPayloadForMwCAS(  //
      MwCASDescriptor &desc,
      const Metadata meta,
      const Payload &new_payload)
  {
    static_assert(CanCASUpdate<Payload>());

    auto &&old_payload = MwCASDescriptor::Read<Payload>(GetPayloadAddr(meta));
    desc.AddMwCASTarget(GetPayloadAddr(meta), old_payload, new_payload);
  }

  /*####################################################################################
   * Internal functions for destruction
   *##################################################################################*/

  /**
   * @brief Compute the maximum number of records in a node.
   *
   * @return the expected maximum number of records.
   */
  static constexpr auto
  GetMaxRecordNum()  //
      -> size_t
  {
    // the length of metadata
    auto record_min_length = sizeof(Metadata);

    // the length of keys
    if constexpr (IsVariableLengthData<Key>()) {
      record_min_length += 1;
    } else {
      record_min_length += sizeof(Key);
    }

    // the minimum length of payloads
    record_min_length += 1;

    return (kPageSize - kHeaderLength) / record_min_length;
  }

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  /**
   * @brief Get the position of a specified key by using lenear search.
   *
   * @param key a target key.
   * @param begin_pos the begin position for searching.
   * @return a pair of key existence and a key position.
   */
  [[nodiscard]] auto
  SearchUnsortedRecord(  //
      const Key &key,
      const size_t begin_pos) const  //
      -> std::pair<KeyExistence, size_t>
  {
    std::atomic_thread_fence(std::memory_order_acquire);

    // perform a linear search in revese order
    for (int64_t pos = begin_pos; pos >= sorted_count_; --pos) {
      const auto meta = GetMetadataProtected(pos);
      if (meta.IsInProgress()) {
        if (meta.IsVisible()) return {kUncertain, pos};
        continue;
      }

      const auto &target_key = GetKey(meta);
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
   * @tparam Payload a class of payload.
   * @param key a target key.
   * @param rec_count the total number of records in a node.
   * @return a pair of key existence and a key position.
   */
  template <class Payload>
  [[nodiscard]] auto
  CheckUniqueness(  //
      const Key &key,
      const int64_t begin_pos) const  //
      -> std::pair<KeyExistence, size_t>
  {
    KeyExistence rc{};
    size_t pos{};

    if constexpr (CanCASUpdate<Payload>()) {
      std::tie(rc, pos) = SearchSortedRecord(key);
      if (rc == kNotExist || rc == kDeleted) {
        // a new record may be inserted in an unsorted region
        std::tie(rc, pos) = SearchUnsortedRecord(key, begin_pos);
      }
    } else {
      std::tie(rc, pos) = SearchUnsortedRecord(key, begin_pos);
      if (rc == kNotExist) {
        // a record may be in a sorted region
        std::tie(rc, pos) = SearchSortedRecord(key);
      }
    }

    return {rc, pos};
  }

  /**
   * @brief Insert a new child node into this node.
   *
   * @param orig_node an original node that has a target child.
   * @param orig_meta the corresponding metadata of a target child.
   * @param child_node a target child node.
   * @param rec_count the current number of records in this node.
   * @param offset the current offset of this node.
   * @return the updated offset value.
   */
  auto
  InsertChild(  //
      const Node *orig_node,
      const Metadata orig_meta,
      const Node *child_node,
      const size_t rec_count,
      const size_t offset)  //
      -> size_t
  {
    size_t key_len{};
    size_t rec_len{};

    const auto &key = orig_node->GetKey(orig_meta);
    if constexpr (IsVariableLengthData<Key>()) {
      const auto key_length = orig_meta.GetKeyLength();
      std::tie(key_len, rec_len) = Align<Key, Node *>(key_length);
    } else {
      std::tie(key_len, rec_len) = Align<Key, Node *>(sizeof(Key));
    }

    auto tmp_offset = SetPayload(offset, child_node);
    tmp_offset = SetKey(tmp_offset, key, key_len);
    meta_array_[rec_count] = Metadata{tmp_offset, key_len, rec_len};

    return offset - rec_len;
  }

  /**
   * @brief Copy a highest key from a given node.
   *
   * @param node an original node that has a target key.
   * @param offset the current offset of this node.
   * @return the updated offset value.
   */
  template <class Payload>
  auto
  CopyHighKeyFrom(  //
      const Node *node,
      size_t offset)  //
      -> size_t
  {
    const auto high_meta = node->high_meta_;
    const auto key_len = high_meta.GetKeyLength();

    // copy a highest key from the given node
    offset -= key_len;
    memcpy(ShiftAddr(this, offset), node->GetKeyAddr(high_meta), key_len);
    high_meta_ = Metadata{offset, key_len, key_len};

    // align offset if needed
    if constexpr (NeedOffsetAlignment<Key, Payload>()) {
      if (key_len > 0) {
        offset = Pad<Key, Payload>(offset);
      }
    }

    return offset;
  }

  /**
   * @brief Copy a record from a given node to another one.
   *
   * @tparam Payload a class of payload.
   * @param from_node an original node that has a target record.
   * @param to_node a destination node for copying.
   * @param meta the corresponding metadata of a target record.
   * @param offset the current offset of this node.
   * @param r_node an optional right-split node for splitting.
   * @return the updated offset value.
   */
  template <class Payload>
  static auto
  CopyRecord(  //
      const Node *from_node,
      Node *&to_node,
      const Metadata meta,
      size_t offset,
      Node *r_node = nullptr)  //
      -> size_t
  {
    const auto key_len = meta.GetKeyLength();
    const auto rec_len = meta.GetTotalLength();
    auto tmp_offset = offset;

    if constexpr (CanCASUpdate<Payload>()) {
      // copy a payload with MwCAS read protection
      tmp_offset -= sizeof(Payload);
      const auto &payload = MwCASDescriptor::Read<Payload>(from_node->GetPayloadAddr(meta));
      memcpy(ShiftAddr(to_node, tmp_offset), &payload, sizeof(Payload));

      // copy a correspondng key
      tmp_offset -= key_len;
      memcpy(ShiftAddr(to_node, tmp_offset), from_node->GetKeyAddr(meta), key_len);
    } else {
      // copy a record from the given node
      const auto act_rec_len = key_len + sizeof(Payload);
      tmp_offset -= rec_len;
      memcpy(ShiftAddr(to_node, tmp_offset), from_node->GetKeyAddr(meta), act_rec_len);
    }

    // set new metadata
    to_node->meta_array_[to_node->sorted_count_] = Metadata{tmp_offset, key_len, rec_len};

    // update header information
    ++to_node->sorted_count_;
    to_node->node_size_ += rec_len + sizeof(Metadata);

    if (to_node->do_splitting_ && to_node->node_size_ > kPageSize / 2) {
      // finalize copying of a split-left node
      to_node->do_splitting_ = false;
      to_node->high_meta_ = Metadata{tmp_offset, key_len, key_len};

      // reduce offset to preserve space for a highest key
      offset -= rec_len + key_len;
      if constexpr (NeedOffsetAlignment<Key, Payload>()) {
        // align offset if needed
        offset = Pad<Key, Payload>(offset);
      }
      to_node->status_ = StatusWord{to_node->sorted_count_, kPageSize - offset};

      // initialize a split-right node
      to_node = r_node;
      offset = kPageSize;
    } else {
      offset -= rec_len;
    }

    return offset;
  }

  /**
   * @brief Copy records from a given node to another one.
   *
   * @tparam Payload a class of payload.
   * @param from_node an original node that has a target record.
   * @param to_node a destination node for copying.
   * @param begin_pos the begin position of target records.
   * @param end_pos the end position of target records.
   * @param offset the current offset of this node.
   * @return the updated offset value.
   */
  template <class Payload>
  static auto
  CopyRecords(  //
      const Node *from_node,
      Node *to_node,
      const size_t begin_pos,
      const size_t end_pos,
      size_t offset)  //
      -> size_t
  {
    // copy records from the given node
    for (size_t i = begin_pos; i < end_pos; ++i) {
      offset = CopyRecord<Payload>(from_node, to_node, from_node->meta_array_[i], offset);
    }

    return offset;
  }

  /**
   * @brief Sort unsorted records by insertion sort.
   *
   * @return a pair of the number of new records and its array.
   */
  [[nodiscard]] auto
  SortNewRecords() const  //
      -> std::pair<size_t, std::array<MetaKeyPair, kMaxDeltaRecNum>>
  {
    const auto rec_count = GetStatusWordProtected().GetRecordCount();
    std::atomic_thread_fence(std::memory_order_acquire);

    std::array<MetaKeyPair, kMaxDeltaRecNum> arr;
    size_t count = 0;

    // sort unsorted records by insertion sort
    for (size_t pos = sorted_count_; pos < rec_count; ++pos) {
      // check whether a record has been inserted
      const auto meta = GetMetadataProtected(pos);
      if (meta.IsInProgress()) continue;

      // search an inserting position
      const auto &cur_key = GetKey(meta);
      size_t i = 0;
      for (; i < count; ++i) {
        if (!Compare{}(arr[i].key, cur_key)) break;
      }

      // shift upper records if needed
      if (i >= count) {
        ++count;
      } else if (Compare{}(cur_key, arr[i].key)) {
        memmove(&(arr[i + 1]), &(arr[i]), sizeof(MetaKeyPair) * (count - i));
        ++count;
      }

      // insert a new record
      arr[i] = MetaKeyPair{meta, cur_key};
    }

    return {count, arr};
  }

  /**
   * @brief Consolidate a target leaf node.
   *
   * @tparam Payload a class of payload.
   * @param old_node an original node.
   */
  template <class Payload>
  auto
  ConsolidateTo(  //
      size_t offset,
      Node *node) const  //
      -> size_t
  {
    // sort records in an unsorted region
    const auto [new_rec_num, records] = SortNewRecords();

    // perform merge-sort to consolidate a node
    size_t j = 0;
    for (size_t i = 0; i < sorted_count_; ++i) {
      const auto meta = GetMetadataProtected(i);
      const auto &key = GetKey(meta);

      // copy new records
      for (; j < new_rec_num; ++j) {
        const auto &[rec_meta, rec_key] = records[j];
        if (!Compare{}(rec_key, key)) break;

        // check a new record is active
        if (rec_meta.IsVisible()) {
          offset = CopyRecord<Payload>(this, node, rec_meta, offset);
        }
      }

      // check a new record is updated one
      if (j < new_rec_num && IsEqual<Compare>(key, records[j].key)) {
        const auto rec_meta = records[j++].meta;
        if (rec_meta.IsVisible()) {
          offset = CopyRecord<Payload>(this, node, rec_meta, offset);
        }
      } else if (meta.IsVisible()) {
        offset = CopyRecord<Payload>(this, node, meta, offset);
      }
    }

    // move remaining new records
    for (; j < new_rec_num; ++j) {
      const auto rec_meta = records[j].meta;
      if (rec_meta.IsVisible()) {
        offset = CopyRecord<Payload>(this, node, rec_meta, offset);
      }
    }

    return offset;
  }

  /*####################################################################################
   * Internal variables
   *##################################################################################*/

  /// the byte length of a node page.
  uint64_t node_size_ : 32;

  /// the number of sorted records.
  uint64_t sorted_count_ : 16;

  /// a flag for indicating whether this node is a leaf or internal node.
  uint64_t is_leaf_ : 1;

  /// a flag for indicating whether this node is being split.
  uint64_t do_splitting_ : 1;

  /// a black block for alignment.
  uint64_t : 0;

  /// a status word.
  StatusWord status_{};

  /// the metadata of a highest key.
  Metadata high_meta_{};

  /// the head of a metadata array.
  Metadata meta_array_[0];
};

}  // namespace dbgroup::index::bztree::component

#endif  // BZTREE_COMPONENT_NODE_HPP
