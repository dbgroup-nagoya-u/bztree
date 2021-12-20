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

#include <stdlib.h>

#include <atomic>
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
class alignas(kMaxAlignment) Node
{
 public:
  /*################################################################################################
   * Public constructors and assignment operators
   *##############################################################################################*/

  /**
   * @brief Construct a new node object.
   *
   * @param is_leaf a flag to indicate whether a leaf node is constructed.
   */
  template <class Payload>
  explicit Node(const bool is_leaf)
      : node_size_{kPageSize},
        sorted_count_{0},
        is_leaf_{is_leaf},
        is_right_end_{true},
        status_{0, GetInitialOffset<Payload>()}
  {
  }

  Node(const Node &) = delete;
  Node &operator=(const Node &) = delete;
  Node(Node &&) = delete;
  Node &operator=(Node &&) = delete;

  /*################################################################################################
   * Public destructors
   *##############################################################################################*/

  /**
   * @brief Destroy the node object.
   *
   */
  ~Node()
  {
    // fill a metadata region with zeros
    for (size_t i = 0; i < GetMaxRecordNum(); ++i) {
      auto *dummy_p = reinterpret_cast<size_t *>(meta_array_ + i);
      *dummy_p = 0UL;
    }

    // set a status word to release memory brrier
    SetStatus(StatusWord{});
  }

  /*################################################################################################
   * new/delete operators
   *##############################################################################################*/

  static auto
  operator new(std::size_t)  //
      -> void *
  {
    return calloc(1UL, kPageSize);
  }

  static auto
  operator new(std::size_t, void *where)  //
      -> void *
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
   * @retval false otherwise.
   */
  constexpr auto
  IsLeaf() const  //
      -> bool
  {
    return is_leaf_;
  }

  /**
   * @retval true if this node has a next sibling node.
   * @retval false otherwise.
   */
  constexpr auto
  IsRightEnd() const  //
      -> bool
  {
    return is_right_end_;
  }

  /**
   * @return the number of sorted records.
   */
  constexpr auto
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
  auto
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
  auto
  GetStatusWordProtected() const  //
      -> StatusWord
  {
    const auto stat = MwCASDescriptor::Read<StatusWord>(&status_);
    std::atomic_thread_fence(std::memory_order_acquire);
    return stat;
  }

  auto
  GetChild(const size_t position) const  //
      -> Node *
  {
    return MwCASDescriptor::Read<Node *>(GetPayloadAddr(meta_array_[position]));
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

  constexpr void
  SetChildForMwCAS(  //
      MwCASDescriptor &desc,
      const size_t pos,
      const Node *old_child,
      const Node *new_child)
  {
    desc.AddMwCASTarget(GetPayloadAddr(meta_array_[pos]), old_child, new_child);
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
   * @return the position of a specified key.
   */
  auto
  Search(  //
      const Key &key,
      const bool range_is_closed) const  //
      -> size_t
  {
    const auto *atomic_stat = reinterpret_cast<const std::atomic<StatusWord> *>(&status_);
    [[maybe_unused]] const auto stat = atomic_stat->load(std::memory_order_acquire);

    int64_t begin_pos = 0;
    int64_t end_pos = sorted_count_ - 2;
    while (begin_pos <= end_pos) {
      size_t pos = (begin_pos + end_pos) >> 1;

      const auto index_key = GetKey(meta_array_[pos]);

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
   * @brief Freeze this node for SMOs.
   *
   * @retval kSuccess if a node has become frozen.
   * @retval kFrozen if a node has been already frozen.
   */
  auto
  Freeze()  //
      -> NodeReturnCode
  {
    while (true) {
      const auto current_status = GetStatusWordProtected();
      if (current_status.IsFrozen()) return kFrozen;

      MwCASDescriptor desc;
      SetStatusForMwCAS(desc, current_status, current_status.Freeze());
      if (desc.MwCAS()) break;
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
    SetStatus(status_.Unfreeze());
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
  template <class Payload>
  auto
  Read(  //
      const Key &key,
      Payload &out_payload) const  //
      -> NodeReturnCode
  {
    // check whether there is a given key in this node
    const auto status = GetStatusWordProtected();
    const auto [rc, pos] = CheckUniqueness(key, status.GetRecordCount());
    if (rc == kNotExist || rc == kDeleted) return kKeyNotExist;

    // copy a written payload to a given address
    const auto meta = GetMetadataProtected(pos);
    if constexpr (IsVariableLengthData<Payload>()) {
      const auto payload_length = meta.GetPayloadLength();
      out_payload = reinterpret_cast<Payload>(::operator new(payload_length));
      memcpy(out_payload, GetPayloadAddr(meta), payload_length);
    } else if constexpr (CanCASUpdate<Payload>()) {
      out_payload = MwCASDescriptor::Read<Payload>(GetPayloadAddr(meta));
    } else {
      memcpy(&out_payload, GetPayloadAddr(meta), sizeof(Payload));
    }

    return kSuccess;
  }

  /*################################################################################################
   * Leaf write operations
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
  template <class Payload>
  auto
  Write(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload,
      const size_t payload_length)  //
      -> NodeReturnCode
  {
    // variables and constants shared in Phase 1 & 2
    const auto [key_len, pay_len, rec_len] = AlignRecord<Payload>(key_length, payload_length);
    const auto in_progress_meta = Metadata{key_len, rec_len};
    StatusWord cur_status;
    size_t target_pos;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to write a record
     *--------------------------------------------------------------------------------------------*/
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
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: write a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - cur_status.GetBlockSize();
    offset = SetPayload(offset, payload, pay_len);
    offset = SetKey(offset, key, key_len);

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
    }
    std::atomic_thread_fence(std::memory_order_release);

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
      const Payload &payload,
      const size_t payload_length)  //
      -> NodeReturnCode
  {
    // variables and constants shared in Phase 1 & 2
    const auto [key_len, pay_len, rec_len] = AlignRecord<Payload>(key_length, payload_length);
    const auto in_progress_meta = Metadata{key_len, rec_len};
    StatusWord cur_status;
    size_t target_pos;
    KeyExistence rc;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = GetStatusWordProtected();
      if (cur_status.IsFrozen()) return kFrozen;

      // prepare new status for MwCAS
      const auto new_status = cur_status.Add(rec_len);
      if (new_status.NeedConsolidation(sorted_count_)) return kNeedConsolidation;

      // check uniqueness
      target_pos = cur_status.GetRecordCount();
      rc = CheckUniqueness(key, target_pos).first;
      if (rc == kExist) return kKeyExist;

      // perform MwCAS to reserve space
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, cur_status, new_status);
      SetMetadataForMwCAS(desc, target_pos, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - cur_status.GetBlockSize();
    offset = SetPayload(offset, payload, pay_len);
    offset = SetKey(offset, key, key_len);

    // prepare record metadata for MwCAS
    const auto inserted_meta = in_progress_meta.Commit(offset);

    // check concurrent SMOs
    while (true) {
      const auto status = GetStatusWordProtected();
      if (status.IsFrozen()) return kFrozen;

      // recheck uniqueness if required
      if (rc == kUncertain) {
        rc = CheckUniqueness(key, target_pos).first;
        if (rc == kExist) {
          // delete an inserted record
          SetMetadata(target_pos, in_progress_meta.Delete());
          return kKeyExist;
        } else if (rc == kUncertain) {
          // retry if there are still uncertain records
          continue;
        }
      }

      // perform MwCAS to complete an insert
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, status, status);
      SetMetadataForMwCAS(desc, target_pos, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
    }
    std::atomic_thread_fence(std::memory_order_release);

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
      const Payload &payload,
      const size_t payload_length)  //
      -> NodeReturnCode
  {
    // variables and constants shared in Phase 1 & 2
    const auto [key_len, pay_len, rec_len] = AlignRecord<Payload>(key_length, payload_length);
    const auto in_progress_meta = Metadata{key_len, rec_len};
    StatusWord cur_status;
    size_t target_pos;
    KeyExistence rc;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = GetStatusWordProtected();
      if (cur_status.IsFrozen()) return kFrozen;

      // check whether a node includes a target key
      target_pos = cur_status.GetRecordCount();
      size_t exist_pos;
      std::tie(rc, exist_pos) = CheckUniqueness(key, target_pos);
      if (rc == kNotExist || rc == kDeleted) return kKeyNotExist;

      if constexpr (CanCASUpdate<Payload>()) {
        if (rc == kExist && exist_pos < sorted_count_) {
          const auto target_meta = GetMetadataProtected(exist_pos);

          // update a record directly
          MwCASDescriptor desc{};
          SetStatusForMwCAS(desc, cur_status, cur_status);
          SetMetadataForMwCAS(desc, exist_pos, target_meta, target_meta);
          SetPayloadForMwCAS(desc, target_meta, payload);
          if (desc.MwCAS()) return kSuccess;
          continue;
        }
      }

      // prepare new status for MwCAS
      const auto target_meta = GetMetadataProtected(exist_pos);
      const auto deleted_size = kWordLength + target_meta.GetTotalLength();
      const auto new_status = cur_status.Add(rec_len).Delete(deleted_size);
      if (new_status.NeedConsolidation(sorted_count_)) return kNeedConsolidation;

      // perform MwCAS to reserve space
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, cur_status, new_status);
      SetMetadataForMwCAS(desc, target_pos, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = kPageSize - cur_status.GetBlockSize();
    offset = SetPayload(offset, payload, pay_len);
    offset = SetKey(offset, key, key_len);

    // prepare record metadata for MwCAS
    const auto inserted_meta = in_progress_meta.Commit(offset);

    // check conflicts (concurrent SMOs)
    while (true) {
      const auto status = GetStatusWordProtected();
      if (status.IsFrozen()) return kFrozen;

      // recheck uniqueness if required
      if (rc == kUncertain) {
        rc = CheckUniqueness(key, target_pos).first;
        if (rc == kNotExist || rc == kDeleted) {
          // delete an inserted record
          SetMetadata(target_pos, in_progress_meta.Delete());
          return kKeyNotExist;
        } else if (rc == kUncertain) {
          continue;
        }
      }

      // perform MwCAS to complete an update
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, status, status);
      SetMetadataForMwCAS(desc, target_pos, in_progress_meta, inserted_meta);
      if (desc.MwCAS()) break;
    }
    std::atomic_thread_fence(std::memory_order_release);

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
      const size_t key_length)  //
      -> NodeReturnCode
  {
    // variables and constants
    const auto [key_len, pay_len, rec_len] = AlignRecord<Payload>(key_length, 0);
    const auto in_progress_meta = Metadata{key_len, rec_len};
    StatusWord cur_status;
    size_t target_pos;
    KeyExistence rc;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a null record
     *--------------------------------------------------------------------------------------------*/
    while (true) {
      cur_status = GetStatusWordProtected();
      if (cur_status.IsFrozen()) return kFrozen;

      // check whether a node includes a target key
      target_pos = cur_status.GetRecordCount();
      size_t exist_pos;
      std::tie(rc, exist_pos) = CheckUniqueness(key, target_pos);
      if (rc == kNotExist || rc == kDeleted) return kKeyNotExist;

      const auto target_meta = GetMetadataProtected(exist_pos);
      if constexpr (CanCASUpdate<Payload>()) {
        if (rc == kExist && exist_pos < sorted_count_) {
          const auto deleted_meta = target_meta.Delete();
          const auto deleted_size = kWordLength + target_meta.GetTotalLength();
          const auto new_status = cur_status.Delete(deleted_size);
          if (new_status.NeedConsolidation(sorted_count_)) return kNeedConsolidation;

          // delete a record directly
          MwCASDescriptor desc{};
          SetStatusForMwCAS(desc, cur_status, new_status);
          SetMetadataForMwCAS(desc, exist_pos, target_meta, deleted_meta);
          if (desc.MwCAS()) return kSuccess;
          continue;
        }
      }

      // prepare new status for MwCAS
      const auto deleted_size = (2 * kWordLength) + target_meta.GetTotalLength() + rec_len;
      const auto new_status = cur_status.Add(rec_len).Delete(deleted_size);
      if (new_status.NeedConsolidation(sorted_count_)) return kNeedConsolidation;

      // perform MwCAS to reserve space
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, cur_status, new_status);
      SetMetadataForMwCAS(desc, target_pos, Metadata{}, in_progress_meta);
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a null record
    auto offset = kPageSize - cur_status.GetBlockSize();
    offset = SetKey(offset, key, key_len);

    // prepare record metadata for MwCAS
    const auto deleted_meta = in_progress_meta.Delete(offset);

    // check concurrent SMOs
    while (true) {
      const auto status = GetStatusWordProtected();
      if (status.IsFrozen()) return kFrozen;

      // recheck uniqueness if required
      if (rc == kUncertain) {
        rc = CheckUniqueness(key, target_pos).first;
        if (rc == kNotExist || rc == kDeleted) {
          // delete an inserted record
          SetMetadata(target_pos, in_progress_meta.Delete());
          return kKeyNotExist;
        } else if (rc == kUncertain) {
          // retry if there are still uncertain records
          continue;
        }
      }

      // perform MwCAS to complete an insert
      MwCASDescriptor desc{};
      SetStatusForMwCAS(desc, status, status);
      SetMetadataForMwCAS(desc, target_pos, in_progress_meta, deleted_meta);
      if (desc.MwCAS()) break;
    }
    std::atomic_thread_fence(std::memory_order_release);

    return kSuccess;
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
  template <class Payload>
  void
  Consolidate(const Node *old_node)
  {
    // set a right-end flag if needed
    is_right_end_ = old_node->is_right_end_;

    // sort records in an unsorted region
    const auto [new_rec_num, records] = old_node->SortNewRecords();

    // perform merge-sort to consolidate a node
    const auto sorted_count = old_node->sorted_count_;
    auto offset = GetInitialOffset<Payload>();
    size_t rec_count = 0;

    size_t j = 0;
    for (size_t i = 0; i < sorted_count; ++i) {
      const auto meta = old_node->GetMetadataProtected(i);
      const auto key = old_node->GetKey(meta);

      // copy new records
      for (; j < new_rec_num; ++j) {
        auto [target_meta, target_key] = records[j];
        if (!Compare{}(target_key, key)) break;

        // check a new record is active
        if (target_meta.IsVisible()) {
          offset = CopyRecordFrom<Payload>(old_node, target_meta, rec_count++, offset);
        }
      }

      // check a new record is updated one
      if (j < new_rec_num && IsEqual<Compare>(key, records[j].key)) {
        const auto target_meta = records[j++].meta;
        if (target_meta.IsVisible()) {
          offset = CopyRecordFrom<Payload>(old_node, target_meta, rec_count++, offset);
        }
      } else if (meta.IsVisible()) {
        offset = CopyRecordFrom<Payload>(old_node, meta, rec_count++, offset);
      }
    }

    // move remaining new records
    for (; j < new_rec_num; ++j) {
      const auto target_meta = records[j].meta;
      if (target_meta.IsVisible()) {
        offset = CopyRecordFrom<Payload>(old_node, target_meta, rec_count++, offset);
      }
    }

    // set header information
    sorted_count_ = rec_count;
    SetStatus(StatusWord{rec_count, kPageSize - offset});
  }

  template <class Payload>
  static void
  Split(  //
      const Node *node,
      Node *l_node,
      Node *r_node)
  {
    // set a right-end flag
    r_node->is_right_end_ = node->is_right_end_;
    l_node->is_right_end_ = false;

    // copy records to a left node
    const auto rec_count = node->sorted_count_;
    const size_t l_count = rec_count / 2;
    auto l_offset = GetInitialOffset<Payload>();
    l_offset = l_node->CopyRecordsFrom<Payload>(node, 0, l_count, 0, l_offset);
    l_node->sorted_count_ = l_count;
    l_node->SetStatus(StatusWord{l_count, kPageSize - l_offset});

    // copy records to a right node
    auto r_offset = GetInitialOffset<Payload>();
    r_offset = r_node->CopyRecordsFrom<Payload>(node, l_count, rec_count, 0, r_offset);
    const auto r_count = rec_count - l_count;
    r_node->sorted_count_ = r_count;
    r_node->SetStatus(StatusWord{r_count, kPageSize - r_offset});
  }

  void
  InitAsSplitParent(  //
      const Node *old_node,
      const Node *l_child,
      const Node *r_child,
      const size_t l_pos)
  {
    // set a right-end flag if needed
    is_right_end_ = old_node->is_right_end_;

    // copy lower records
    auto offset = GetInitialOffset<Node *>();
    offset = CopyRecordsFrom<Node *>(old_node, 0, l_pos, 0, offset);

    // insert split nodes
    const auto l_meta = l_child->meta_array_[l_child->sorted_count_ - 1];
    offset = InsertChild(l_child, l_meta, l_child, l_pos, offset);
    const auto r_meta = r_child->meta_array_[r_child->sorted_count_ - 1];
    const auto r_pos = l_pos + 1;
    offset = InsertChild(r_child, r_meta, r_child, r_pos, offset);

    // copy upper records
    auto rec_count = old_node->sorted_count_;
    offset = CopyRecordsFrom<Node *>(old_node, r_pos, rec_count, r_pos + 1, offset);

    // set an updated header
    sorted_count_ = rec_count + 1;
    SetStatus(StatusWord{sorted_count_, kPageSize - offset});
  }

  void
  InitAsRoot(  //
      const Node *l_child,
      const Node *r_child)
  {
    // set a right-end flag
    is_right_end_ = true;

    // insert initial children
    const auto l_meta = l_child->meta_array_[l_child->sorted_count_ - 1];
    auto offset = GetInitialOffset<Node *>();
    offset = InsertChild(l_child, l_meta, l_child, 0, offset);
    const auto r_meta = r_child->meta_array_[r_child->sorted_count_ - 1];
    offset = InsertChild(r_child, r_meta, r_child, 1, offset);

    // set an updated header
    sorted_count_ = 2;
    SetStatus(StatusWord{2, kPageSize - offset});
  }

  template <class Payload>
  static void
  Merge(  //
      const Node *l_node,
      const Node *r_node,
      Node *merged_node)
  {
    // set a right-end flag
    merged_node->is_right_end_ = r_node->is_right_end_;

    // copy records in left/right nodes
    const auto l_count = l_node->sorted_count_;
    auto offset = GetInitialOffset<Payload>();
    if constexpr (std::is_same_v<Payload, Node *>) {
      offset = merged_node->CopyRecordsFrom<Payload>(l_node, 0, l_count, 0, offset);
    } else {  // when merging a leaf node, only update its offset
      offset = (l_count > 0) ? merged_node->meta_array_[l_count - 1].GetOffset() : offset;
    }
    const auto r_count = r_node->sorted_count_;
    offset = merged_node->CopyRecordsFrom<Payload>(r_node, 0, r_count, l_count, offset);

    // create a merged node
    const auto rec_count = l_count + r_count;
    merged_node->sorted_count_ = rec_count;
    merged_node->SetStatus(StatusWord{rec_count, kPageSize - offset});
  }

  void
  InitAsMergeParent(  //
      const Node *old_node,
      const Node *merged_child,
      const size_t position)
  {
    // set a right-end flag
    is_right_end_ = old_node->is_right_end_;

    // copy lower records
    auto offset = GetInitialOffset<Node *>();
    offset = CopyRecordsFrom<Node *>(old_node, 0, position, 0, offset);

    // insert a merged node
    const auto meta = merged_child->meta_array_[merged_child->sorted_count_ - 1];
    offset = InsertChild(merged_child, meta, merged_child, position, offset);

    // copy upper records
    const auto r_pos = position + 1;
    auto rec_count = old_node->sorted_count_;
    offset = CopyRecordsFrom<Node *>(old_node, r_pos + 1, rec_count, r_pos, offset);

    // set an updated header
    sorted_count_ = rec_count - 1;
    SetStatus(StatusWord{sorted_count_, kPageSize - offset});
  }

 private:
  /*################################################################################################
   * Internal classes
   *##############################################################################################*/

  /**
   * @brief A class to sort metadata.
   *
   */
  struct MetaKeyPair {
    Metadata meta{};
    Key key{};
  };

  /*################################################################################################
   * Internal getters
   *##############################################################################################*/

  /**
   * @brief Read metadata with MwCAS read protection.
   *
   * This function uses a MwCAS read operation internally, and so it is guaranteed that
   * read metadata is valid.
   *
   * @return Metadata: metadata.
   */
  auto
  GetMetadataProtected(const size_t position) const  //
      -> Metadata
  {
    return MwCASDescriptor::Read<Metadata>(&meta_array_[position]);
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return Key: a target key.
   */
  constexpr auto
  GetKey(const Metadata meta) const  //
      -> Key
  {
    if constexpr (std::is_pointer_v<Key>) {
      return GetKeyAddr(meta);
    } else {
      return *GetKeyAddr(meta);
    }
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return auto: an address of a target key.
   */
  constexpr auto
  GetKeyAddr(const Metadata meta) const
  {
    if constexpr (std::is_pointer_v<Key>) {
      return Cast<Key>(ShiftAddr(this, meta.GetOffset()));
    } else {
      return Cast<Key *>(ShiftAddr(this, meta.GetOffset()));
    }
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return void*: an address of a target payload.
   */
  constexpr auto
  GetPayloadAddr(const Metadata meta) const  //
      -> void *
  {
    return ShiftAddr(this, meta.GetOffset() + meta.GetKeyLength());
  }

  /*################################################################################################
   * Internal setters
   *##############################################################################################*/

  /**
   * @brief Set a status word atomically.
   *
   * @param status a status word.
   */
  void
  SetStatus(const StatusWord status)
  {
    auto *atomic_stat = reinterpret_cast<std::atomic<StatusWord> *>(&status_);
    atomic_stat->store(status, std::memory_order_release);
  }

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
   * @brief Set a target key.
   *
   * @param offset an offset to set a target key.
   * @param key a target key to be set.
   * @param key_length the length of a target key.
   */
  auto
  SetKey(  //
      size_t offset,
      const Key &key,
      const size_t key_length)  //
      -> size_t
  {
    offset -= key_length;
    if constexpr (IsVariableLengthData<Key>()) {
      memcpy(ShiftAddr(this, offset), key, key_length);
    } else {
      auto *key_addr = reinterpret_cast<Key *>(ShiftAddr(this, offset));
      *key_addr = key;
    }

    return offset;
  }

  /**
   * @brief Set a target payload directly.
   *
   * @tparam T a class of a target payload.
   * @param offset an offset to set a target payload.
   * @param payload a target payload to be set.
   * @param payload_length the length of a target payload.
   */
  template <class Payload>
  auto
  SetPayload(  //
      size_t offset,
      const T &payload,
      const size_t payload_length)  //
      -> size_t
  {
    offset -= payload_length;
    if constexpr (IsVariableLengthData<Payload>()) {
      memcpy(ShiftAddr(this, offset), payload, payload_length);
    } else {
      auto *pay_addr = reinterpret_cast<Payload *>(ShiftAddr(this, offset));
      *pay_addr = payload;
    }

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

    const auto old_payload = MwCASDescriptor::Read<Payload>(GetPayloadAddr(meta));
    desc.AddMwCASTarget(GetPayloadAddr(meta), old_payload, new_payload);
  }

  /*################################################################################################
   * Internal functions for record alignment
   *##############################################################################################*/

  /**
   * @brief Compute the maximum number of records in a node.
   *
   * @return size_t the expected maximum number of records.
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

  template <class Payload>
  static constexpr auto
  GetInitialOffset()  //
      -> size_t
  {
    if constexpr (IsVariableLengthData<Payload>() || IsVariableLengthData<Key>()) {
      return kPageSize;
    } else if constexpr (alignof(Key) <= alignof(Payload)) {
      return kPageSize;
    } else {
      constexpr size_t kAlignLen = alignof(Key) - sizeof(Payload) % alignof(Key);
      if constexpr (kAlignLen == alignof(Key)) {
        return kPageSize;
      } else {
        return kPageSize - kAlignLen;
      }
    }
  }

  template <class Payload>
  static constexpr auto
  AlignRecord(  //
      size_t key_len,
      size_t pay_len)  //
      -> std::tuple<size_t, size_t, size_t>
  {
    if constexpr (IsVariableLengthData<Key>() && IsVariableLengthData<Payload>()) {
      // record alignment is not required
      return {key_len, pay_len, key_len + pay_len};
    } else if constexpr (IsVariableLengthData<Key>()) {
      // dynamic alignment is required
      const size_t align_len = alignof(Payload) - key_len % alignof(Payload);
      if (align_len == alignof(Payload)) {
        // alignment is not required
        return {key_len, pay_len, key_len + pay_len};
      }
      return {key_len, pay_len, align_len + key_len + pay_len};
    } else if constexpr (IsVariableLengthData<Payload>()) {
      const size_t align_len = alignof(Key) - pay_len % alignof(Key);
      if (align_len != alignof(Key)) {
        // dynamic alignment is required
        key_len += align_len;
      }
      return {key_len, pay_len, key_len + pay_len};
    } else if constexpr (alignof(Key) < alignof(Payload)) {
      constexpr size_t kAlignLen = alignof(Payload) - sizeof(Key) % alignof(Payload);
      if constexpr (kAlignLen == alignof(Payload)) {
        // alignment is not required
        return {key_len, pay_len, key_len + pay_len};
      } else {
        // fixed-length alignment is required
        constexpr size_t kKeyLen = sizeof(Key) + kAlignLen;
        return {kKeyLen, pay_len, kKeyLen + pay_len};
      }
    } else if constexpr (alignof(Key) > alignof(Payload)) {
      constexpr size_t kAlignLen = alignof(Key) - sizeof(Payload) % alignof(Key);
      if constexpr (kAlignLen == alignof(Key)) {
        // alignment is not required
        return {key_len, pay_len, key_len + pay_len};
      } else {
        // fixed-length alignment is required
        constexpr size_t kPayLen = sizeof(Payload) + kAlignLen;
        return {key_len, kPayLen, key_len + kPayLen};
      }
    } else {
      // alignment is not required
      return {key_len, pay_len, key_len + pay_len};
    }
  }

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

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
    int64_t end_pos = sorted_count_ - 1;
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
    // perform a linear search in revese order
    for (int64_t pos = begin_pos; pos >= sorted_count_; --pos) {
      const auto meta = GetMetadataProtected(pos);
      if (meta.IsInProgress()) {
        if (meta.IsVisible()) return {kUncertain, pos};
        continue;
      }

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
  template <class Payload>
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

  auto
  InsertChild(  //
      const Node *orig_node,
      const Metadata orig_meta,
      const Node *child_node,
      const size_t rec_count,
      const size_t offset)  //
      -> size_t
  {
    const auto key = orig_node->GetKey(orig_meta);
    const auto key_length = orig_meta.GetKeyLength();
    const auto [key_len, pay_len, rec_len] = AlignRecord<Node *>(key_length, sizeof(Node *));

    auto tmp_offset = SetPayload(offset, child_node, pay_len);
    tmp_offset = SetKey(tmp_offset, key, key_len);
    meta_array_[rec_count] = Metadata{tmp_offset, key_len, rec_len};

    return offset - rec_len;
  }

  template <class Payload>
  auto
  CopyRecordFrom(  //
      const Node *orig_node,
      const Metadata target_meta,
      const size_t rec_count,
      size_t offset)  //
      -> size_t
  {
    if constexpr (IsVariableLengthData<Key> && !IsVariableLengthData<Payload>) {
      // record's offset is different its aligned position
      const auto aligned_rec_len = target_meta.GetTotalLength();
      offset -= aligned_rec_len;

      const auto pad_len = aligned_rec_len - (target_meta.GetKeyLength() + sizeof(Payload));
      auto *src_addr = ShiftAddr(orig_node->GetKeyAddr(target_meta), -pad_len);
      memcpy(ShiftAddr(this, offset), src_addr, aligned_rec_len);
    } else {
      // copy a record from the given node
      const auto rec_len = target_meta.GetTotalLength();
      offset -= rec_len;
      memcpy(ShiftAddr(this, offset), orig_node->GetKeyAddr(target_meta), rec_len);
    }

    // set new metadata
    meta_array_[rec_count] = target_meta.UpdateOffset(offset);

    return offset;
  }

  template <class Payload>
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
      const auto target_meta = orig_node->meta_array_[i];
      offset = CopyRecordFrom<Payload>(orig_node, target_meta, rec_count++, offset);
    }

    return offset;
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
      -> std::pair<size_t, std::array<MetaKeyPair, kMaxUnsortedRecNum>>
  {
    const int64_t rec_count = GetStatusWordProtected().GetRecordCount();

    std::array<MetaKeyPair, kMaxUnsortedRecNum> arr;
    size_t count = 0;

    // sort unsorted records by insertion sort
    for (int64_t index = rec_count - 1; index >= sorted_count_; --index) {
      // check whether a record has been inserted
      const auto meta = GetMetadataProtected(index);
      if (meta.IsInProgress()) continue;

      // search an inserting position
      const auto cur_key = GetKey(meta);
      size_t i = 0;
      for (; i < count; ++i) {
        if (!Compare{}(arr[i].key, cur_key)) break;
      }

      // shift upper records if needed
      if (i < count) {
        if (!Compare{}(cur_key, arr[i].key)) continue;  // there is a duplicate key
        memmove(&(arr[i + 1]), &(arr[i]), sizeof(MetaKeyPair) * (count - i));
      }

      // insert a new record
      arr[i] = MetaKeyPair{meta, cur_key};
      ++count;
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
  StatusWord status_{};

  /// the head of a metadata array.
  Metadata meta_array_[0];
};

}  // namespace dbgroup::index::bztree::component

#endif  // BZTREE_COMPONENT_NODE_HPP
