// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <functional>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "base_node.hpp"
#include "record.hpp"

namespace dbgroup::index::bztree
{
template <class Key, class Payload, class Compare = std::less<Key>>
class LeafNode : public BaseNode<Key, Payload, Compare>
{
  using KeyExistence = typename BaseNode<Key, Payload, Compare>::KeyExistence;
  using NodeReturnCode = typename BaseNode<Key, Payload, Compare>::NodeReturnCode;
  using Record_t = Record<Key, Payload>;

 private:
  /*################################################################################################
   * Internal structs to cpmare key & metadata pairs
   *##############################################################################################*/

  struct PairComp {
    PairComp() {}

    bool
    operator()(  //
        const std::pair<Key, Metadata> &a,
        const std::pair<Key, Metadata> &b) const noexcept
    {
      return Compare{}(a.first, b.first);
    }
  };

  struct PairEqual {
    PairEqual() {}

    bool
    operator()(  //
        const std::pair<Key, Metadata> &a,
        const std::pair<Key, Metadata> &b) const noexcept
    {
      return IsEqual<Compare>(a.first, b.first);
    }
  };

  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  explicit LeafNode(const size_t node_size) : BaseNode<Key, Payload, Compare>(node_size, true) {}

  /*################################################################################################
   * Internal setter/getter
   *##############################################################################################*/

  std::unique_ptr<Record_t>
  GetRecord(const Metadata meta) const
  {
    const auto key_addr = this->GetKeyAddr(meta);
    const auto key_length = meta.GetKeyLength();
    const auto payload_length = meta.GetPayloadLength();

    return Record_t::Create(key_addr, key_length, payload_length);
  }

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  KeyExistence
  SearchUnsortedMetaToWrite(  //
      const Key &key,
      const int64_t begin_index,
      const int64_t sorted_count,
      const size_t index_epoch) const
  {
    // perform a linear search in revese order
    for (int64_t index = begin_index; index >= sorted_count; --index) {
      const auto meta = this->GetMetadata(index);
      const auto target_key = CastKey<Key>(this->GetKeyAddr(meta));
      if (IsEqual<Compare>(key, target_key)) {
        if (meta.IsVisible()) {
          return KeyExistence::kExist;
        } else if (meta.IsDeleted()) {
          return KeyExistence::kDeleted;
        } else if (!meta.IsCorrupted(index_epoch)) {
          // there is in progress records
          return KeyExistence::kUncertain;
        }
        // there is a key, but it is corrupted due to a machine failure
      }
    }
    return KeyExistence::kNotExist;
  }

  KeyExistence
  CheckUniqueness(  //
      const Key &key,
      const int64_t record_count,
      const size_t index_epoch) const
  {
    const auto existence =
        SearchUnsortedMetaToWrite(key, record_count - 1, this->GetSortedCount(), index_epoch);
    if (existence == KeyExistence::kNotExist) {
      // there is no key in unsorted metadata, so search a sorted region
      return this->SearchSortedMetadata(key, true).first;
    } else {
      return existence;
    }
  }

  std::pair<KeyExistence, size_t>
  SearchUnsortedMetaToRead(  //
      const Key &key,
      const int64_t end_index,
      const int64_t record_count) const
  {
    for (int64_t index = record_count - 1; index >= end_index; --index) {
      const auto meta = this->GetMetadata(index);
      const auto target_key = CastKey<Key>(this->GetKeyAddr(meta));
      if (IsEqual<Compare>(key, target_key)) {
        if (meta.IsVisible()) {
          return {KeyExistence::kExist, index};
        } else if (meta.IsDeleted()) {
          return {KeyExistence::kDeleted, index};
        }
        // there is a key, but it is in inserting or corrupted
      }
    }
    return {KeyExistence::kNotExist, 0};
  }

  std::pair<KeyExistence, size_t>
  SearchMetadataToRead(  //
      const Key &key,
      const size_t record_count) const
  {
    const auto [existence, index] =
        SearchUnsortedMetaToRead(key, this->GetSortedCount(), record_count);
    if (existence == KeyExistence::kExist || existence == KeyExistence::kDeleted) {
      return {existence, index};
    } else {
      return this->SearchSortedMetadata(key, true);
    }
  }

  static void
  CopyRecordsViaMetadata(  //
      LeafNode *copied_node,
      const LeafNode *original_node,
      const typename std::vector<std::pair<Key, Metadata>>::const_iterator begin_iter,
      const typename std::vector<std::pair<Key, Metadata>>::const_iterator end_iter)
  {
    const auto node_size = original_node->GetNodeSize();

    auto record_count = copied_node->GetSortedCount();
    auto offset = node_size - copied_node->GetStatusWord().GetBlockSize();
    for (auto iter = begin_iter; iter != end_iter; ++record_count, ++iter) {
      // copy a record
      const auto [key, meta] = *iter;
      offset = copied_node->CopyRecord(original_node, meta, offset);
      // copy metadata
      const auto new_meta = meta.UpdateOffset(offset);
      copied_node->SetMetadata(record_count, new_meta);
    }
    copied_node->status_ = StatusWord{}.AddRecordInfo(record_count, node_size - offset, 0);
    copied_node->sorted_count_ = record_count;
  }

 public:
  /*################################################################################################
   * Public constructor/destructor
   *##############################################################################################*/

  ~LeafNode() = default;

  LeafNode(const LeafNode &) = delete;
  LeafNode &operator=(const LeafNode &) = delete;
  LeafNode(LeafNode &&) = default;
  LeafNode &operator=(LeafNode &&) = default;

  /*################################################################################################
   * Public builders
   *##############################################################################################*/

  static LeafNode *
  CreateEmptyNode(const size_t node_size)
  {
    assert((node_size % kWordLength) == 0);

    auto page = calloc(1, node_size);
    auto new_node = new (page) LeafNode{node_size};
    return new_node;
  }

  /*################################################################################################
   * Read operations
   *##############################################################################################*/

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @return std::pair<NodeReturnCode, std::unique_ptr<std::byte[]>>
   */
  std::pair<NodeReturnCode, std::unique_ptr<Record_t>>
  Read(const Key &key)
  {
    const auto status = this->GetStatusWordProtected();
    const auto [existence, index] = SearchMetadataToRead(key, status.GetRecordCount());
    if (existence == KeyExistence::kNotExist || existence == KeyExistence::kDeleted) {
      return {NodeReturnCode::kKeyNotExist, nullptr};
    } else {
      const auto meta = this->GetMetadata(index);
      return {NodeReturnCode::kSuccess, GetRecord(meta)};
    }
  }

  /**
   * @brief
   *
   * @tparam Compare
   * @param begin_key
   * @param begin_is_closed
   * @param end_key
   * @param end_is_closed
   * @return std::pair<NodeReturnCode,
   *         std::vector<std::pair<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>>>>
   */
  std::pair<NodeReturnCode, std::vector<std::unique_ptr<Record_t>>>
  Scan(  //
      const Key *begin_key,
      const bool begin_is_closed,
      const Key *end_key,
      const bool end_is_closed)
  {
    const auto status = this->GetStatusWordProtected();
    const int64_t record_count = status.GetRecordCount();
    const int64_t sorted_count = this->GetSortedCount();

    // gather valid (live or deleted) records
    std::vector<std::pair<Key, Metadata>> meta_arr;
    meta_arr.reserve(record_count);

    // search unsorted metadata in reverse order
    for (int64_t index = record_count - 1; index >= sorted_count; --index) {
      const auto meta = this->GetMetadata(index);
      const auto key = CastKey<Key>(this->GetKeyAddr(meta));
      if (IsInRange<Compare>(key, begin_key, begin_is_closed, end_key, end_is_closed)
          && (meta.IsVisible() || meta.IsDeleted())) {
        meta_arr.emplace_back(key, meta);
      }
    }

    // search sorted metadata
    const auto begin_index =
        (begin_key == nullptr) ? 0 : this->SearchSortedMetadata(*begin_key, begin_is_closed).second;
    for (int64_t index = begin_index; index < sorted_count; ++index) {
      const auto meta = this->GetMetadata(index);
      const auto key = CastKey<Key>(this->GetKeyAddr(meta));
      if (IsInRange<Compare>(key, begin_key, begin_is_closed, end_key, end_is_closed)) {
        meta_arr.emplace_back(key, meta);
        if (end_key != nullptr && end_is_closed && IsEqual<Compare>(key, *end_key)) {
          break;  // a current key is end of range condition
        }
      } else {
        break;  // a current key is out of range condition
      }
    }

    // make unique with keeping the order of writes
    std::stable_sort(meta_arr.begin(), meta_arr.end(), PairComp{});
    auto end_iter = std::unique(meta_arr.begin(), meta_arr.end(), PairEqual{});
    meta_arr.erase(end_iter, meta_arr.end());

    // copy live records for return
    std::vector<std::unique_ptr<Record_t>> scan_results;
    scan_results.reserve(meta_arr.size());
    for (auto &&[key, meta] : meta_arr) {
      if (meta.IsVisible()) {
        scan_results.emplace_back(GetRecord(meta));
      }
    }
    return {NodeReturnCode::kSuccess, std::move(scan_results)};
  }

  /*################################################################################################
   * Write operations
   *##############################################################################################*/

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param payload
   * @param payload_length
   * @param descriptor_pool
   * @return NodeReturnCode
   */
  std::pair<NodeReturnCode, StatusWord>
  Write(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload,
      const size_t payload_length,
      const size_t index_epoch = 0)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord current_status;
    size_t record_count;
    const auto total_length = key_length + payload_length;
    const auto inserting_meta = Metadata::GetInsertingMeta(index_epoch);

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to write a record
     *--------------------------------------------------------------------------------------------*/
    bool mwcas_success;
    do {
      current_status = this->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      if (current_status.GetOccupiedSize() + kWordLength + total_length > this->GetNodeSize()) {
        return {NodeReturnCode::kNoSpace, StatusWord{}};
      }

      // prepare for MwCAS
      record_count = current_status.GetRecordCount();
      const auto new_status = current_status.AddRecordInfo(1, total_length, 0);
      const auto current_meta = this->GetMetadata(record_count);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      this->SetStatusForMwCAS(desc, current_status, new_status);
      this->SetMetadataForMwCAS(desc, record_count, current_meta, inserting_meta);
      mwcas_success = desc.MwCAS();
    } while (!mwcas_success);

    /*----------------------------------------------------------------------------------------------
     * Phase 2: write a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = this->GetNodeSize() - current_status.GetBlockSize();
    offset = this->SetRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent SMOs)
    do {
      current_status = this->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      // perform MwCAS to complete a write
      auto desc = MwCASDescriptor{};
      this->SetStatusForMwCAS(desc, current_status, current_status);
      this->SetMetadataForMwCAS(desc, record_count, inserting_meta, inserted_meta);
      mwcas_success = desc.MwCAS();
    } while (!mwcas_success);

    return {NodeReturnCode::kSuccess, current_status};
  }

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param payload
   * @param payload_length
   * @param descriptor_pool
   * @return NodeReturnCode
   */
  std::pair<NodeReturnCode, StatusWord>
  Insert(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload,
      const size_t payload_length,
      const size_t index_epoch = 0)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord current_status;
    size_t record_count;
    const auto total_length = key_length + payload_length;
    const auto inserting_meta = Metadata::GetInsertingMeta(index_epoch);

    // local flags for insertion
    auto uniqueness = KeyExistence::kNotExist;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    bool mwcas_success;
    do {
      current_status = this->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      record_count = current_status.GetRecordCount();
      if (uniqueness != KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(key, record_count, index_epoch);
        if (uniqueness == KeyExistence::kExist) {
          return {NodeReturnCode::kKeyExist, StatusWord{}};
        }
      }

      if (current_status.GetOccupiedSize() + kWordLength + total_length > this->GetNodeSize()) {
        return {NodeReturnCode::kNoSpace, StatusWord{}};
      }

      // prepare new status for MwCAS
      const auto new_status = current_status.AddRecordInfo(1, total_length, 0);

      // get current metadata for MwCAS
      const auto current_meta = this->GetMetadata(record_count);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      this->SetStatusForMwCAS(desc, current_status, new_status);
      this->SetMetadataForMwCAS(desc, record_count, current_meta, inserting_meta);
      mwcas_success = desc.MwCAS();

      if (!mwcas_success) {
        uniqueness = KeyExistence::kUncertain;
      }
    } while (!mwcas_success);

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = this->GetNodeSize() - current_status.GetBlockSize();
    offset = this->SetRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent inserts and SMOs)
    do {
      if (uniqueness == KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(key, record_count, index_epoch);
        if (uniqueness == KeyExistence::kExist) {
          // delete an inserted record
          this->SetMetadata(record_count, inserting_meta.UpdateOffset(0));
          return {NodeReturnCode::kKeyExist, StatusWord{}};
        }
        continue;  // recheck
      }

      current_status = this->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      // perform MwCAS to complete an insert
      auto desc = MwCASDescriptor{};
      this->SetStatusForMwCAS(desc, current_status, current_status);
      this->SetMetadataForMwCAS(desc, record_count, inserting_meta, inserted_meta);
      mwcas_success = desc.MwCAS();
    } while (!mwcas_success);

    return {NodeReturnCode::kSuccess, current_status};
  }

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param payload
   * @param payload_length
   * @param descriptor_pool
   * @return NodeReturnCode
   */
  std::pair<NodeReturnCode, StatusWord>
  Update(  //
      const Key &key,
      const size_t key_length,
      const Payload &payload,
      const size_t payload_length,
      const size_t index_epoch = 0)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord current_status;
    size_t record_count;
    const auto total_length = key_length + payload_length;
    const auto inserting_meta = Metadata::GetInsertingMeta(index_epoch);

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    bool mwcas_success;
    do {
      current_status = this->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      record_count = current_status.GetRecordCount();
      const auto [existence, updated_index] = SearchMetadataToRead(key, record_count);
      if (existence == KeyExistence::kNotExist || existence == KeyExistence::kDeleted) {
        return {NodeReturnCode::kKeyNotExist, StatusWord{}};
      }

      if (current_status.GetOccupiedSize() + kWordLength + total_length > this->GetNodeSize()) {
        return {NodeReturnCode::kNoSpace, StatusWord{}};
      }

      // prepare new status for MwCAS
      const auto updated_meta = this->GetMetadata(updated_index);
      const auto deleted_size = kWordLength + updated_meta.GetTotalLength();
      const auto new_status = current_status.AddRecordInfo(1, total_length, deleted_size);

      // get current metadata for MwCAS
      const auto current_meta = this->GetMetadata(record_count);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      this->SetStatusForMwCAS(desc, current_status, new_status);
      this->SetMetadataForMwCAS(desc, record_count, current_meta, inserting_meta);
      mwcas_success = desc.MwCAS();
    } while (!mwcas_success);

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = this->GetNodeSize() - current_status.GetBlockSize();
    offset = this->SetRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent SMOs)
    do {
      current_status = this->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      // perform MwCAS to complete an update
      auto desc = MwCASDescriptor{};
      this->SetStatusForMwCAS(desc, current_status, current_status);
      this->SetMetadataForMwCAS(desc, record_count, inserting_meta, inserted_meta);
      mwcas_success = desc.MwCAS();
    } while (!mwcas_success);

    return {NodeReturnCode::kSuccess, current_status};
  }

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param descriptor_pool
   * @return NodeReturnCode
   */
  std::pair<NodeReturnCode, StatusWord>
  Delete(  //
      const Key &key,
      const size_t key_length)
  {
    // variables and constants
    StatusWord new_status;

    bool mwcas_success;
    do {
      const auto current_status = this->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, StatusWord{}};
      }

      const auto record_count = current_status.GetRecordCount();
      const auto [existence, index] = SearchMetadataToRead(key, record_count);
      if (existence == KeyExistence::kNotExist || existence == KeyExistence::kDeleted) {
        return {NodeReturnCode::kKeyNotExist, StatusWord{}};
      }

      // delete payload infomation from metadata
      const auto current_meta = this->GetMetadata(index);
      const auto deleted_meta = current_meta.DeleteRecordInfo();

      // prepare new status
      const auto deleted_block_size = kWordLength + key_length + current_meta.GetPayloadLength();
      new_status = current_status.AddRecordInfo(0, 0, deleted_block_size);

      // perform MwCAS to reserve space
      auto desc = MwCASDescriptor{};
      this->SetStatusForMwCAS(desc, current_status, new_status);
      this->SetMetadataForMwCAS(desc, index, current_meta, deleted_meta);
      mwcas_success = desc.MwCAS();
    } while (!mwcas_success);

    return {NodeReturnCode::kSuccess, new_status};
  }

  /*################################################################################################
   * Public structure modification operations
   *##############################################################################################*/

  static LeafNode *
  Consolidate(  //
      const LeafNode *target_node,
      const std::vector<std::pair<Key, Metadata>> &live_meta)
  {
    // create a new node and copy records
    auto new_node = CreateEmptyNode(target_node->GetNodeSize());
    CopyRecordsViaMetadata(new_node, target_node, live_meta.begin(), live_meta.end());

    return new_node;
  }

  static std::pair<LeafNode *, LeafNode *>
  Split(  //
      const LeafNode *target_node,
      const std::vector<std::pair<Key, Metadata>> &sorted_meta,
      const size_t left_record_count)
  {
    const auto node_size = target_node->GetNodeSize();
    const auto split_iter = sorted_meta.begin() + left_record_count;

    // create a split left node
    auto left_node = CreateEmptyNode(node_size);
    CopyRecordsViaMetadata(left_node, target_node, sorted_meta.begin(), split_iter);

    // create a split right node
    auto right_node = CreateEmptyNode(node_size);
    CopyRecordsViaMetadata(right_node, target_node, split_iter, sorted_meta.end());

    return {left_node, right_node};
  }

  static LeafNode *
  Merge(  //
      const LeafNode *target_node,
      const std::vector<std::pair<Key, Metadata>> &this_meta,
      const LeafNode *sibling_node,
      const std::vector<std::pair<Key, Metadata>> &sibling_meta,
      const bool sibling_is_left)
  {
    // create a merged node
    auto merged_node = CreateEmptyNode(target_node->GetNodeSize());
    if (sibling_is_left) {
      CopyRecordsViaMetadata(merged_node, sibling_node, sibling_meta.begin(), sibling_meta.end());
      CopyRecordsViaMetadata(merged_node, target_node, this_meta.begin(), this_meta.end());
    } else {
      CopyRecordsViaMetadata(merged_node, target_node, this_meta.begin(), this_meta.end());
      CopyRecordsViaMetadata(merged_node, sibling_node, sibling_meta.begin(), sibling_meta.end());
    }

    return merged_node;
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  std::vector<std::pair<Key, Metadata>>
  GatherSortedLiveMetadata() const
  {
    const auto record_count = this->GetStatusWord().GetRecordCount();
    const int64_t sorted_count = this->GetSortedCount();

    // gather valid (live or deleted) records
    std::vector<std::pair<Key, Metadata>> meta_arr;
    meta_arr.reserve(record_count);

    // search unsorted metadata in reverse order
    for (int64_t index = record_count - 1; index >= sorted_count; --index) {
      const auto meta = this->GetMetadata(index);
      if (meta.IsVisible() || meta.IsDeleted()) {
        const auto key = CastKey<Key>(this->GetKeyAddr(meta));
        meta_arr.emplace_back(key, meta);
      } else {
        // there is a key, but it is in inserting or corrupted.
        // NOTE: we can ignore inserting records because concurrent writes are aborted due to SMOs.
      }
    }

    // search sorted metadata
    for (int64_t index = 0; index < sorted_count; ++index) {
      const auto meta = this->GetMetadata(index);
      const auto key = CastKey<Key>(this->GetKeyAddr(meta));
      meta_arr.emplace_back(key, meta);
    }

    // make unique with keeping the order of writes
    std::stable_sort(meta_arr.begin(), meta_arr.end(), PairComp{});
    auto end_iter = std::unique(meta_arr.begin(), meta_arr.end(), PairEqual{});

    // gather live records
    end_iter = std::remove_if(meta_arr.begin(), end_iter,
                              [](auto &obj) { return obj.second.IsDeleted(); });
    meta_arr.erase(end_iter, meta_arr.end());

    return meta_arr;
  }
};

}  // namespace dbgroup::index::bztree
