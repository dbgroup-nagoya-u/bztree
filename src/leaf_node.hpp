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

namespace bztree
{
template <class Key, class Payload, class Compare = std::less<Key>>
class LeafNode : public BaseNode<Key, Payload, Compare>
{
  using KeyExistence = BaseNode<Key, Payload, Compare>::KeyExistence;
  using NodeReturnCode = BaseNode<Key, Payload, Compare>::NodeReturnCode;

 private:
  /*################################################################################################
   * Internal structs to cpmare key & metadata pairs
   *##############################################################################################*/

  struct PairComp {
    Compare comp;

    explicit PairComp(Compare comparator) : comp{comparator} {}

    bool
    operator()(  //
        std::pair<Key, Metadata> a,
        std::pair<Key, Metadata> b) const noexcept
    {
      return comp(a.first, b.first);
    }
  };

  struct PairEqual {
    Compare comp;

    explicit PairEqual(Compare comparator) : comp{comparator} {}

    bool
    operator()(  //
        std::pair<Key, Metadata> a,
        std::pair<Key, Metadata> b) const noexcept
    {
      return IsEqual(a.first, b.first, comp);
    }
  };

  /*################################################################################################
   * Internal constructors
   *##############################################################################################*/

  explicit LeafNode(const size_t node_size) : BaseNode<Key, Payload, Compare>(node_size, true) {}

  /*################################################################################################
   * Internal setter/getter
   *##############################################################################################*/

  std::unique_ptr<Key>
  GetCopiedKey(const Metadata meta)
  {
    const auto key_ptr = this->GetKeyPtr(meta);
    const auto key_length = meta.GetKeyLength();
    auto raw_key = static_cast<Key>(malloc(key_length));
    memcpy(raw_key, key_ptr, key_length);
    return std::unique_ptr<Key>(raw_key);
  }

  std::unique_ptr<Payload>
  GetCopiedPayload(const Metadata meta)
  {
    const auto payload_ptr = this->GetPayloadPtr(meta);
    const auto payload_length = meta.GetPayloadLength();
    auto raw_payload = static_cast<Payload>(malloc(payload_length));
    memcpy(raw_payload, payload_ptr, payload_length);
    return std::unique_ptr<Payload>(raw_payload);
  }

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  KeyExistence
  SearchUnsortedMetaToWrite(  //
      const Key key,
      const size_t begin_index,
      const size_t sorted_count,
      const size_t index_epoch,
      Compare comp)
  {
    // perform a linear search in revese order
    for (size_t index = begin_index; index >= sorted_count; --index) {
      const auto meta = this->GetMetadataProtected(index);
      if (IsEqual(key, GetKeyPtr(meta), comp)) {
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
      const Key key,
      const size_t record_count,
      const size_t index_epoch,
      Compare comp)
  {
    const auto existence =
        SearchUnsortedMetaToWrite(key, record_count - 1, this->GetSortedCount(), index_epoch, comp);
    if (existence == KeyExistence::kNotExist) {
      // there is no key in unsorted metadata, so search a sorted region
      return SearchSortedMetadata(key, true, comp).first;
    } else {
      return existence;
    }
  }

  std::pair<KeyExistence, size_t>
  SearchUnsortedMetaToRead(  //
      const Key key,
      const int64_t end_index,
      const size_t record_count,
      Compare comp)
  {
    for (int64_t index = record_count - 1; index >= end_index; --index) {
      const auto meta = this->GetMetadata(index);
      if (IsEqual(key, GetKeyPtr(meta), comp)) {
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
      const Key key,
      const size_t record_count,
      Compare comp)
  {
    const auto [existence, index] =
        SearchUnsortedMetaToRead(key, this->GetSortedCount(), record_count, comp);
    if (existence == KeyExistence::kExist) {
      return {existence, index};
    } else {
      return SearchSortedMetadata(key, true, comp);
    }
  }

  std::vector<std::pair<std::byte *, Metadata>>::const_iterator
  CopyRecordsViaMetadata(  //
      LeafNode *original_node,
      std::vector<std::pair<std::byte *, Metadata>>::const_iterator meta_iter,
      const size_t record_count)
  {
    auto offset = this->GetNodeSize();
    for (size_t index = 0; index < record_count; ++index) {
      const auto meta = meta_iter->second;
      // copy a record
      const auto key = meta_iter->first;
      const auto key_length = meta.GetKeyLength();
      const auto payload = original_node->GetPayloadPtr(meta);
      const auto payload_length = meta.GetPayloadLength();
      offset = CopyRecord(key, key_length, payload, payload_length, offset);
      // copy metadata
      const auto new_meta = meta.UpdateOffset(offset);
      this->SetMetadata(index, new_meta);
      // get a next capied metadata
      ++meta_iter;
    }
    this->SetStatusWord(
        kInitStatusWord.AddRecordInfo(record_count, this->GetNodeSize() - offset, 0));
    this->SetSortedCount(record_count);

    return meta_iter;
  }

 public:
  /*################################################################################################
   * Public constructor/destructor
   *##############################################################################################*/

  LeafNode(const LeafNode &) = delete;
  LeafNode &operator=(const LeafNode &) = delete;
  LeafNode(LeafNode &&) = default;
  LeafNode &operator=(LeafNode &&) = default;
  ~LeafNode() = default;

  /*################################################################################################
   * Public builders
   *##############################################################################################*/

  // template <class Key, class Payload, class Compare = std::less<Key>>
  static LeafNode *
  CreateEmptyNode(const size_t node_size)
  {
    assert((node_size % kWordLength) == 0);

    auto aligned_page = aligned_alloc(kWordLength, node_size);
    auto new_node = new (aligned_page) LeafNode{node_size};
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
   * @param comp
   * @return std::pair<NodeReturnCode, std::unique_ptr<std::byte[]>>
   */
  std::pair<NodeReturnCode, std::unique_ptr<Payload>>
  Read(  //
      const Key key,
      Compare comp)
  {
    const auto status = this->GetStatusWord();
    const auto [existence, index] = SearchMetadataToRead(key, status.GetRecordCount(), comp);
    if (existence == KeyExistence::kNotExist) {
      return {NodeReturnCode::kKeyNotExist, nullptr};
    } else {
      const auto meta = this->GetMetadata(index);
      return {NodeReturnCode::kSuccess, GetCopiedPayload<Payload>(meta)};
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
   * @param comp
   * @return std::pair<NodeReturnCode,
   *         std::vector<std::pair<std::unique_ptr<std::byte[]>, std::unique_ptr<std::byte[]>>>>
   */
  std::pair<NodeReturnCode, std::vector<std::pair<std::unique_ptr<Key>, std::unique_ptr<Payload>>>>
  Scan(  //
      const Key begin_key,
      const bool begin_is_closed,
      const Key end_key,
      const bool end_is_closed,
      Compare comp)
  {
    const auto status = this->GetStatusWord();
    const auto record_count = this->GetStatusWord().GetRecordCount();
    const auto sorted_count = this->GetSortedCount();

    // gather valid (live or deleted) records
    std::vector<std::pair<Key, Metadata>> meta_arr;
    meta_arr.reserve(record_count);

    // search unsorted metadata in reverse order
    for (size_t index = record_count - 1; index >= sorted_count; --index) {
      const auto meta = this->GetMetadata(index);
      if (IsInRange(GetKeyPtr(meta), begin_key, begin_is_closed, end_key, end_is_closed, comp)
          && (meta.IsVisible() || meta.IsDeleted())) {
        meta_arr.emplace_back(GetKeyPtr(meta), meta);
      } else {
        // there is a key, but it is in inserting or corrupted.
      }
    }

    // search sorted metadata
    auto return_code = NodeReturnCode::kScanInProgress;
    const auto begin_index = SearchSortedMetadata(begin_key, begin_is_closed, comp).second;
    for (size_t index = begin_index; index < sorted_count; ++index) {
      const auto meta = this->GetMetadata(index);
      if (IsInRange(GetKeyPtr(meta), begin_key, begin_is_closed, end_key, end_is_closed, comp)) {
        meta_arr.emplace_back(GetKeyPtr(meta), meta);
      } else {
        // a current key is out of range condition
        return_code = NodeReturnCode::kSuccess;
        break;
      }
    }

    // make unique with keeping the order of writes
    std::stable_sort(meta_arr.begin(), meta_arr.end(), PairComp{comp});
    auto end_iter = std::unique(meta_arr.begin(), meta_arr.end(), PairEqual{comp});
    meta_arr.erase(end_iter, meta_arr.end());

    // copy live records for return
    std::vector<std::pair<std::unique_ptr<Key>, std::unique_ptr<Payload>>> scan_results;
    scan_results.reserve(meta_arr.size());
    for (auto &&[key, meta] : meta_arr) {
      if (meta.IsVisible()) {
        scan_results.emplace_back(GetCopiedKey<Key>(meta), GetCopiedPayload<Payload>(meta));
      }
    }

    return {return_code, scan_results};
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
   * @param pmwcas_pool
   * @return NodeReturnCode
   */
  std::pair<NodeReturnCode, StatusWord>
  Write(  //
      const Key key,
      const size_t key_length,
      const Payload payload,
      const size_t payload_length,
      const size_t index_epoch,
      pmwcas::DescriptorPool *pmwcas_pool)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord current_status;
    size_t record_count;
    const auto total_length = key_length + payload_length;
    const auto inserting_meta = kInitMetadata.InitForInsert(index_epoch);
    pmwcas::Descriptor *pd;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to write a record
     *--------------------------------------------------------------------------------------------*/
    do {
      current_status = this->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, kInitStatusWord};
      }

      // prepare for MwCAS
      record_count = current_status.GetRecordCount();
      const auto new_status = current_status.AddRecordInfo(1, total_length, 0);
      const auto current_meta = this->GetMetadata(record_count);

      // perform MwCAS to reserve space
      pd = pmwcas_pool->AllocateDescriptor();
      this->SetStatusForMwCAS(current_status, new_status, pd);
      this->SetMetadataForMwCAS(record_count, current_meta, inserting_meta, pd);
    } while (!pd->MwCAS());

    /*----------------------------------------------------------------------------------------------
     * Phase 2: write a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = this->GetNodeSize() - current_status.GetBlockSize();
    offset = SetRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent SMOs)
    do {
      current_status = this->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, kInitStatusWord};
      }

      // perform MwCAS to complete a write
      pd = pmwcas_pool->AllocateDescriptor();
      this->SetStatusForMwCAS(current_status, current_status, pd);
      this->SetMetadataForMwCAS(record_count, inserting_meta, inserted_meta, pd);
    } while (!pd->MwCAS());

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
   * @param comp
   * @param pmwcas_pool
   * @return NodeReturnCode
   */
  std::pair<NodeReturnCode, StatusWord>
  Insert(  //
      const Key key,
      const size_t key_length,
      const Payload payload,
      const size_t payload_length,
      const size_t index_epoch,
      Compare comp,
      pmwcas::DescriptorPool *pmwcas_pool)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord new_status;
    size_t record_count;
    const auto total_length = key_length + payload_length;
    const auto inserting_meta = kInitMetadata.InitForInsert(index_epoch);
    pmwcas::Descriptor *pd;
    bool cas_failed;

    // local flags for insertion
    auto uniqueness = KeyExistence::kNotExist;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    do {
      const auto current_status = this->GetStatusWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, kInitStatusWord};
      }

      record_count = current_status.GetRecordCount();
      if (uniqueness != KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(key, record_count, index_epoch, comp);
        if (uniqueness == KeyExistence::kExist) {
          return {NodeReturnCode::kKeyExist, kInitStatusWord};
        }
      }

      // prepare new status for MwCAS
      new_status = current_status.AddRecordInfo(1, total_length, 0);

      // get current metadata for MwCAS
      const auto current_meta = this->GetMetadata(record_count);

      // perform MwCAS to reserve space
      pd = pmwcas_pool->AllocateDescriptor();
      this->SetStatusForMwCAS(current_status, new_status, pd);
      this->SetMetadataForMwCAS(record_count, current_meta, inserting_meta, pd);
      cas_failed = !pd->MwCAS();

      if (cas_failed) {
        uniqueness = KeyExistence::kUncertain;
      }
    } while (cas_failed);

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = this->GetNodeSize() - new_status.GetBlockSize();
    offset = SetRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent inserts and SMOs)
    do {
      if (uniqueness == KeyExistence::kUncertain) {
        uniqueness = CheckUniqueness(key, record_count, index_epoch, comp);
        if (uniqueness == KeyExistence::kExist) {
          // delete an inserted record
          this->SetMetadata(record_count, inserting_meta.UpdateOffset(0));
          return {NodeReturnCode::kKeyExist, kInitStatusWord};
        }
        continue;  // recheck
      }

      new_status = this->GetStausWordProtected();
      if (new_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, kInitStatusWord};
      }

      // perform MwCAS to complete an insert
      pd = pmwcas_pool->AllocateDescriptor();
      this->SetStatusForMwCAS(new_status, new_status, pd);
      this->SetMetadataForMwCAS(record_count, inserting_meta, inserted_meta, pd);
    } while (!pd->MwCAS());

    return {NodeReturnCode::kSuccess, new_status};
  }

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param payload
   * @param payload_length
   * @param comp
   * @param pmwcas_pool
   * @return NodeReturnCode
   */
  std::pair<NodeReturnCode, StatusWord>
  Update(  //
      const Key key,
      const size_t key_length,
      const Payload payload,
      const size_t payload_length,
      const size_t index_epoch,
      Compare comp,
      pmwcas::DescriptorPool *pmwcas_pool)
  {
    // variables and constants shared in Phase 1 & 2
    StatusWord new_status;
    size_t record_count;
    const auto total_length = key_length + payload_length;
    const auto inserting_meta = kInitMetadata.InitForInsert(index_epoch);
    pmwcas::Descriptor *pd;

    /*----------------------------------------------------------------------------------------------
     * Phase 1: reserve free space to insert a record
     *--------------------------------------------------------------------------------------------*/
    do {
      const auto current_status = this->GetStausWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, kInitStatusWord};
      }

      record_count = current_status.GetRecordCount();
      const auto existence = SearchMetadataToRead(key, record_count, comp).first;
      if (existence == KeyExistence::kNotExist) {
        return {NodeReturnCode::kKeyNotExist, kInitStatusWord};
      }

      // prepare new status for MwCAS
      new_status = current_status.AddRecordInfo(1, total_length, 0);

      // get current metadata for MwCAS
      const auto current_meta = this->GetMetadata(record_count);

      // perform MwCAS to reserve space
      pd = pmwcas_pool->AllocateDescriptor();
      this->SetStatusForMwCAS(current_status, new_status, pd);
      this->SetMetadataForMwCAS(record_count, current_meta, inserting_meta, pd);
    } while (!pd->MwCAS());

    /*----------------------------------------------------------------------------------------------
     * Phase 2: insert a record and check conflicts
     *--------------------------------------------------------------------------------------------*/

    // insert a record
    auto offset = this->GetNodeSize() - new_status.GetBlockSize();
    offset = SetRecord(key, key_length, payload, payload_length, offset);

    // prepare record metadata for MwCAS
    const auto inserted_meta = inserting_meta.SetRecordInfo(offset, key_length, total_length);

    // check conflicts (concurrent SMOs)
    do {
      new_status = this->GetStausWordProtected();
      if (new_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, kInitStatusWord};
      }

      // perform MwCAS to complete an update
      pd = pmwcas_pool->AllocateDescriptor();
      this->SetStatusForMwCAS(new_status, new_status, pd);
      this->SetMetadataForMwCAS(record_count, inserting_meta, inserted_meta, pd);
    } while (!pd->MwCAS());

    return {NodeReturnCode::kSuccess, new_status};
  }

  /**
   * @brief
   *
   * @tparam Compare
   * @param key
   * @param key_length
   * @param comp
   * @param pmwcas_pool
   * @return NodeReturnCode
   */
  std::pair<NodeReturnCode, StatusWord>
  Delete(  //
      const Key key,
      const size_t key_length,
      Compare comp,
      pmwcas::DescriptorPool *pmwcas_pool)
  {
    // variables and constants
    pmwcas::Descriptor *pd;
    StatusWord new_status;

    do {
      const auto current_status = this->GetStausWordProtected();
      if (current_status.IsFrozen()) {
        return {NodeReturnCode::kFrozen, kInitStatusWord};
      }

      const auto record_count = current_status.GetRecordCount();
      const auto existence = SearchMetadataToRead(key, record_count, comp).first;
      if (existence == KeyExistence::kNotExist) {
        return {NodeReturnCode::kKeyNotExist, kInitStatusWord};
      }

      // delete payload infomation from metadata
      const auto current_meta = this->GetMetadata(record_count);
      const auto deleted_meta = current_meta.DeleteRecordInfo();

      // prepare new status
      const auto total_length = key_length + current_meta.GetPayloadLength();
      new_status = current_status.AddRecordInfo(0, 0, total_length);

      // perform MwCAS to reserve space
      pd = pmwcas_pool->AllocateDescriptor();
      this->SetStatusForMwCAS(current_status, new_status, pd);
      this->SetMetadataForMwCAS(record_count, current_meta, deleted_meta, pd);
    } while (!pd->MwCAS());

    return {NodeReturnCode::kSuccess, new_status};
  }

  /*################################################################################################
   * Public structure modification operations
   *##############################################################################################*/

  LeafNode *
  Consolidate(const std::vector<std::pair<std::byte *, Metadata>> &live_meta)
  {
    assert(this->IsFrozen());  // a consolidating node must be locked

    // create a new node and copy records
    auto new_node = CreateEmptyNode<Key, Payload, Compare>(this->GetNodeSize());
    new_node->CopyRecordsViaMetadata(this, live_meta.begin(), live_meta.size());

    return new_node;
  }

  std::pair<BaseNode<Key, Payload, Compare> *, BaseNode<Key, Payload, Compare> *>
  Split(  //
      const std::vector<std::pair<std::byte *, Metadata>> &sorted_meta,
      const size_t left_record_count)
  {
    const auto node_size = this->GetNodeSize();

    // create a split left node
    auto left_node = CreateEmptyNode<Key, Payload, Compare>(node_size);
    auto meta_iter = sorted_meta.begin();
    meta_iter = left_node->CopyRecordsViaMetadata(this, meta_iter, left_record_count);

    // create a split right node
    auto right_node = CreateEmptyNode<Key, Payload, Compare>(node_size);
    const auto right_record_count = sorted_meta.size() - left_record_count;
    meta_iter = right_node->CopyRecordsViaMetadata(this, meta_iter, right_record_count);

    // all the records must be copied
    assert(meta_iter == sorted_meta.end());

    return {dynamic_cast<BaseNode<Key, Payload, Compare> *>(left_node),
            dynamic_cast<BaseNode<Key, Payload, Compare> *>(right_node)};
  }

  BaseNode<Key, Payload, Compare> *
  Merge(  //
      const std::vector<std::pair<std::byte *, Metadata>> &this_meta,
      LeafNode *sibling_node,
      const std::vector<std::pair<std::byte *, Metadata>> &sibling_meta,
      const bool sibling_is_left)
  {
    // create a merged node
    auto merged_node = CreateEmptyNode<Key, Payload, Compare>(this->GetNodeSize());
    if (sibling_is_left) {
      merged_node->CopyRecordsViaMetadata(sibling_node, sibling_meta.begin(), sibling_meta.size());
      merged_node->CopyRecordsViaMetadata(this, this_meta.begin(), this_meta.size());
    } else {
      merged_node->CopyRecordsViaMetadata(this, this_meta.begin(), this_meta.size());
      merged_node->CopyRecordsViaMetadata(sibling_node, sibling_meta.begin(), sibling_meta.size());
    }

    return dynamic_cast<BaseNode<Key, Payload, Compare> *>(merged_node);
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  std::vector<std::pair<std::byte *, Metadata>>
  GatherSortedLiveMetadata(Compare comp)
  {
    const auto record_count = this->GetStausWord().GetRecordCount();
    const auto sorted_count = this->GetSortedCount();

    // gather valid (live or deleted) records
    std::vector<std::pair<Key, Metadata>> meta_arr;
    meta_arr.reserve(record_count);

    // search unsorted metadata in reverse order
    for (size_t index = record_count - 1; index >= sorted_count; --index) {
      const auto meta = this->GetMetadata(index);
      if (meta.IsVisible() || meta.IsDeleted()) {
        meta_arr.emplace_back(GetKeyPtr(meta), meta);
      } else {
        // there is a key, but it is in inserting or corrupted.
        // NOTE: we can ignore inserting records because concurrent writes are aborted due to SMOs.
      }
    }

    // search sorted metadata
    for (size_t index = 0; index < sorted_count; ++index) {
      const auto meta = this->GetMetadata(index);
      meta_arr.emplace_back(GetKeyPtr(meta), meta);
    }

    // make unique with keeping the order of writes
    std::stable_sort(meta_arr.begin(), meta_arr.end(), PairComp{comp});
    auto end_iter = std::unique(meta_arr.begin(), meta_arr.end(), PairEqual{comp});

    // gather live records
    end_iter = std::remove_if(meta_arr.begin(), end_iter,
                              [](auto &obj) { return obj.second.IsDeleted(); });
    meta_arr.erase(end_iter, meta_arr.end());

    // ...WIP... convert std::byte* from Key

    return meta_arr;
  }
};

}  // namespace bztree
