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

#include <gtest/gtest.h>

#include <memory>
#include <utility>
#include <vector>

#include "bztree/components/leaf_node.hpp"

namespace dbgroup::index::bztree
{
class LeafNodeFixture : public testing::Test
{
 public:
  using NodeReturnCode = BaseNode<Key, Payload, Compare>::NodeReturnCode;
  using Record_t = Record<Key, Payload>;
  using BaseNode_t = BaseNode<Key, Payload, Compare>;
  using LeafNode_t = LeafNode<Key, Payload, Compare>;

  static constexpr size_t kNodeSize = 256;
  static constexpr size_t kIndexEpoch = 1;
  static constexpr size_t kKeyNumForTest = 10000;
  static constexpr size_t kRecordLength = kKeyLength + kPayloadLength;
  static constexpr size_t kNullKeyLength = 8;
  static constexpr size_t kNullPayloadLength = 8;
  static constexpr size_t kNullRecordLength = kNullKeyLength + kNullPayloadLength;

  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];
  Key key_null;          // null key must have 8 bytes to fill a node
  Payload payload_null;  // null payload must have 8 bytes to fill a node

  std::unique_ptr<LeafNode_t> node;
  size_t index;
  size_t expected_record_count;
  size_t expected_block_size;
  size_t expected_deleted_size;
  size_t expected_occupied_size;

  NodeReturnCode rc;
  StatusWord status;
  std::unique_ptr<Record_t> record;

 protected:
  LeafNodeFixture()
      : node{LeafNode_t::CreateEmptyNode(kNodeSize)},
        index{0},
        expected_record_count{0},
        expected_block_size{0},
        expected_deleted_size{0},
        expected_occupied_size{kHeaderLength}
  {
  }

  void SetUp() override;

  void TearDown() override;

  void
  WriteNullKey(const size_t write_num)
  {
    for (size_t index = 0; index < write_num; ++index) {
      LeafNode_t::Write(node.get(), key_null, kNullKeyLength, payload_null, kNullPayloadLength);
      ++expected_record_count;
      expected_block_size += kNullRecordLength;
      expected_occupied_size += kWordLength + kNullRecordLength;
    }
  }

  std::vector<Key>
  WriteOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    std::vector<Key> written_keys;
    for (size_t index = begin_index; index <= end_index; ++index) {
      auto key = keys[index];
      auto payload = payloads[index];
      LeafNode_t::Write(node.get(), key, kKeyLength, payload, kPayloadLength);

      written_keys.emplace_back(key);
      ++expected_record_count;
      expected_block_size += kRecordLength;
      expected_occupied_size += kWordLength + kRecordLength;
    }
    return written_keys;
  }

  void
  VerifyMetadata(  //
      const Metadata meta,
      const bool record_is_visible = true)
  {
    if (record_is_visible) {
      EXPECT_TRUE(meta.IsVisible());
      EXPECT_FALSE(meta.IsDeleted());
    } else {
      EXPECT_FALSE(meta.IsVisible());
      EXPECT_TRUE(meta.IsDeleted());
    }
    EXPECT_EQ(kKeyLength, meta.GetKeyLength());
    EXPECT_EQ(kPayloadLength, meta.GetPayloadLength());
  }

  void
  VerifyStatusWord(  //
      const StatusWord status,
      const bool status_is_frozen = false)
  {
    EXPECT_EQ(status, node->GetStatusWord());
    if (status_is_frozen) {
      EXPECT_TRUE(status.IsFrozen());
    } else {
      EXPECT_FALSE(status.IsFrozen());
    }
    EXPECT_EQ(expected_record_count, status.GetRecordCount());
    EXPECT_EQ(expected_block_size, status.GetBlockSize());
    EXPECT_EQ(expected_deleted_size, status.GetDeletedSize());
  }

  void
  VerifyKey(  //
      const Key expected,
      const Key actual)
  {
    EXPECT_TRUE(IsEqual<Compare>(expected, actual));
  }

  void
  VerifyPayload(  //
      const Payload expected,
      const Payload actual)
  {
    EXPECT_TRUE(IsEqual<PayloadComparator>(expected, actual));
  }
};

TEST_F(LeafNodeFixture, New_EmptyNode_CorrectlyInitialized)
{
  auto status = *CastAddress<StatusWord*>(ShiftAddress(node.get(), kWordLength));

  EXPECT_EQ(kNodeSize, node->GetNodeSize());
  EXPECT_EQ(0, node->GetSortedCount());
  EXPECT_EQ(status, node->GetStatusWord());
}

/*--------------------------------------------------------------------------------------------------
 * Read operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeFixture, Read_NotPresentKey_ReadFailed)
{
  std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[1]);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Scan operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeFixture, Scan_EmptyNode_NoResult)
{
  auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[0], true, &keys[9], true);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(0, scan_results.size());
}

TEST_F(LeafNodeFixture, Scan_BothClosed_ScanTargetValues)
{
  WriteOrderedKeys(4, 9);
  WriteOrderedKeys(0, 3);

  auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[4], true, &keys[6], true);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(3, scan_results.size());
  VerifyKey(keys[4], scan_results[0]->GetKey());
  VerifyPayload(payloads[4], scan_results[0]->GetPayload());
  VerifyKey(keys[5], scan_results[1]->GetKey());
  VerifyPayload(payloads[5], scan_results[1]->GetPayload());
  VerifyKey(keys[6], scan_results[2]->GetKey());
  VerifyPayload(payloads[6], scan_results[2]->GetPayload());
}

TEST_F(LeafNodeFixture, Scan_LeftClosed_ScanTargetValues)
{
  WriteOrderedKeys(4, 9);
  WriteOrderedKeys(0, 3);

  auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[7], true, &keys[9], false);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  VerifyKey(keys[7], scan_results[0]->GetKey());
  VerifyPayload(payloads[7], scan_results[0]->GetPayload());
  VerifyKey(keys[8], scan_results[1]->GetKey());
  VerifyPayload(payloads[8], scan_results[1]->GetPayload());
}

TEST_F(LeafNodeFixture, Scan_RightClosed_ScanTargetValues)
{
  WriteOrderedKeys(4, 9);
  WriteOrderedKeys(0, 3);

  auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[7], false, &keys[9], true);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  VerifyKey(keys[8], scan_results[0]->GetKey());
  VerifyPayload(payloads[8], scan_results[0]->GetPayload());
  VerifyKey(keys[9], scan_results[1]->GetKey());
  VerifyPayload(payloads[9], scan_results[1]->GetPayload());
}

TEST_F(LeafNodeFixture, Scan_BothOpened_ScanTargetValues)
{
  WriteOrderedKeys(4, 9);
  WriteOrderedKeys(0, 3);

  auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[7], false, &keys[9], false);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(1, scan_results.size());
  VerifyKey(keys[8], scan_results[0]->GetKey());
  VerifyPayload(payloads[8], scan_results[0]->GetPayload());
}

TEST_F(LeafNodeFixture, Scan_LeftInfinity_ScanTargetValues)
{
  WriteOrderedKeys(4, 9);
  WriteOrderedKeys(0, 3);

  auto [rc, scan_results] = LeafNode_t::Scan(node.get(), nullptr, false, &keys[1], true);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  VerifyKey(keys[0], scan_results[0]->GetKey());
  VerifyPayload(payloads[0], scan_results[0]->GetPayload());
  VerifyKey(keys[1], scan_results[1]->GetKey());
  VerifyPayload(payloads[1], scan_results[1]->GetPayload());
}

TEST_F(LeafNodeFixture, Scan_RightInfinity_ScanTargetValues)
{
  WriteOrderedKeys(4, 9);
  WriteOrderedKeys(0, 3);

  auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[8], true, nullptr, false);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  VerifyKey(keys[8], scan_results[0]->GetKey());
  VerifyPayload(payloads[8], scan_results[0]->GetPayload());
  VerifyKey(keys[9], scan_results[1]->GetKey());
  VerifyPayload(payloads[9], scan_results[1]->GetPayload());
}

TEST_F(LeafNodeFixture, Scan_LeftOutsideRange_NoResults)
{
  WriteOrderedKeys(4, 9);

  auto [rc, scan_results] = LeafNode_t::Scan(node.get(), nullptr, false, &keys[3], false);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(0, scan_results.size());
}

TEST_F(LeafNodeFixture, Scan_RightOutsideRange_NoResults)
{
  WriteOrderedKeys(0, 3);

  auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[5], false, nullptr, false);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(0, scan_results.size());
}

TEST_F(LeafNodeFixture, Scan_WithUpdateDelete_ScanLatestValues)
{
  WriteOrderedKeys(0, 4);
  node->Update(keys[2], kKeyLength, payloads[0], kPayloadLength);
  node->Delete(keys[3], kKeyLength);

  auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[2], true, &keys[4], true);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  VerifyKey(keys[2], scan_results[0]->GetKey());
  VerifyPayload(payloads[0], scan_results[0]->GetPayload());
  VerifyKey(keys[4], scan_results[1]->GetKey());
  VerifyPayload(payloads[4], scan_results[1]->GetPayload());
}

TEST_F(LeafNodeFixture, Scan_ConsolidatedNodeWithinRange_ScanTargetValues)
{
  // prepare a consolidated node
  WriteOrderedKeys(3, 7);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[4], true, &keys[6], true);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(3, scan_results.size());
  VerifyKey(keys[4], scan_results[0]->GetKey());
  VerifyPayload(payloads[4], scan_results[0]->GetPayload());
  VerifyKey(keys[5], scan_results[1]->GetKey());
  VerifyPayload(payloads[5], scan_results[1]->GetPayload());
  VerifyKey(keys[6], scan_results[2]->GetKey());
  VerifyPayload(payloads[6], scan_results[2]->GetPayload());
}

TEST_F(LeafNodeFixture, Scan_ConsolidatedNodeWithLeftInfinity_ScanTargetValues)
{
  // prepare a consolidated node
  WriteOrderedKeys(3, 7);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  auto [rc, scan_results] = LeafNode_t::Scan(node.get(), nullptr, true, &keys[3], true);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(1, scan_results.size());
  VerifyKey(keys[3], scan_results[0]->GetKey());
  VerifyPayload(payloads[3], scan_results[0]->GetPayload());
}

TEST_F(LeafNodeFixture, Scan_ConsolidatedNodeWithRightInfinity_ScanTargetValues)
{
  // prepare a consolidated node
  WriteOrderedKeys(3, 7);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[7], true, nullptr, true);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(1, scan_results.size());
  VerifyKey(keys[7], scan_results[0]->GetKey());
  VerifyPayload(payloads[7], scan_results[0]->GetPayload());
}

TEST_F(LeafNodeFixture, Scan_ConsolidatedNodeWithUpdateDelete_ScanLatestValues)
{
  // prepare a consolidated node
  WriteOrderedKeys(3, 7);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));
  node->Update(keys[5], kKeyLength, payloads[0], kPayloadLength);
  node->Delete(keys[7], kKeyLength);

  auto [rc, scan_results] = LeafNode_t::Scan(node.get(), &keys[5], true, &keys[7], true);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  VerifyKey(keys[5], scan_results[0]->GetKey());
  VerifyPayload(payloads[0], scan_results[0]->GetPayload());
  VerifyKey(keys[6], scan_results[1]->GetKey());
  VerifyPayload(payloads[6], scan_results[1]->GetPayload());
}

/*--------------------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeFixture, Write_TwoKeys_MetadataCorrectlyUpdated)
{
  std::tie(rc, status) =
      LeafNode_t::Write(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);

  std::tie(rc, status) =
      LeafNode_t::Write(node.get(), keys[2], kKeyLength, payloads[2], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);
}

TEST_F(LeafNodeFixture, Write_TwoKeys_ReadWrittenValues)
{
  LeafNode_t::Write(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);
  LeafNode_t::Write(node.get(), keys[2], kKeyLength, payloads[2], kPayloadLength);

  // read 1st input value
  std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[1]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyPayload(payloads[1], record->GetPayload());

  // read 2nd input value
  std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[2]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyPayload(payloads[2], record->GetPayload());
}

TEST_F(LeafNodeFixture, Write_DuplicateKey_ReadLatestValue)
{
  LeafNode_t::Write(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);
  LeafNode_t::Write(node.get(), keys[1], kKeyLength, payloads[2], kPayloadLength);

  std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[1]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyPayload(payloads[2], record->GetPayload());
}

TEST_F(LeafNodeFixture, Write_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(9);

  std::tie(rc, status) =
      LeafNode_t::Write(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);

  std::tie(rc, status) =
      LeafNode_t::Write(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kNoSpace, rc);
}

TEST_F(LeafNodeFixture, Write_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) =
      LeafNode_t::Write(node.get(), keys[11], kKeyLength, payloads[11], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);
}

TEST_F(LeafNodeFixture, Write_ConsolidatedNode_ReadWrittenValue)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  LeafNode_t::Write(node.get(), keys[11], kKeyLength, payloads[11], kPayloadLength);
  std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[11]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyPayload(payloads[11], record->GetPayload());
}

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeFixture, Insert_TwoKeys_MetadataCorrectlyUpdated)
{
  std::tie(rc, status) =
      LeafNode_t::Insert(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);

  std::tie(rc, status) =
      LeafNode_t::Insert(node.get(), keys[2], kKeyLength, payloads[2], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);
}

TEST_F(LeafNodeFixture, Insert_TwoKeys_ReadInsertedValues)
{
  LeafNode_t::Insert(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);
  LeafNode_t::Insert(node.get(), keys[2], kKeyLength, payloads[2], kPayloadLength);

  // read 1st input value
  std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[1]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyPayload(payloads[1], record->GetPayload());

  // read 2nd input value
  std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[2]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyPayload(payloads[2], record->GetPayload());
}

TEST_F(LeafNodeFixture, Insert_DuplicateKey_InsertionFailed)
{
  LeafNode_t::Insert(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);

  std::tie(rc, status) =
      LeafNode_t::Insert(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyExist, rc);
}

TEST_F(LeafNodeFixture, Insert_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(9);

  // fill a node
  std::tie(rc, status) =
      LeafNode_t::Insert(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);
  expected_occupied_size += kWordLength + kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(expected_occupied_size, status.GetOccupiedSize());

  // insert a filled node with a not present key
  std::tie(rc, status) =
      LeafNode_t::Insert(node.get(), keys[2], kKeyLength, payloads[2], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kNoSpace, rc);

  // insert a filled node with an present key
  std::tie(rc, status) =
      LeafNode_t::Insert(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyExist, rc);
}

TEST_F(LeafNodeFixture, Insert_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) =
      LeafNode_t::Insert(node.get(), keys[11], kKeyLength, payloads[11], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);
}

TEST_F(LeafNodeFixture, Insert_ConsolidatedNode_ReadInsertedValue)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  LeafNode_t::Insert(node.get(), keys[11], kKeyLength, payloads[11], kPayloadLength);
  std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[11]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyPayload(payloads[11], record->GetPayload());
}

TEST_F(LeafNodeFixture, Insert_ConsolidatedNodeWithDuplicateKey_InsertionFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) =
      LeafNode_t::Insert(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeFixture, Update_SingleKey_MetadataCorrectlyUpdated)
{
  LeafNode_t::Insert(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;

  std::tie(rc, status) = node->Update(keys[1], kKeyLength, payloads[2], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;
  expected_deleted_size += kWordLength + kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);
}

TEST_F(LeafNodeFixture, Update_SingleKey_ReadUpdatedValue)
{
  LeafNode_t::Insert(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);
  node->Update(keys[1], kKeyLength, payloads[2], kPayloadLength);

  std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[1]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyPayload(payloads[2], record->GetPayload());
}

TEST_F(LeafNodeFixture, Update_NotPresentKey_UpdatedFailed)
{
  std::tie(rc, status) = node->Update(keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeFixture, Update_DeletedKey_UpdateFailed)
{
  LeafNode_t::Insert(node.get(), keys[1], kKeyLength, payloads[2], kPayloadLength);
  node->Delete(keys[1], kKeyLength);

  std::tie(rc, status) = node->Update(keys[1], kKeyLength, payloads[2], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeFixture, Update_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(9);

  // fill a node
  std::tie(rc, status) = node->Update(key_null, kNullKeyLength, payload_null, kNullPayloadLength);
  expected_occupied_size += kWordLength + kNullRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_EQ(expected_occupied_size, status.GetOccupiedSize());

  // update a filled node with an present key
  std::tie(rc, status) = node->Update(key_null, kNullKeyLength, payload_null, kNullPayloadLength);

  EXPECT_EQ(NodeReturnCode::kNoSpace, rc);

  // update a filled node with a not present key
  std::tie(rc, status) = node->Update(keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeFixture, Update_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) = node->Update(keys[1], kKeyLength, payloads[11], kPayloadLength);
  expected_record_count += 1;
  expected_block_size += kRecordLength;
  expected_deleted_size += kWordLength + kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);
}

TEST_F(LeafNodeFixture, Update_ConsolidatedNode_ReadUpdatedValue)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  node->Update(keys[1], kKeyLength, payloads[11], kPayloadLength);
  std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[1]);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyPayload(payloads[11], record->GetPayload());
}

TEST_F(LeafNodeFixture, Update_ConsolidatedNodeWithNotPresentKey_UpdatedFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) = node->Update(key_null, kNullKeyLength, payload_null, kNullPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeFixture, Update_ConsolidatedNodeWithDeletedKey_UpdatedFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  node->Delete(keys[1], kKeyLength);
  std::tie(rc, status) = node->Update(keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeFixture, Delete_TwoKeys_MetadataCorrectlyUpdated)
{
  constexpr bool kRecordIsVisible = false;

  LeafNode_t::Insert(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);
  LeafNode_t::Insert(node.get(), keys[2], kKeyLength, payloads[2], kPayloadLength);
  expected_record_count += 2;
  expected_block_size += 2 * kRecordLength;

  std::tie(rc, status) = node->Delete(keys[1], kKeyLength);
  expected_deleted_size += kWordLength + kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++), kRecordIsVisible);
  VerifyStatusWord(status);

  std::tie(rc, status) = node->Delete(keys[2], kKeyLength);
  expected_deleted_size += kWordLength + kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++), kRecordIsVisible);
  VerifyStatusWord(status);
}

TEST_F(LeafNodeFixture, Delete_PresentKey_DeletionSucceed)
{
  LeafNode_t::Insert(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);

  std::tie(rc, status) = node->Delete(keys[1], kKeyLength);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
}

TEST_F(LeafNodeFixture, Delete_PresentKey_ReadFailed)
{
  LeafNode_t::Insert(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);
  node->Delete(keys[1], kKeyLength);

  std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[1]);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeFixture, Delete_NotPresentKey_DeletionFailed)
{
  std::tie(rc, status) = node->Delete(keys[1], kKeyLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeFixture, Delete_DeletedKey_DeletionFailed)
{
  LeafNode_t::Insert(node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);
  node->Delete(keys[1], kKeyLength);

  std::tie(rc, status) = node->Delete(keys[1], kKeyLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeFixture, Delete_FilledNode_GetCorrectReturnCodes)
{
  WriteNullKey(10);

  std::tie(rc, status) = node->Delete(key_null, kNullKeyLength);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);

  std::tie(rc, status) = node->Delete(key_null, kNullKeyLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeFixture, Delete_ConsolidatedNode_MetadataCorrectlyUpdated)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) = node->Delete(keys[1], kKeyLength);
  expected_deleted_size += kWordLength + kRecordLength;

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  VerifyMetadata(node->GetMetadata(index++));
  VerifyStatusWord(status);
}

TEST_F(LeafNodeFixture, Delete_ConsolidatedNode_DeletionSucceed)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) = node->Delete(keys[1], kKeyLength);

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
}

TEST_F(LeafNodeFixture, Delete_ConsolidatedNodeWithNotPresentKey_DeletionFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  std::tie(rc, status) = node->Delete(key_null, kNullKeyLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

TEST_F(LeafNodeFixture, Delete_ConsolidatedNodeWithDeletedKey_DeletionFailed)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 4);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  node->Delete(keys[1], kKeyLength);
  std::tie(rc, status) = node->Delete(keys[1], kKeyLength);

  EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Consolide operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeFixture, Consolidate_SortedTenKeys_GatherSortedLiveMetadata)
{
  // fill a node with ordered keys
  auto written_keys = WriteOrderedKeys(0, 9);

  // gather live metadata and check equality
  auto meta_vec = node->GatherSortedLiveMetadata();

  EXPECT_EQ(expected_record_count, meta_vec.size());
  for (size_t index = 0; index < meta_vec.size(); index++) {
    auto [key, meta] = meta_vec[index];
    VerifyKey(written_keys[index], key);
    EXPECT_TRUE(meta.IsVisible());
  }
}

TEST_F(LeafNodeFixture, Consolidate_SortedTenKeysWithDelete_GatherSortedLiveMetadata)
{
  // fill a node with ordered keys
  auto written_keys = WriteOrderedKeys(0, 9);

  // delete a key
  node->Delete(keys[2], kKeyLength);
  --expected_record_count;
  written_keys.erase(std::find(written_keys.begin(), written_keys.end(), keys[2]));

  // gather live metadata and check equality
  auto meta_vec = node->GatherSortedLiveMetadata();

  EXPECT_EQ(expected_record_count, meta_vec.size());
  for (size_t index = 0; index < meta_vec.size(); index++) {
    auto [key, meta] = meta_vec[index];
    VerifyKey(written_keys[index], key);
    EXPECT_TRUE(meta.IsVisible());
  }
}

TEST_F(LeafNodeFixture, Consolidate_SortedTenKeysWithUpdate_GatherSortedLiveMetadata)
{
  // fill a node with ordered keys
  auto written_keys = WriteOrderedKeys(0, 8);
  node->Update(keys[2], kKeyLength, payload_null, kNullPayloadLength);

  // gather live metadata and check equality
  auto meta_vec = node->GatherSortedLiveMetadata();

  EXPECT_EQ(expected_record_count, meta_vec.size());
  for (size_t index = 0; index < meta_vec.size(); index++) {
    auto [key, meta] = meta_vec[index];
    VerifyKey(written_keys[index], key);
    EXPECT_TRUE(meta.IsVisible());
  }
}

TEST_F(LeafNodeFixture, Consolidate_UnsortedTenKeys_GatherSortedLiveMetadata)
{
  // fill a node with ordered keys
  auto tmp_keys = WriteOrderedKeys(4, 9);
  auto written_keys = WriteOrderedKeys(0, 3);
  written_keys.insert(written_keys.end(), tmp_keys.begin(), tmp_keys.end());

  // gather live metadata and check equality
  auto meta_vec = node->GatherSortedLiveMetadata();

  EXPECT_EQ(expected_record_count, meta_vec.size());
  for (size_t index = 0; index < meta_vec.size(); index++) {
    auto [key, meta] = meta_vec[index];
    VerifyKey(written_keys[index], key);
    EXPECT_TRUE(meta.IsVisible());
  }
}

TEST_F(LeafNodeFixture, Consolidate_SortedTenKeys_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 9);
  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(LeafNodeFixture, Consolidate_SortedTenKeysWithDelete_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 9);
  node->Delete(keys[2], kKeyLength);
  expected_record_count -= 1;
  expected_block_size -= kRecordLength;

  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(LeafNodeFixture, Consolidate_SortedTenKeysWithUpdate_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(0, 8);
  node->Update(keys[2], kKeyLength, payload_null, kNullPayloadLength);
  expected_block_size += kNullPayloadLength - kPayloadLength;

  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(LeafNodeFixture, Consolidate_UnsortedTenKeys_NodeHasCorrectStatus)
{
  // prepare a consolidated node
  WriteOrderedKeys(4, 9);
  WriteOrderedKeys(0, 3);

  auto meta_vec = node->GatherSortedLiveMetadata();
  node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

  VerifyStatusWord(node->GetStatusWord());
}

/*--------------------------------------------------------------------------------------------------
 * Split operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeFixture, Split_EquallyDivided_NodesHaveCorrectStatus)
{
  // prepare split nodes
  WriteOrderedKeys(0, 9);
  const auto left_record_count = 5;
  auto meta_vec = node->GatherSortedLiveMetadata();

  auto [left_node, right_node] = LeafNode_t::Split(node.get(), meta_vec, left_record_count);
  expected_record_count = 5;
  expected_block_size = expected_record_count * kRecordLength;

  node.reset(left_node);
  VerifyStatusWord(node->GetStatusWord());

  node.reset(right_node);
  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(LeafNodeFixture, Split_EquallyDivided_NodesHaveCorrectKeyPayloads)
{
  // prepare split nodes
  WriteOrderedKeys(0, 9);
  const auto left_record_count = 5;
  auto meta_vec = node->GatherSortedLiveMetadata();
  auto [left_node, right_node] = LeafNode_t::Split(node.get(), meta_vec, left_record_count);

  // check a split left node
  size_t index = 0;
  for (; index < left_record_count; ++index) {
    std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(left_node), keys[index]);

    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index], record->GetPayload());
  }

  // check a split right node
  for (; index < expected_record_count; ++index) {
    std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(right_node), keys[index]);

    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index], record->GetPayload());
  }
}

/*--------------------------------------------------------------------------------------------------
 * Merge operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(LeafNodeFixture, Merge_LeftSiblingNode_NodeHasCorrectStatus)
{
  // prepare a merged node
  auto target = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  LeafNode_t::Write(target.get(), keys[4], kKeyLength, payloads[4], kPayloadLength);
  auto target_meta = target->GatherSortedLiveMetadata();

  auto sibling = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  LeafNode_t::Write(sibling.get(), keys[3], kKeyLength, payloads[3], kPayloadLength);
  auto sibling_meta = sibling->GatherSortedLiveMetadata();

  node.reset(LeafNode_t::Merge(target.get(), target_meta, sibling.get(), sibling_meta, true));
  expected_record_count = 2;
  expected_block_size = expected_record_count * kRecordLength;

  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(LeafNodeFixture, Merge_RightSiblingNode_NodeHasCorrectStatus)
{
  // prepare a merged node
  auto target = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  LeafNode_t::Write(target.get(), keys[2], kKeyLength, payloads[2], kPayloadLength);
  auto target_meta = target->GatherSortedLiveMetadata();

  auto sibling = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  LeafNode_t::Write(sibling.get(), keys[3], kKeyLength, payloads[3], kPayloadLength);
  auto sibling_meta = sibling->GatherSortedLiveMetadata();

  node.reset(LeafNode_t::Merge(target.get(), target_meta, sibling.get(), sibling_meta, false));
  expected_record_count = 2;
  expected_block_size = expected_record_count * kRecordLength;

  VerifyStatusWord(node->GetStatusWord());
}

TEST_F(LeafNodeFixture, Merge_LeftSiblingNode_NodeHasCorrectKeyPayloads)
{
  // prepare a merged node
  auto target = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  LeafNode_t::Write(target.get(), keys[4], kKeyLength, payloads[4], kPayloadLength);
  auto target_meta = target->GatherSortedLiveMetadata();

  auto sibling = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  LeafNode_t::Write(sibling.get(), keys[3], kKeyLength, payloads[3], kPayloadLength);
  auto sibling_meta = sibling->GatherSortedLiveMetadata();

  node.reset(LeafNode_t::Merge(target.get(), target_meta, sibling.get(), sibling_meta, true));

  // check keys and payloads
  for (size_t index = 3; index <= 4; ++index) {
    std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[index]);

    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index], record->GetPayload());
  }
}

TEST_F(LeafNodeFixture, Merge_RightSiblingNode_NodeHasCorrectKeyPayloads)
{
  // prepare a merged node
  auto target = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  LeafNode_t::Write(target.get(), keys[2], kKeyLength, payloads[2], kPayloadLength);
  auto target_meta = target->GatherSortedLiveMetadata();

  auto sibling = std::unique_ptr<LeafNode_t>(LeafNode_t::CreateEmptyNode(kNodeSize));
  LeafNode_t::Write(sibling.get(), keys[3], kKeyLength, payloads[3], kPayloadLength);
  auto sibling_meta = sibling->GatherSortedLiveMetadata();

  node.reset(LeafNode_t::Merge(target.get(), target_meta, sibling.get(), sibling_meta, false));

  // check keys and payloads
  for (size_t index = 2; index <= 3; ++index) {
    std::tie(rc, record) = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[index]);

    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index], record->GetPayload());
  }
}

}  // namespace dbgroup::index::bztree
