// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <gtest/gtest.h>

#include <memory>
#include <thread>

#include "bztree/bztree.hpp"

using std::byte;

namespace dbgroup::index::bztree
{
using Record_t = Record<Key, Payload>;
using BaseNode_t = BaseNode<Key, Payload, Compare>;
using LeafNode_t = LeafNode<Key, Payload, Compare>;
using InternalNode_t = InternalNode<Key, Payload, Compare>;
using BzTree_t = BzTree<Key, Payload, Compare>;
using NodeReturnCode = BaseNode<Key, Payload, Compare>::NodeReturnCode;
using KeyExistence = BaseNode<Key, Payload, Compare>::KeyExistence;

constexpr size_t kTestNodeSize = 256;          // a node can have 10 records (16 + 24 * 10)
constexpr size_t kTestMinNodeSize = 89;        // a node with 3 records invokes merging
constexpr size_t kTestMinFreeSpace = 24;       // keep free space with 1 record size
constexpr size_t kTestExpectedFreeSpace = 72;  // expect free space can write 3 records
constexpr size_t kTestMaxDeletedSize = 119;    // consolidate when 5 records are deleted
constexpr size_t kTestMaxMergedSize = 137;     // a merged node has space for 5 records
constexpr size_t kIndexEpoch = 1;
constexpr size_t kKeyNumForTest = 10000;
constexpr size_t kRecordLength = kKeyLength + kPayloadLength;
constexpr size_t kNullKeyLength = 8;
constexpr size_t kNullPayloadLength = 8;
constexpr size_t kNullRecordLength = kNullKeyLength + kNullPayloadLength;

class BzTreeFixture : public testing::Test
{
 public:
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];
  Key key_null;          // null key must have 8 bytes to fill a node
  Payload payload_null;  // null payload must have 8 bytes to fill a node

  BzTree_t bztree = BzTree_t{};

  ReturnCode rc;
  std::unique_ptr<Record_t> record;

  void
  WriteNullKey(  //
      BzTree_t* bztree,
      const size_t write_num)
  {
    for (size_t index = 0; index < write_num; ++index) {
      bztree->Write(key_null, kNullKeyLength, payload_null, kNullPayloadLength);
    }
  }

  void
  WriteOrderedKeys(  //
      BzTree_t* bztree,
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      bztree->Write(keys[index], kKeyLength, payloads[index], kPayloadLength);
    }
  }

  void
  InsertOrderedKeys(  //
      BzTree_t* bztree,
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      bztree->Insert(keys[index], kKeyLength, payloads[index], kPayloadLength);
    }
  }

  void
  UpdateOrderedKeys(  //
      BzTree_t* bztree,
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index + 1 < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      bztree->Update(keys[index], kKeyLength, payloads[index + 1], kPayloadLength);
    }
  }

  void
  DeleteOrderedKeys(  //
      BzTree_t* bztree,
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      bztree->Delete(keys[index], kKeyLength);
    }
  }

 protected:
  void SetUp() override;

  void TearDown() override;

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

/*--------------------------------------------------------------------------------------------------
 * Read operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeFixture, Read_NotPresentKey_ReadFailed)
{
  std::tie(rc, record) = bztree.Read(keys[1]);

  EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Scan operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeFixture, Scan_EmptyNode_NoResult)
{
  auto [rc, scan_results] = bztree.Scan(keys[1], true, keys[10], true);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(0, scan_results.size());
}

TEST_F(BzTreeFixture, Scan_BothClosed_ScanTargetValues)
{
  WriteOrderedKeys(&bztree, 1, 10);

  auto [rc, scan_results] = bztree.Scan(keys[4], true, keys[6], true);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(3, scan_results.size());
  VerifyKey(keys[4], scan_results[0]->GetKey());
  VerifyPayload(payloads[4], scan_results[0]->GetPayload());
  VerifyKey(keys[5], scan_results[1]->GetKey());
  VerifyPayload(payloads[5], scan_results[1]->GetPayload());
  VerifyKey(keys[6], scan_results[2]->GetKey());
  VerifyPayload(payloads[6], scan_results[2]->GetPayload());
}

TEST_F(BzTreeFixture, Scan_LeftClosed_ScanTargetValues)
{
  WriteOrderedKeys(&bztree, 1, 10);

  auto [rc, scan_results] = bztree.Scan(keys[8], true, keys[10], false);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  VerifyKey(keys[8], scan_results[0]->GetKey());
  VerifyPayload(payloads[8], scan_results[0]->GetPayload());
  VerifyKey(keys[9], scan_results[1]->GetKey());
  VerifyPayload(payloads[9], scan_results[1]->GetPayload());
}

TEST_F(BzTreeFixture, Scan_RightClosed_ScanTargetValues)
{
  WriteOrderedKeys(&bztree, 1, 10);

  auto [rc, scan_results] = bztree.Scan(keys[8], false, keys[10], true);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  VerifyKey(keys[9], scan_results[0]->GetKey());
  VerifyPayload(payloads[9], scan_results[0]->GetPayload());
  VerifyKey(keys[10], scan_results[1]->GetKey());
  VerifyPayload(payloads[10], scan_results[1]->GetPayload());
}

TEST_F(BzTreeFixture, Scan_BothOpened_ScanTargetValues)
{
  WriteOrderedKeys(&bztree, 1, 10);

  auto [rc, scan_results] = bztree.Scan(keys[8], false, keys[10], false);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(1, scan_results.size());
  VerifyKey(keys[9], scan_results[0]->GetKey());
  VerifyPayload(payloads[9], scan_results[0]->GetPayload());
}

TEST_F(BzTreeFixture, ScanLess_EndClosed_ScanTargetValues)
{
  WriteOrderedKeys(&bztree, 1, 10);

  auto [rc, scan_results] = bztree.ScanLess(keys[2], true);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  VerifyKey(keys[1], scan_results[0]->GetKey());
  VerifyPayload(payloads[1], scan_results[0]->GetPayload());
  VerifyKey(keys[2], scan_results[1]->GetKey());
  VerifyPayload(payloads[2], scan_results[1]->GetPayload());
}

TEST_F(BzTreeFixture, ScanGreater_BeginClosed_ScanTargetValues)
{
  WriteOrderedKeys(&bztree, 1, 10);

  auto [rc, scan_results] = bztree.ScanGreater(keys[9], true);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  VerifyKey(keys[9], scan_results[0]->GetKey());
  VerifyPayload(payloads[9], scan_results[0]->GetPayload());
  VerifyKey(keys[10], scan_results[1]->GetKey());
  VerifyPayload(payloads[10], scan_results[1]->GetPayload());
}

TEST_F(BzTreeFixture, ScanLess_LeftOutsideRange_NoResults)
{
  WriteOrderedKeys(&bztree, 5, 10);

  auto [rc, scan_results] = bztree.ScanLess(keys[3], false);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(0, scan_results.size());
}

TEST_F(BzTreeFixture, ScanGreater_RightOutsideRange_NoResults)
{
  WriteOrderedKeys(&bztree, 1, 4);

  auto [rc, scan_results] = bztree.ScanGreater(keys[5], false);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(0, scan_results.size());
}

TEST_F(BzTreeFixture, Scan_WithUpdateDelete_ScanLatestValues)
{
  WriteOrderedKeys(&bztree, 1, 5);
  bztree.Update(keys[2], kKeyLength, payloads[0], kPayloadLength);
  bztree.Delete(keys[3], kKeyLength);

  auto [rc, scan_results] = bztree.Scan(keys[2], true, keys[4], true);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  VerifyKey(keys[2], scan_results[0]->GetKey());
  VerifyPayload(payloads[0], scan_results[0]->GetPayload());
  VerifyKey(keys[4], scan_results[1]->GetKey());
  VerifyPayload(payloads[4], scan_results[1]->GetPayload());
}

/*--------------------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeFixture, Write_TwoKeys_ReadWrittenValues)
{
  bztree.Write(keys[1], kKeyLength, payloads[1], kPayloadLength);
  bztree.Write(keys[2], kKeyLength, payloads[2], kPayloadLength);

  // read 1st input value
  std::tie(rc, record) = bztree.Read(keys[1]);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  VerifyPayload(payloads[1], record->GetPayload());

  // read 2nd input value
  std::tie(rc, record) = bztree.Read(keys[2]);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  VerifyPayload(payloads[2], record->GetPayload());
}

TEST_F(BzTreeFixture, Write_DuplicateKey_ReadLatestValue)
{
  bztree.Write(keys[1], kKeyLength, payloads[1], kPayloadLength);
  bztree.Write(keys[1], kKeyLength, payloads[2], kPayloadLength);

  std::tie(rc, record) = bztree.Read(keys[1]);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  VerifyPayload(payloads[2], record->GetPayload());
}

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeFixture, Insert_TwoKeys_ReadInsertedValues)
{
  bztree.Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);
  bztree.Insert(keys[2], kKeyLength, payloads[2], kPayloadLength);

  // read 1st input value
  std::tie(rc, record) = bztree.Read(keys[1]);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  VerifyPayload(payloads[1], record->GetPayload());

  // read 2nd input value
  std::tie(rc, record) = bztree.Read(keys[2]);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  VerifyPayload(payloads[2], record->GetPayload());
}

TEST_F(BzTreeFixture, Insert_DuplicateKey_InsertionFailed)
{
  bztree.Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);

  rc = bztree.Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(ReturnCode::kKeyExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeFixture, Update_SingleKey_ReadUpdatedValue)
{
  bztree.Insert(keys[1], kKeyLength, payloads[2], kPayloadLength);
  bztree.Update(keys[1], kKeyLength, payloads[2], kPayloadLength);

  std::tie(rc, record) = bztree.Read(keys[1]);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  VerifyPayload(payloads[2], record->GetPayload());
}

TEST_F(BzTreeFixture, Update_NotPresentKey_UpdatedFailed)
{
  rc = bztree.Update(keys[1], kKeyLength, payloads[1], kPayloadLength);

  EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
}

TEST_F(BzTreeFixture, Update_DeletedKey_UpdateFailed)
{
  bztree.Insert(keys[1], kKeyLength, payloads[2], kPayloadLength);
  bztree.Delete(keys[1], kKeyLength);

  rc = bztree.Update(keys[1], kKeyLength, payloads[2], kPayloadLength);

  EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeFixture, Delete_PresentKey_DeletionSucceed)
{
  bztree.Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);

  rc = bztree.Delete(keys[1], kKeyLength);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
}

TEST_F(BzTreeFixture, Delete_PresentKey_ReadFailed)
{
  bztree.Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);
  bztree.Delete(keys[1], kKeyLength);

  std::tie(rc, record) = bztree.Read(keys[1]);

  EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
}

TEST_F(BzTreeFixture, Delete_NotPresentKey_DeletionFailed)
{
  rc = bztree.Delete(keys[1], kKeyLength);

  EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
}

TEST_F(BzTreeFixture, Delete_DeletedKey_DeletionFailed)
{
  bztree.Insert(keys[1], kKeyLength, payloads[1], kPayloadLength);
  bztree.Delete(keys[1], kKeyLength);

  rc = bztree.Delete(keys[1], kKeyLength);

  EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Split operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeFixture, Split_OrderedKeyWrites_ReadWrittenKeys)
{
  const auto record_count = 1000;

  WriteOrderedKeys(&bztree, 1, record_count);
  // std::thread{&BzTreeFixture::WriteOrderedKeys, this, &bztree, 1, record_count}.join();

  for (size_t index = 1; index <= record_count; ++index) {
    std::tie(rc, record) = bztree.Read(keys[index]);
    auto result = record->GetPayload();
    EXPECT_EQ(ReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index], result);
  }

  auto [rc, scan_results] = bztree.Scan(keys[50], true, keys[100], true);
  EXPECT_EQ(ReturnCode::kSuccess, rc);
  auto index = 50;
  for (auto&& record : scan_results) {
    VerifyKey(keys[index], record->GetKey());
    VerifyPayload(payloads[index++], record->GetPayload());
  }
}

TEST_F(BzTreeFixture, Split_OrderedKeyInserts_ReadInsertedKeys)
{
  const auto record_count = 1000;

  InsertOrderedKeys(&bztree, 1, record_count);
  // std::thread{&BzTreeFixture::InsertOrderedKeys, this, &bztree, 1, record_count}.join();

  for (size_t index = 1; index <= record_count; ++index) {
    std::tie(rc, record) = bztree.Read(keys[index]);
    auto result = record->GetPayload();
    EXPECT_EQ(ReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index], result);
  }

  auto [rc, scan_results] = bztree.Scan(keys[50], true, keys[100], true);
  EXPECT_EQ(ReturnCode::kSuccess, rc);
  auto index = 50UL;
  for (auto&& record : scan_results) {
    VerifyKey(keys[index], record->GetKey());
    VerifyPayload(payloads[index++], record->GetPayload());
  }
}

TEST_F(BzTreeFixture, Split_OrderedKeyInsertsUpdates_ReadLatestKeys)
{
  const auto record_count = 1000;

  InsertOrderedKeys(&bztree, 1, record_count);
  UpdateOrderedKeys(&bztree, 1, record_count);
  // std::thread{&BzTreeFixture::InsertOrderedKeys, this, &bztree, 1, record_count}.join();
  // std::thread{&BzTreeFixture::UpdateOrderedKeys, this, &bztree, 1, record_count}.join();

  for (size_t index = 1; index <= record_count; ++index) {
    std::tie(rc, record) = bztree.Read(keys[index]);
    auto result = record->GetPayload();
    EXPECT_EQ(ReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index + 1], result);
  }

  auto [rc, scan_results] = bztree.Scan(keys[50], true, keys[100], true);
  EXPECT_EQ(ReturnCode::kSuccess, rc);
  auto index = 50UL;
  for (auto&& record : scan_results) {
    VerifyKey(keys[index], record->GetKey());
    VerifyPayload(payloads[++index], record->GetPayload());
  }
}

/*--------------------------------------------------------------------------------------------------
 * Merge operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeFixture, Merge_OrderedKeyWritesDeletes_ReadRemainingKey)
{
  const auto record_count = 1000;

  std::thread{&BzTreeFixture::WriteOrderedKeys, this, &bztree, 1, record_count}.join();
  std::thread{&BzTreeFixture::DeleteOrderedKeys, this, &bztree, 2, record_count}.join();

  std::tie(rc, record) = bztree.Read(keys[1]);
  auto result = record->GetPayload();
  EXPECT_EQ(ReturnCode::kSuccess, rc);
  VerifyPayload(payloads[1], result);

  for (size_t index = 2; index <= record_count; ++index) {
    std::tie(rc, record) = bztree.Read(keys[index]);
    EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
  }
}

}  // namespace dbgroup::index::bztree
