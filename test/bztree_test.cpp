// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "bztree/bztree.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <thread>

using std::byte;

namespace dbgroup::index::bztree
{
using Key = uint64_t;
using Payload = uint64_t;
using Record_t = Record<Key, Payload>;
using BaseNode_t = BaseNode<Key, Payload>;
using LeafNode_t = LeafNode<Key, Payload>;
using InternalNode_t = InternalNode<Key, Payload>;
using BzTree_t = BzTree<Key, Payload>;
using NodeReturnCode = BaseNode<Key, Payload>::NodeReturnCode;
using KeyExistence = BaseNode<Key, Payload>::KeyExistence;

constexpr size_t kTestNodeSize = 256;          // a node can have 10 records (16 + 24 * 10)
constexpr size_t kTestMinNodeSize = 89;        // a node with 3 records invokes merging
constexpr size_t kTestMinFreeSpace = 24;       // keep free space with 1 record size
constexpr size_t kTestExpectedFreeSpace = 72;  // expect free space can write 3 records
constexpr size_t kTestMaxDeletedSize = 119;    // consolidate when 5 records are deleted
constexpr size_t kTestMaxMergedSize = 137;     // a merged node has space for 5 records
constexpr size_t kIndexEpoch = 0;
constexpr size_t kKeyNumForTest = 10000;
constexpr size_t kKeyLength = sizeof(Key);
constexpr size_t kPayloadLength = sizeof(Payload);
constexpr size_t kRecordLength = kKeyLength + kPayloadLength;

class BzTreeUInt64Fixture : public testing::Test
{
 public:
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];
  Key key_null = 0;          // null key must have 8 bytes to fill a node
  Payload payload_null = 0;  // null payload must have 8 bytes to fill a node

  ReturnCode rc;
  std::unique_ptr<Record_t> record;

  constexpr uint64_t
  CastToValue(const void* target_addr)
  {
    return *BitCast<uint64_t*>(target_addr);
  }

  void
  WriteNullKey(  //
      BzTree_t* bztree,
      const size_t write_num)
  {
    for (size_t index = 0; index < write_num; ++index) {
      bztree->Write(key_null, kKeyLength, payload_null, kPayloadLength);
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
      bztree->Update(keys[index], kKeyLength, payloads[index], kPayloadLength);
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
  void
  SetUp() override
  {
    for (size_t index = 0; index < kKeyNumForTest; index++) {
      keys[index] = index + 1;
      payloads[index] = index + 1;
    }
  }

  void
  TearDown() override
  {
  }
};

/*--------------------------------------------------------------------------------------------------
 * Read operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeUInt64Fixture, Read_NotPresentKey_ReadFailed)
{
  auto bztree = BzTree_t{};
  std::tie(rc, record) = bztree.Read(keys[1]);

  EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
}

// /*--------------------------------------------------------------------------------------------------
//  * Scan operation
//  *------------------------------------------------------------------------------------------------*/

// TEST_F(BzTreeUInt64Fixture, Scan_EmptyNode_NoResult)
// {
//   auto bztree = BzTree_t{};
//   auto [rc, scan_results] = bztree.Scan(keys[1], true, keys[10], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TEST_F(BzTreeUInt64Fixture, Scan_BothClosed_ScanTargetValues)
// {
//   auto bztree = BzTree_t{};
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.Scan(keys[4], true, keys[6], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(3, scan_results.size());
//   EXPECT_EQ(keys[4], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[4], CastToValue(scan_results[0].second.get()));
//   EXPECT_EQ(keys[5], CastToValue(scan_results[1].first.get()));
//   EXPECT_EQ(payloads[5], CastToValue(scan_results[1].second.get()));
//   EXPECT_EQ(keys[6], CastToValue(scan_results[2].first.get()));
//   EXPECT_EQ(payloads[6], CastToValue(scan_results[2].second.get()));
// }

// TEST_F(BzTreeUInt64Fixture, Scan_LeftClosed_ScanTargetValues)
// {
//   auto bztree = BzTree_t{};
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.Scan(keys[8], true, keys[10], false);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   EXPECT_EQ(keys[8], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[8], CastToValue(scan_results[0].second.get()));
//   EXPECT_EQ(keys[9], CastToValue(scan_results[1].first.get()));
//   EXPECT_EQ(payloads[9], CastToValue(scan_results[1].second.get()));
// }

// TEST_F(BzTreeUInt64Fixture, Scan_RightClosed_ScanTargetValues)
// {
//   auto bztree = BzTree_t{};
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.Scan(keys[8], false, keys[10], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   EXPECT_EQ(keys[9], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[9], CastToValue(scan_results[0].second.get()));
//   EXPECT_EQ(keys[10], CastToValue(scan_results[1].first.get()));
//   EXPECT_EQ(payloads[10], CastToValue(scan_results[1].second.get()));
// }

// TEST_F(BzTreeUInt64Fixture, Scan_BothOpened_ScanTargetValues)
// {
//   auto bztree = BzTree_t{};
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.Scan(keys[8], false, keys[10], false);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(1, scan_results.size());
//   EXPECT_EQ(keys[9], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[9], CastToValue(scan_results[0].second.get()));
// }

// TEST_F(BzTreeUInt64Fixture, Scan_LeftInfinity_ScanTargetValues)
// {
//   auto bztree = BzTree_t{};
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.Scan(nullptr, false, keys[2], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   EXPECT_EQ(keys[1], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[1], CastToValue(scan_results[0].second.get()));
//   EXPECT_EQ(keys[2], CastToValue(scan_results[1].first.get()));
//   EXPECT_EQ(payloads[2], CastToValue(scan_results[1].second.get()));
// }

// TEST_F(BzTreeUInt64Fixture, Scan_RightInfinity_ScanTargetValues)
// {
//   auto bztree = BzTree_t{};
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.Scan(keys[9], true, nullptr, false);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   EXPECT_EQ(keys[9], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[9], CastToValue(scan_results[0].second.get()));
//   EXPECT_EQ(keys[10], CastToValue(scan_results[1].first.get()));
//   EXPECT_EQ(payloads[10], CastToValue(scan_results[1].second.get()));
// }

// TEST_F(BzTreeUInt64Fixture, Scan_LeftOutsideRange_NoResults)
// {
//   auto bztree = BzTree_t{};
//   WriteOrderedKeys(&bztree, 5, 10);

//   auto [rc, scan_results] = bztree.Scan(nullptr, false, keys[3], false);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TEST_F(BzTreeUInt64Fixture, Scan_RightOutsideRange_NoResults)
// {
//   auto bztree = BzTree_t{};
//   WriteOrderedKeys(&bztree, 1, 4);

//   auto [rc, scan_results] = bztree.Scan(keys[5], false, nullptr, false);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TEST_F(BzTreeUInt64Fixture, Scan_WithUpdateDelete_ScanLatestValues)
// {
//   auto bztree = BzTree_t{};
//   WriteOrderedKeys(&bztree, 1, 5);
//   bztree.Update(keys[2], payloads[0]);
//   bztree.Delete(keys[3]);

//   auto [rc, scan_results] = bztree.Scan(keys[2], true, keys[4], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   EXPECT_EQ(keys[2], CastToValue(scan_results[0].first.get()));
//   EXPECT_EQ(payloads[0], CastToValue(scan_results[0].second.get()));
//   EXPECT_EQ(keys[4], CastToValue(scan_results[1].first.get()));
//   EXPECT_EQ(payloads[4], CastToValue(scan_results[1].second.get()));
// }

/*--------------------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeUInt64Fixture, Write_TwoKeys_ReadWrittenValues)
{
  auto bztree = BzTree_t{};
  bztree.Write(keys[1], payloads[1]);
  bztree.Write(keys[2], payloads[2]);

  // read 1st input value
  std::tie(rc, record) = bztree.Read(keys[1]);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[1], record->GetPayload());

  // read 2nd input value
  std::tie(rc, record) = bztree.Read(keys[2]);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[2], record->GetPayload());
}

// TEST_F(BzTreeUInt64Fixture, Write_DuplicateKey_ReadLatestValue)
// {
//   auto bztree = BzTree_t{};
//   bztree.Write(keys[1], payloads[1]);
//   bztree.Write(keys[1], payloads[2]);

//   std::tie(rc, record) = bztree.Read(keys[1]);
//

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(payloads[2], record->GetPayload());
// }

// /*--------------------------------------------------------------------------------------------------
//  * Insert operation
//  *------------------------------------------------------------------------------------------------*/

// TEST_F(BzTreeUInt64Fixture, Insert_TwoKeys_ReadInsertedValues)
// {
//   auto bztree = BzTree_t{};
//   bztree.Insert(keys[1], payloads[1]);
//   bztree.Insert(keys[2], payloads[2]);

//   // read 1st input value
//   std::tie(rc, record) = bztree.Read(keys[1]);
//

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(payloads[1], record->GetPayload());

//   // read 2nd input value
//   std::tie(rc, record) = bztree.Read(keys[2]);
//

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(payloads[2], record->GetPayload());
// }

// TEST_F(BzTreeUInt64Fixture, Insert_DuplicateKey_InsertionFailed)
// {
//   auto bztree = BzTree_t{};
//   bztree.Insert(keys[1], payloads[1]);

//   auto rc = bztree.Insert(keys[1], payloads[1]);

//   EXPECT_EQ(ReturnCode::kKeyExist, rc);
// }

// /*--------------------------------------------------------------------------------------------------
//  * Update operation
//  *------------------------------------------------------------------------------------------------*/

// TEST_F(BzTreeUInt64Fixture, Update_SingleKey_ReadUpdatedValue)
// {
//   auto bztree = BzTree_t{};
//   bztree.Insert(keys[1], payloads[2]);
//   bztree.Update(keys[1], payloads[2]);

//   std::tie(rc, record) = bztree.Read(keys[1]);
//

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(payloads[2], record->GetPayload());
// }

// TEST_F(BzTreeUInt64Fixture, Update_NotPresentKey_UpdatedFailed)
// {
//   auto bztree = BzTree_t{};
//   auto rc = bztree.Update(keys[1], payloads[1]);

//   EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
// }

// TEST_F(BzTreeUInt64Fixture, Update_DeletedKey_UpdateFailed)
// {
//   auto bztree = BzTree_t{};
//   bztree.Insert(keys[1], payloads[2]);
//   bztree.Delete(keys[1]);
//   auto rc = bztree.Update(keys[1], payloads[2]);

//   EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
// }

// /*--------------------------------------------------------------------------------------------------
//  * Delete operation
//  *------------------------------------------------------------------------------------------------*/

// TEST_F(BzTreeUInt64Fixture, Delete_PresentKey_DeletionSucceed)
// {
//   auto bztree = BzTree_t{};
//   bztree.Insert(keys[1], payloads[1]);

//   auto rc = bztree.Delete(keys[1]);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
// }

// TEST_F(BzTreeUInt64Fixture, Delete_PresentKey_ReadFailed)
// {
//   auto bztree = BzTree_t{};
//   bztree.Insert(keys[1], payloads[1]);
//   bztree.Delete(keys[1]);

//   std::tie(rc, record) = bztree.Read(keys[1]);

//   EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
// }

// TEST_F(BzTreeUInt64Fixture, Delete_NotPresentKey_DeletionFailed)
// {
//   auto bztree = BzTree_t{};
//   auto rc = bztree.Delete(keys[1]);

//   EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
// }

// TEST_F(BzTreeUInt64Fixture, Delete_DeletedKey_DeletionFailed)
// {
//   auto bztree = BzTree_t{};
//   bztree.Insert(keys[1], payloads[1]);
//   bztree.Delete(keys[1]);

//   auto rc = bztree.Delete(keys[1]);

//   EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
// }

// /*--------------------------------------------------------------------------------------------------
//  * Split operation
//  *------------------------------------------------------------------------------------------------*/

// TEST_F(BzTreeUInt64Fixture, Split_OrderedKeyWrites_ReadWrittenKeys)
// {
//   auto bztree =
//       BzTree_t{kTestNodeSize,          kTestMinNodeSize,    kTestMinFreeSpace,
//                               kTestExpectedFreeSpace, kTestMaxDeletedSize, kTestMaxMergedSize};
//   const auto record_count = 1000;

//   std::thread{&BzTreeUInt64Fixture::WriteOrderedKeys, this, &bztree, 1, record_count}.join();

//   for (size_t index = 1; index <= record_count; ++index) {
//     std::tie(rc, record) = bztree.Read(key_ptrs[index]);
//     auto result = record->GetPayload();
//     EXPECT_EQ(ReturnCode::kSuccess, rc);
//     EXPECT_EQ(payloads[index], result);
//   }

//   auto [rc, scan_results] = bztree.Scan(keys[50], true, keys[100], true);
//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   auto index = 50UL;
//   for (auto&& [key, payload] : scan_results) {
//     EXPECT_EQ(keys[index], CastToValue(key.get()));
//     EXPECT_EQ(payloads[index++], CastToValue(payload.get()));
//   }
// }

// TEST_F(BzTreeUInt64Fixture, Split_OrderedKeyInserts_ReadInsertedKeys)
// {
//   auto bztree =
//       BzTree_t{kTestNodeSize,          kTestMinNodeSize,    kTestMinFreeSpace,
//                               kTestExpectedFreeSpace, kTestMaxDeletedSize, kTestMaxMergedSize};
//   const auto record_count = 1000;

//   std::thread{&BzTreeUInt64Fixture::InsertOrderedKeys, this, &bztree, 1, record_count}.join();

//   for (size_t index = 1; index <= record_count; ++index) {
//     std::tie(rc, record) = bztree.Read(key_ptrs[index]);
//     auto result = record->GetPayload();
//     EXPECT_EQ(ReturnCode::kSuccess, rc);
//     EXPECT_EQ(payloads[index], result);
//   }

//   auto [rc, scan_results] = bztree.Scan(keys[50], true, keys[100], true);
//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   auto index = 50UL;
//   for (auto&& [key, payload] : scan_results) {
//     EXPECT_EQ(keys[index], CastToValue(key.get()));
//     EXPECT_EQ(payloads[index++], CastToValue(payload.get()));
//   }
// }

// TEST_F(BzTreeUInt64Fixture, Split_OrderedKeyInsertsUpdates_ReadLatestKeys)
// {
//   auto bztree =
//       BzTree_t{kTestNodeSize,          kTestMinNodeSize,    kTestMinFreeSpace,
//                               kTestExpectedFreeSpace, kTestMaxDeletedSize, kTestMaxMergedSize};
//   const auto record_count = 1000;

//   std::thread{&BzTreeUInt64Fixture::InsertOrderedKeys, this, &bztree, 1, record_count}.join();
//   std::thread{&BzTreeUInt64Fixture::UpdateOrderedKeys, this, &bztree, 1, record_count}.join();

//   for (size_t index = 1; index <= record_count; ++index) {
//     std::tie(rc, record) = bztree.Read(key_ptrs[index]);
//     auto result = record->GetPayload();
//     EXPECT_EQ(ReturnCode::kSuccess, rc);
//     EXPECT_EQ(payloads[index + 1], result);
//   }

//   auto [rc, scan_results] = bztree.Scan(keys[50], true, keys[100], true);
//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   auto index = 50UL;
//   for (auto&& [key, payload] : scan_results) {
//     EXPECT_EQ(keys[index], CastToValue(key.get()));
//     EXPECT_EQ(payloads[++index], CastToValue(payload.get()));
//   }
// }

// /*--------------------------------------------------------------------------------------------------
//  * Merge operation
//  *------------------------------------------------------------------------------------------------*/

// TEST_F(BzTreeUInt64Fixture, Merge_OrderedKeyWritesDeletes_ReadRemainingKey)
// {
//   auto bztree =
//       BzTree_t{kTestNodeSize,          kTestMinNodeSize,    kTestMinFreeSpace,
//                               kTestExpectedFreeSpace, kTestMaxDeletedSize, kTestMaxMergedSize};
//   const auto record_count = 1000;

//   std::thread{&BzTreeUInt64Fixture::WriteOrderedKeys, this, &bztree, 1, record_count}.join();
//   std::thread{&BzTreeUInt64Fixture::DeleteOrderedKeys, this, &bztree, 2, record_count}.join();

//   std::tie(rc, record) = bztree.Read(keys[1]);
//   auto result = record->GetPayload();
//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(payloads[1], result);

//   for (size_t index = 2; index <= record_count; ++index) {
//     std::tie(rc, record) = bztree.Read(key_ptrs[index]);
//     EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
//   }
// }

}  // namespace dbgroup::index::bztree
