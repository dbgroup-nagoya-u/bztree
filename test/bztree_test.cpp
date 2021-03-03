// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "bztree/bztree.hpp"

#include <gtest/gtest.h>

#include <memory>

using std::byte;

namespace dbgroup::index::bztree
{
class BzTreeUInt64Fixture : public testing::Test
{
 public:
  static constexpr size_t kTestNodeSize = 256;          // a node can have 10 records (16 + 24 * 10)
  static constexpr size_t kTestMinNodeSize = 89;        // a node with 3 records invokes merging
  static constexpr size_t kTestMinFreeSpace = 24;       // keep free space with 1 record size
  static constexpr size_t kTestExpectedFreeSpace = 72;  // expect free space can write 3 records
  static constexpr size_t kTestMaxDeletedSize = 119;    // consolidate when 5 records are deleted
  static constexpr size_t kTestMaxMergedSize = 137;     // a merged node has space for 5 records
  static constexpr size_t kIndexEpoch = 0;
  static constexpr size_t kKeyNumForTest = 2000;

  uint64_t keys[kKeyNumForTest];
  uint64_t* key_ptrs[kKeyNumForTest];
  uint64_t key_lengths[kKeyNumForTest];
  uint64_t payloads[kKeyNumForTest];
  uint64_t* payload_ptrs[kKeyNumForTest];
  uint64_t payload_lengths[kKeyNumForTest];
  uint64_t key_null = 0;  // null key must have 8 bytes to fill a node
  uint64_t* key_null_ptr = &key_null;
  size_t key_length_null = kWordLength;  // null key must have 8 bytes to fill a node
  uint64_t payload_null = 0;             // null payload must have 8 bytes to fill a node
  uint64_t* payload_null_ptr = &payload_null;
  size_t payload_length_null = kWordLength;  // null payload must have 8 bytes to fill a node

  std::unique_ptr<BzTree<CompareAsUInt64>> bztree;

  CompareAsUInt64 comp{};

 protected:
  void
  SetUp() override
  {
    bztree.reset(new BzTree<CompareAsUInt64>{});

    for (uint64_t index = 0; index < kKeyNumForTest; index++) {
      keys[index] = index;
      key_ptrs[index] = &keys[index];
      key_lengths[index] = kWordLength;
      payloads[index] = index;
      payload_ptrs[index] = &payloads[index];
      payload_lengths[index] = kWordLength;
    }
  }

  void
  TearDown() override
  {
  }

  constexpr uint64_t
  CastToValue(const void* target_addr)
  {
    return *BitCast<uint64_t*>(target_addr);
  }

  void
  WriteNullKey(const size_t write_num)
  {
    for (size_t index = 0; index < write_num; ++index) {
      bztree->Write(key_null_ptr, key_length_null, payload_null_ptr, payload_length_null);
    }
  }

  void
  WriteOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      auto key_ptr = key_ptrs[index];
      auto key_length = key_lengths[index];
      auto payload_ptr = payload_ptrs[index];
      auto payload_length = payload_lengths[index];
      bztree->Write(key_ptr, key_length, payload_ptr, payload_length);
    }
  }

  void
  InsertOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      auto key_ptr = key_ptrs[index];
      auto key_length = key_lengths[index];
      auto payload_ptr = payload_ptrs[index];
      auto payload_length = payload_lengths[index];
      bztree->Insert(key_ptr, key_length, payload_ptr, payload_length);
    }
  }

  void
  UpdateOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index + 1 < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      auto key_ptr = key_ptrs[index];
      auto key_length = key_lengths[index];
      auto payload_ptr = payload_ptrs[index + 1];
      auto payload_length = payload_lengths[index + 1];
      bztree->Update(key_ptr, key_length, payload_ptr, payload_length);
    }
  }

  void
  DeleteOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      auto key_ptr = key_ptrs[index];
      auto key_length = key_lengths[index];
      bztree->Delete(key_ptr, key_length);
    }
  }
};

/*--------------------------------------------------------------------------------------------------
 * Read operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeUInt64Fixture, Read_NotPresentKey_ReadFailed)
{
  auto [rc, u_ptr] = bztree->Read(key_ptrs[1]);

  EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Scan operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeUInt64Fixture, Scan_EmptyNode_NoResult)
{
  auto [rc, scan_results] = bztree->Scan(key_ptrs[1], true, key_ptrs[10], true);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(0, scan_results.size());
}

TEST_F(BzTreeUInt64Fixture, Scan_BothClosed_ScanTargetValues)
{
  WriteOrderedKeys(1, 10);

  auto [rc, scan_results] = bztree->Scan(key_ptrs[4], true, key_ptrs[6], true);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(3, scan_results.size());
  EXPECT_EQ(keys[4], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[4], CastToValue(scan_results[0].second.get()));
  EXPECT_EQ(keys[5], CastToValue(scan_results[1].first.get()));
  EXPECT_EQ(payloads[5], CastToValue(scan_results[1].second.get()));
  EXPECT_EQ(keys[6], CastToValue(scan_results[2].first.get()));
  EXPECT_EQ(payloads[6], CastToValue(scan_results[2].second.get()));
}

TEST_F(BzTreeUInt64Fixture, Scan_LeftClosed_ScanTargetValues)
{
  WriteOrderedKeys(1, 10);

  auto [rc, scan_results] = bztree->Scan(key_ptrs[8], true, key_ptrs[10], false);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  EXPECT_EQ(keys[8], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[8], CastToValue(scan_results[0].second.get()));
  EXPECT_EQ(keys[9], CastToValue(scan_results[1].first.get()));
  EXPECT_EQ(payloads[9], CastToValue(scan_results[1].second.get()));
}

TEST_F(BzTreeUInt64Fixture, Scan_RightClosed_ScanTargetValues)
{
  WriteOrderedKeys(1, 10);

  auto [rc, scan_results] = bztree->Scan(key_ptrs[8], false, key_ptrs[10], true);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  EXPECT_EQ(keys[9], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[9], CastToValue(scan_results[0].second.get()));
  EXPECT_EQ(keys[10], CastToValue(scan_results[1].first.get()));
  EXPECT_EQ(payloads[10], CastToValue(scan_results[1].second.get()));
}

TEST_F(BzTreeUInt64Fixture, Scan_BothOpened_ScanTargetValues)
{
  WriteOrderedKeys(1, 10);

  auto [rc, scan_results] = bztree->Scan(key_ptrs[8], false, key_ptrs[10], false);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(1, scan_results.size());
  EXPECT_EQ(keys[9], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[9], CastToValue(scan_results[0].second.get()));
}

TEST_F(BzTreeUInt64Fixture, Scan_LeftInfinity_ScanTargetValues)
{
  WriteOrderedKeys(1, 10);

  auto [rc, scan_results] = bztree->Scan(nullptr, false, key_ptrs[2], true);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  EXPECT_EQ(keys[1], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[1], CastToValue(scan_results[0].second.get()));
  EXPECT_EQ(keys[2], CastToValue(scan_results[1].first.get()));
  EXPECT_EQ(payloads[2], CastToValue(scan_results[1].second.get()));
}

TEST_F(BzTreeUInt64Fixture, Scan_RightInfinity_ScanTargetValues)
{
  WriteOrderedKeys(1, 10);

  auto [rc, scan_results] = bztree->Scan(key_ptrs[9], true, nullptr, false);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  EXPECT_EQ(keys[9], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[9], CastToValue(scan_results[0].second.get()));
  EXPECT_EQ(keys[10], CastToValue(scan_results[1].first.get()));
  EXPECT_EQ(payloads[10], CastToValue(scan_results[1].second.get()));
}

TEST_F(BzTreeUInt64Fixture, Scan_LeftOutsideRange_NoResults)
{
  WriteOrderedKeys(5, 10);

  auto [rc, scan_results] = bztree->Scan(nullptr, false, key_ptrs[3], false);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(0, scan_results.size());
}

TEST_F(BzTreeUInt64Fixture, Scan_RightOutsideRange_NoResults)
{
  WriteOrderedKeys(1, 4);

  auto [rc, scan_results] = bztree->Scan(key_ptrs[5], false, nullptr, false);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(0, scan_results.size());
}

TEST_F(BzTreeUInt64Fixture, Scan_WithUpdateDelete_ScanLatestValues)
{
  WriteOrderedKeys(1, 5);
  bztree->Update(key_ptrs[2], key_lengths[2], payload_ptrs[0], payload_lengths[0]);
  bztree->Delete(key_ptrs[3], key_lengths[3]);

  auto [rc, scan_results] = bztree->Scan(key_ptrs[2], true, key_ptrs[4], true);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(2, scan_results.size());
  EXPECT_EQ(keys[2], CastToValue(scan_results[0].first.get()));
  EXPECT_EQ(payloads[0], CastToValue(scan_results[0].second.get()));
  EXPECT_EQ(keys[4], CastToValue(scan_results[1].first.get()));
  EXPECT_EQ(payloads[4], CastToValue(scan_results[1].second.get()));
}

/*--------------------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeUInt64Fixture, Write_TwoKeys_ReadWrittenValues)
{
  bztree->Write(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1]);
  bztree->Write(key_ptrs[2], key_lengths[2], payload_ptrs[2], payload_lengths[2]);

  // read 1st input value
  auto [rc, u_ptr] = bztree->Read(key_ptrs[1]);
  auto read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[1], read_result);

  // read 2nd input value
  std::tie(rc, u_ptr) = bztree->Read(key_ptrs[2]);
  read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[2], read_result);
}

TEST_F(BzTreeUInt64Fixture, Write_DuplicateKey_ReadLatestValue)
{
  bztree->Write(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1]);
  bztree->Write(key_ptrs[1], key_lengths[1], payload_ptrs[2], payload_lengths[2]);

  auto [rc, u_ptr] = bztree->Read(key_ptrs[1]);
  auto read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[2], read_result);
}

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeUInt64Fixture, Insert_TwoKeys_ReadInsertedValues)
{
  bztree->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1]);
  bztree->Insert(key_ptrs[2], key_lengths[2], payload_ptrs[2], payload_lengths[2]);

  // read 1st input value
  auto [rc, u_ptr] = bztree->Read(key_ptrs[1]);
  auto read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[1], read_result);

  // read 2nd input value
  std::tie(rc, u_ptr) = bztree->Read(key_ptrs[2]);
  read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[2], read_result);
}

TEST_F(BzTreeUInt64Fixture, Insert_DuplicateKey_InsertionFailed)
{
  bztree->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1]);

  auto rc = bztree->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1]);

  EXPECT_EQ(ReturnCode::kKeyExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeUInt64Fixture, Update_SingleKey_ReadUpdatedValue)
{
  bztree->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[2], payload_lengths[2]);
  bztree->Update(key_ptrs[1], key_lengths[1], payload_ptrs[2], payload_lengths[2]);

  auto [rc, u_ptr] = bztree->Read(key_ptrs[1]);
  auto read_result = CastToValue(u_ptr.get());

  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[2], read_result);
}

TEST_F(BzTreeUInt64Fixture, Update_NotPresentKey_UpdatedFailed)
{
  auto rc = bztree->Update(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1]);

  EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
}

TEST_F(BzTreeUInt64Fixture, Update_DeletedKey_UpdateFailed)
{
  bztree->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[2], payload_lengths[2]);
  bztree->Delete(key_ptrs[1], key_lengths[1]);
  auto rc = bztree->Update(key_ptrs[1], key_lengths[1], payload_ptrs[2], payload_lengths[2]);

  EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeUInt64Fixture, Delete_PresentKey_DeletionSucceed)
{
  bztree->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1]);

  auto rc = bztree->Delete(key_ptrs[1], key_lengths[1]);

  EXPECT_EQ(ReturnCode::kSuccess, rc);
}

TEST_F(BzTreeUInt64Fixture, Delete_PresentKey_ReadFailed)
{
  bztree->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1]);
  bztree->Delete(key_ptrs[1], key_lengths[1]);

  auto [rc, u_ptr] = bztree->Read(key_ptrs[1]);

  EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
}

TEST_F(BzTreeUInt64Fixture, Delete_NotPresentKey_DeletionFailed)
{
  auto rc = bztree->Delete(key_ptrs[1], key_lengths[1]);

  EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
}

TEST_F(BzTreeUInt64Fixture, Delete_DeletedKey_DeletionFailed)
{
  bztree->Insert(key_ptrs[1], key_lengths[1], payload_ptrs[1], payload_lengths[1]);
  bztree->Delete(key_ptrs[1], key_lengths[1]);

  auto rc = bztree->Delete(key_ptrs[1], key_lengths[1]);

  EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
}

/*--------------------------------------------------------------------------------------------------
 * Split operation
 *------------------------------------------------------------------------------------------------*/

TEST_F(BzTreeUInt64Fixture, Split_OrderedKeyWrites_ReadWrittenKeys)
{
  const auto record_count = 1000;
  bztree.release();
  bztree.reset(new BzTree<CompareAsUInt64>{kTestNodeSize, kTestMinNodeSize, kTestMinFreeSpace,
                                           kTestExpectedFreeSpace, kTestMaxDeletedSize,
                                           kTestMaxMergedSize});

  WriteOrderedKeys(1, record_count);

  for (size_t index = 1; index <= record_count; ++index) {
    auto [rc, u_ptr] = bztree->Read(key_ptrs[index]);
    auto result = CastToValue(u_ptr.get());
    EXPECT_EQ(ReturnCode::kSuccess, rc);
    EXPECT_EQ(payloads[index], result);
  }

  auto [rc, scan_results] = bztree->Scan(key_ptrs[50], true, key_ptrs[100], true);
  EXPECT_EQ(ReturnCode::kSuccess, rc);
  auto index = 50UL;
  for (auto&& [key, payload] : scan_results) {
    EXPECT_EQ(keys[index], CastToValue(key.get()));
    EXPECT_EQ(payloads[index++], CastToValue(payload.get()));
  }
}

TEST_F(BzTreeUInt64Fixture, Split_OrderedKeyInserts_ReadInsertedKeys)
{
  const auto record_count = 1000;
  bztree.release();
  bztree.reset(new BzTree<CompareAsUInt64>{kTestNodeSize, kTestMinNodeSize, kTestMinFreeSpace,
                                           kTestExpectedFreeSpace, kTestMaxDeletedSize,
                                           kTestMaxMergedSize});

  InsertOrderedKeys(1, record_count);

  for (size_t index = 1; index <= record_count; ++index) {
    auto [rc, u_ptr] = bztree->Read(key_ptrs[index]);
    auto result = CastToValue(u_ptr.get());
    EXPECT_EQ(ReturnCode::kSuccess, rc);
    EXPECT_EQ(payloads[index], result);
  }

  auto [rc, scan_results] = bztree->Scan(key_ptrs[50], true, key_ptrs[100], true);
  EXPECT_EQ(ReturnCode::kSuccess, rc);
  auto index = 50UL;
  for (auto&& [key, payload] : scan_results) {
    EXPECT_EQ(keys[index], CastToValue(key.get()));
    EXPECT_EQ(payloads[index++], CastToValue(payload.get()));
  }
}

TEST_F(BzTreeUInt64Fixture, Split_OrderedKeyInsertsUpdates_ReadLatestKeys)
{
  const auto record_count = 1000;
  bztree.release();
  bztree.reset(new BzTree<CompareAsUInt64>{kTestNodeSize, kTestMinNodeSize, kTestMinFreeSpace,
                                           kTestExpectedFreeSpace, kTestMaxDeletedSize,
                                           kTestMaxMergedSize});

  InsertOrderedKeys(1, record_count);
  UpdateOrderedKeys(1, record_count);

  for (size_t index = 1; index <= record_count; ++index) {
    auto [rc, u_ptr] = bztree->Read(key_ptrs[index]);
    auto result = CastToValue(u_ptr.get());
    EXPECT_EQ(ReturnCode::kSuccess, rc);
    EXPECT_EQ(payloads[index + 1], result);
  }

  auto [rc, scan_results] = bztree->Scan(key_ptrs[50], true, key_ptrs[100], true);
  EXPECT_EQ(ReturnCode::kSuccess, rc);
  auto index = 50UL;
  for (auto&& [key, payload] : scan_results) {
    EXPECT_EQ(keys[index], CastToValue(key.get()));
    EXPECT_EQ(payloads[++index], CastToValue(payload.get()));
  }
}

TEST_F(BzTreeUInt64Fixture, Merge_OrderedKeyWritesDeletes_ReadRemainingKey)
{
  const auto record_count = 1000;
  bztree.release();
  bztree.reset(new BzTree<CompareAsUInt64>{kTestNodeSize, kTestMinNodeSize, kTestMinFreeSpace,
                                           kTestExpectedFreeSpace, kTestMaxDeletedSize,
                                           kTestMaxMergedSize});

  WriteOrderedKeys(1, record_count);
  DeleteOrderedKeys(2, record_count);

  auto [rc, u_ptr] = bztree->Read(key_ptrs[1]);
  auto result = CastToValue(u_ptr.get());
  EXPECT_EQ(ReturnCode::kSuccess, rc);
  EXPECT_EQ(payloads[1], result);

  for (size_t index = 2; index <= record_count; ++index) {
    std::tie(rc, u_ptr) = bztree->Read(key_ptrs[index]);
    EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
  }
}

}  // namespace dbgroup::index::bztree
