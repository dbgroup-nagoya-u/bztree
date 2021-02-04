// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "bztree.hpp"

#include <gtest/gtest.h>

#include <memory>

using std::byte;

namespace bztree
{
class BzTreeUInt64Fixture : public testing::Test
{
 public:
  static constexpr size_t kDefaultNodeSize = 256;
  static constexpr size_t kDefaultBlockSizeThreshold = 256;
  static constexpr size_t kDefaultDeletedSizeThreshold = 256;
  static constexpr size_t kDefaultMinNodeSizeThreshold = 128;
  static constexpr size_t kIndexEpoch = 0;
  static constexpr size_t kKeyNumForTest = 100;

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
    pmwcas::Thread::ClearRegistry();
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
      //
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
      //
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

}  // namespace bztree
