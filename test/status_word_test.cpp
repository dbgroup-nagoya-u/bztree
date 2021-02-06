// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "status_word.hpp"

#include <gtest/gtest.h>

namespace bztree
{
class StatusWordFixture : public testing::Test
{
 protected:
  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }
};

TEST_F(StatusWordFixture, New_DefaultConstructor_CorrectlyInitialized)
{
  const StatusWord status;

  EXPECT_EQ(kWordLength, sizeof(status));
  EXPECT_EQ(0, status.GetControlBit());
  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(0, status.GetRecordCount());
  EXPECT_EQ(0, status.GetBlockSize());
  EXPECT_EQ(0, status.GetDeletedSize());
}

TEST_F(StatusWordFixture, Freeze_InitialStatus_FreezeWithoutSideEffect)
{
  const StatusWord status;
  const auto frozen_status = status.Freeze();

  EXPECT_EQ(kWordLength, sizeof(frozen_status));
  EXPECT_EQ(0, frozen_status.GetControlBit());
  EXPECT_TRUE(frozen_status.IsFrozen());
  EXPECT_EQ(0, frozen_status.GetRecordCount());
  EXPECT_EQ(0, frozen_status.GetBlockSize());
  EXPECT_EQ(0, frozen_status.GetDeletedSize());
}

TEST_F(StatusWordFixture, AddRecordInfo_InitialStatus_AddInfoWithoutSideEffect)
{
  const StatusWord status;
  const size_t record_count = 16, block_size = 8, deleted_size = 4;
  const auto updated_status = status.AddRecordInfo(record_count, block_size, deleted_size);

  EXPECT_EQ(kWordLength, sizeof(updated_status));
  EXPECT_EQ(0, updated_status.GetControlBit());
  EXPECT_FALSE(updated_status.IsFrozen());
  EXPECT_EQ(record_count, updated_status.GetRecordCount());
  EXPECT_EQ(block_size, updated_status.GetBlockSize());
  EXPECT_EQ(deleted_size, updated_status.GetDeletedSize());
  EXPECT_EQ(kHeaderLength + (kWordLength * 16) + block_size, updated_status.GetOccupiedSize());
  EXPECT_EQ((kWordLength * 16) + block_size - deleted_size, updated_status.GetLiveDataSize());
}

TEST_F(StatusWordFixture, Union_DefaultConstructor_CorrectTypeInfomation)
{
  StatusUnion union_status;

  EXPECT_EQ(kWordLength, sizeof(union_status));

  const auto union_addr = reinterpret_cast<uint64_t>(&union_status);
  const auto status_addr = reinterpret_cast<uint64_t>(&union_status.word);
  const auto int_addr = reinterpret_cast<uint64_t>(&union_status.int_word);

  EXPECT_EQ(union_addr, status_addr);
  EXPECT_EQ(union_addr, int_addr);
}

TEST_F(StatusWordFixture, Union_CopyByInt_ReadSameInfomation)
{
  StatusUnion union_status, comp_status;
  const size_t record_count = 1, block_size = 3, deleted_size = 7;
  union_status.word = union_status.word.AddRecordInfo(record_count, block_size, deleted_size);
  comp_status.int_word = union_status.int_word;

  EXPECT_EQ(0, comp_status.word.GetControlBit());
  EXPECT_FALSE(comp_status.word.IsFrozen());
  EXPECT_EQ(record_count, comp_status.word.GetRecordCount());
  EXPECT_EQ(block_size, comp_status.word.GetBlockSize());
  EXPECT_EQ(deleted_size, comp_status.word.GetDeletedSize());
}

}  // namespace bztree
