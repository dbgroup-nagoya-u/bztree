// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include <gtest/gtest.h>

#include "bztree.hpp"

namespace bztree
{
class MetadataFixture : public testing::Test
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

TEST_F(MetadataFixture, New_DefaultConstructor_CorrectlyInitialized)
{
  const Metadata meta;

  EXPECT_EQ(kWordLength, sizeof(meta));
  // EXPECT_EQ(0, meta.GetControlBit());
  // EXPECT_FALSE(meta.IsFrozen());
  // EXPECT_EQ(0, meta.GetRecordCount());
  // EXPECT_EQ(0, meta.GetBlockSize());
  // EXPECT_EQ(0, meta.GetDeletedSize());
}

// TEST_F(MetadataFixture, Freeze_InitialStatus_FreezeWithoutSideEffect)
// {
//   const Metadata meta;
//   const auto frozen_meta = meta.Freeze();

//   EXPECT_EQ(kWordLength, sizeof(frozen_meta));
//   EXPECT_EQ(0, frozen_meta.GetControlBit());
//   EXPECT_TRUE(frozen_meta.IsFrozen());
//   EXPECT_EQ(0, frozen_meta.GetRecordCount());
//   EXPECT_EQ(0, frozen_meta.GetBlockSize());
//   EXPECT_EQ(0, frozen_meta.GetDeletedSize());
// }

// TEST_F(MetadataFixture, AddRecordInfo_InitialStatus_AddInfoWithoutSideEffect)
// {
//   const Metadata meta;
//   const size_t record_count = 16, block_size = 8, deleted_size = 4;
//   const auto updated_meta = meta.AddRecordInfo(record_count, block_size, deleted_size);

//   EXPECT_EQ(kWordLength, sizeof(updated_meta));
//   EXPECT_EQ(0, updated_meta.GetControlBit());
//   EXPECT_FALSE(updated_meta.IsFrozen());
//   EXPECT_EQ(record_count, updated_meta.GetRecordCount());
//   EXPECT_EQ(block_size, updated_meta.GetBlockSize());
//   EXPECT_EQ(deleted_size, updated_meta.GetDeletedSize());
// }

}  // namespace bztree
