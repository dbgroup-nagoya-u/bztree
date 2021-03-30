// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "bztree/components/metadata.hpp"

#include "gtest/gtest.h"

namespace dbgroup::index::bztree
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
  EXPECT_FALSE(meta.IsVisible());
  EXPECT_FALSE(meta.IsInProgress());
  EXPECT_EQ(0, meta.GetOffset());
  EXPECT_EQ(0, meta.GetKeyLength());
  EXPECT_EQ(0, meta.GetTotalLength());
}

TEST_F(MetadataFixture, InitForInsert_InitMeta_InitWithoutSideEffect)
{
  const Metadata meta;
  const auto epoch = 256, different_epoch = 512;
  const auto test_meta = Metadata::GetInsertingMeta(epoch);

  EXPECT_EQ(kWordLength, sizeof(test_meta));
  EXPECT_FALSE(test_meta.IsVisible());
  EXPECT_TRUE(test_meta.IsInProgress());
  EXPECT_EQ(epoch, test_meta.GetOffset());
  EXPECT_EQ(0, test_meta.GetKeyLength());
  EXPECT_EQ(0, test_meta.GetTotalLength());

  EXPECT_FALSE(test_meta.IsDeleted());
  EXPECT_FALSE(test_meta.IsFailedRecord(epoch));
  EXPECT_TRUE(test_meta.IsFailedRecord(different_epoch));
  EXPECT_EQ(0, test_meta.GetPayloadLength());
}

TEST_F(MetadataFixture, UpdateOffset_InitMeta_UpdateWithoutSideEffect)
{
  const Metadata meta;
  const auto offset = 256;
  const auto test_meta = meta.UpdateOffset(offset);

  EXPECT_EQ(kWordLength, sizeof(test_meta));
  EXPECT_FALSE(test_meta.IsVisible());
  EXPECT_FALSE(test_meta.IsInProgress());
  EXPECT_EQ(offset, test_meta.GetOffset());
  EXPECT_EQ(0, test_meta.GetKeyLength());
  EXPECT_EQ(0, test_meta.GetTotalLength());
}

TEST_F(MetadataFixture, SetRecordInfo_InitMeta_SetWithoutSideEffect)
{
  const Metadata meta;
  const auto epoch = 0;
  const auto offset = 256, key_length = 16, total_length = 32;
  const auto test_meta = meta.SetRecordInfo(offset, key_length, total_length);

  EXPECT_EQ(kWordLength, sizeof(test_meta));
  EXPECT_TRUE(test_meta.IsVisible());
  EXPECT_FALSE(test_meta.IsInProgress());
  EXPECT_EQ(offset, test_meta.GetOffset());
  EXPECT_EQ(key_length, test_meta.GetKeyLength());
  EXPECT_EQ(total_length, test_meta.GetTotalLength());

  EXPECT_FALSE(test_meta.IsDeleted());
  EXPECT_FALSE(test_meta.IsFailedRecord(epoch));
  EXPECT_EQ(total_length - key_length, test_meta.GetPayloadLength());
}

TEST_F(MetadataFixture, SetRecordInfo_InsertedMeta_SetWithoutSideEffect)
{
  const Metadata meta;
  const auto epoch = 0;
  const auto offset = 256, key_length = 16, total_length = 32;
  const auto test_meta =
      Metadata::GetInsertingMeta(epoch).SetRecordInfo(offset, key_length, total_length);

  EXPECT_EQ(kWordLength, sizeof(test_meta));
  EXPECT_TRUE(test_meta.IsVisible());
  EXPECT_FALSE(test_meta.IsInProgress());
  EXPECT_EQ(offset, test_meta.GetOffset());
  EXPECT_EQ(key_length, test_meta.GetKeyLength());
  EXPECT_EQ(total_length, test_meta.GetTotalLength());

  EXPECT_FALSE(test_meta.IsDeleted());
  EXPECT_FALSE(test_meta.IsFailedRecord(epoch));
  EXPECT_EQ(total_length - key_length, test_meta.GetPayloadLength());
}

TEST_F(MetadataFixture, DeletePayload_InitMeta_DeleteWithoutSideEffect)
{
  const Metadata meta;
  const auto epoch = 0;
  const auto offset = 256, key_length = 16, total_length = 32;
  const auto test_meta = Metadata::GetInsertingMeta(epoch)
                             .SetRecordInfo(offset, key_length, total_length)
                             .DeleteRecordInfo();

  EXPECT_EQ(kWordLength, sizeof(test_meta));
  EXPECT_FALSE(test_meta.IsVisible());
  EXPECT_FALSE(test_meta.IsInProgress());
  EXPECT_EQ(offset, test_meta.GetOffset());
  EXPECT_EQ(key_length, test_meta.GetKeyLength());
  EXPECT_EQ(total_length, test_meta.GetTotalLength());

  EXPECT_TRUE(test_meta.IsDeleted());
  EXPECT_FALSE(test_meta.IsFailedRecord(epoch));
}

}  // namespace dbgroup::index::bztree
