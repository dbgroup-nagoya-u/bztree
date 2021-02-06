// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "metadata.hpp"

#include <gtest/gtest.h>

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
  EXPECT_EQ(0, meta.GetControlBit());
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
  const auto test_meta = meta.InitForInsert(epoch);

  EXPECT_EQ(kWordLength, sizeof(test_meta));
  EXPECT_EQ(0, test_meta.GetControlBit());
  EXPECT_FALSE(test_meta.IsVisible());
  EXPECT_TRUE(test_meta.IsInProgress());
  EXPECT_EQ(epoch, test_meta.GetOffset());
  EXPECT_EQ(0, test_meta.GetKeyLength());
  EXPECT_EQ(0, test_meta.GetTotalLength());

  EXPECT_FALSE(test_meta.IsDeleted());
  EXPECT_FALSE(test_meta.IsCorrupted(epoch));
  EXPECT_TRUE(test_meta.IsCorrupted(different_epoch));
  EXPECT_EQ(0, test_meta.GetPayloadLength());
}

TEST_F(MetadataFixture, UpdateOffset_InitMeta_UpdateWithoutSideEffect)
{
  const Metadata meta;
  const auto offset = 256;
  const auto test_meta = meta.UpdateOffset(offset);

  EXPECT_EQ(kWordLength, sizeof(test_meta));
  EXPECT_EQ(0, test_meta.GetControlBit());
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
  EXPECT_EQ(0, test_meta.GetControlBit());
  EXPECT_TRUE(test_meta.IsVisible());
  EXPECT_FALSE(test_meta.IsInProgress());
  EXPECT_EQ(offset, test_meta.GetOffset());
  EXPECT_EQ(key_length, test_meta.GetKeyLength());
  EXPECT_EQ(total_length, test_meta.GetTotalLength());

  EXPECT_FALSE(test_meta.IsDeleted());
  EXPECT_FALSE(test_meta.IsCorrupted(epoch));
  EXPECT_EQ(total_length - key_length, test_meta.GetPayloadLength());
}

TEST_F(MetadataFixture, SetRecordInfo_InsertedMeta_SetWithoutSideEffect)
{
  const Metadata meta;
  const auto epoch = 0;
  const auto offset = 256, key_length = 16, total_length = 32;
  const auto test_meta = meta.InitForInsert(epoch).SetRecordInfo(offset, key_length, total_length);

  EXPECT_EQ(kWordLength, sizeof(test_meta));
  EXPECT_EQ(0, test_meta.GetControlBit());
  EXPECT_TRUE(test_meta.IsVisible());
  EXPECT_FALSE(test_meta.IsInProgress());
  EXPECT_EQ(offset, test_meta.GetOffset());
  EXPECT_EQ(key_length, test_meta.GetKeyLength());
  EXPECT_EQ(total_length, test_meta.GetTotalLength());

  EXPECT_FALSE(test_meta.IsDeleted());
  EXPECT_FALSE(test_meta.IsCorrupted(epoch));
  EXPECT_EQ(total_length - key_length, test_meta.GetPayloadLength());
}

TEST_F(MetadataFixture, DeletePayload_InitMeta_DeleteWithoutSideEffect)
{
  const Metadata meta;
  const auto epoch = 0;
  const auto offset = 256, key_length = 16, total_length = 32;
  const auto test_meta =
      meta.InitForInsert(epoch).SetRecordInfo(offset, key_length, total_length).DeleteRecordInfo();

  EXPECT_EQ(kWordLength, sizeof(test_meta));
  EXPECT_EQ(0, test_meta.GetControlBit());
  EXPECT_FALSE(test_meta.IsVisible());
  EXPECT_FALSE(test_meta.IsInProgress());
  EXPECT_EQ(offset, test_meta.GetOffset());
  EXPECT_EQ(key_length, test_meta.GetKeyLength());
  EXPECT_EQ(total_length, test_meta.GetTotalLength());

  EXPECT_TRUE(test_meta.IsDeleted());
  EXPECT_FALSE(test_meta.IsCorrupted(epoch));
}

TEST_F(MetadataFixture, Union_DefaultConstructor_CorrectlyInitialized)
{
  const MetaUnion meta_union;
  const auto union_addr = reinterpret_cast<uint64_t>(&meta_union);
  const auto meta_addr = reinterpret_cast<uint64_t>(&meta_union.meta);
  const auto int_addr = reinterpret_cast<uint64_t>(&meta_union.int_meta);

  EXPECT_EQ(kWordLength, sizeof(meta_union));
  EXPECT_EQ(union_addr, meta_addr);
  EXPECT_EQ(union_addr, int_addr);

  const Metadata meta = meta_union.meta;

  EXPECT_EQ(0, meta.GetControlBit());
  EXPECT_FALSE(meta.IsVisible());
  EXPECT_FALSE(meta.IsInProgress());
  EXPECT_EQ(0, meta.GetOffset());
  EXPECT_EQ(0, meta.GetKeyLength());
  EXPECT_EQ(0, meta.GetTotalLength());
}

TEST_F(MetadataFixture, Union_CopyByInt_HasSameInformation)
{
  const auto epoch = 0;
  const auto offset = 256, key_length = 16, total_length = 32;

  MetaUnion meta_union, comp_union;
  meta_union.meta =
      meta_union.meta.InitForInsert(epoch).SetRecordInfo(offset, key_length, total_length);
  comp_union.int_meta = meta_union.int_meta;

  EXPECT_EQ(meta_union.meta.GetControlBit(), comp_union.meta.GetControlBit());
  EXPECT_EQ(meta_union.meta.IsVisible(), comp_union.meta.IsVisible());
  EXPECT_EQ(meta_union.meta.IsInProgress(), comp_union.meta.IsInProgress());
  EXPECT_EQ(meta_union.meta.GetOffset(), comp_union.meta.GetOffset());
  EXPECT_EQ(meta_union.meta.GetKeyLength(), comp_union.meta.GetKeyLength());
  EXPECT_EQ(meta_union.meta.GetTotalLength(), comp_union.meta.GetTotalLength());
}

}  // namespace bztree
