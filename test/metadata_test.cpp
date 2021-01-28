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

}  // namespace bztree
