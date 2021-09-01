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

#include "bztree/component/metadata.hpp"

#include "gtest/gtest.h"

namespace dbgroup::index::bztree::component::test
{
class MetadataFixture : public testing::Test
{
 protected:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr uint64_t kControlBitsMask = 7UL << 61;
  static constexpr size_t kExpectedOffset = 256;
  static constexpr size_t kExpectedKeyLength = 8;
  static constexpr size_t kExpectedTotalLength = 16;
  static constexpr size_t kCurrentEpoch = 1;
  static constexpr size_t kDummyEpoch = 2;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }

  /*################################################################################################
   * Utility functions
   *##############################################################################################*/

  Metadata
  CreateActiveMeta()
  {
    return Metadata{kExpectedOffset, kExpectedKeyLength, kExpectedTotalLength, false};
  }

  Metadata
  CreateInProgressMeta()
  {
    return Metadata{kCurrentEpoch, kExpectedKeyLength, kExpectedTotalLength, true};
  }
};

/*--------------------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(MetadataFixture, Construct_NoArguments_CorrectlyInitialized)
{
  const Metadata meta{};

  EXPECT_FALSE(meta.IsVisible());
  EXPECT_FALSE(meta.IsInProgress());
  EXPECT_EQ(0, meta.GetOffset());
  EXPECT_EQ(0, meta.GetKeyLength());
  EXPECT_EQ(0, meta.GetTotalLength());

  // check control bits
  uint64_t meta_uint;
  memcpy(&meta_uint, &meta, sizeof(Metadata));
  EXPECT_EQ(0, meta_uint & kControlBitsMask);
}

TEST_F(MetadataFixture, Construct_WithArguments_CorrectlyInitialized)
{
  auto meta = CreateActiveMeta();

  EXPECT_TRUE(meta.IsVisible());
  EXPECT_FALSE(meta.IsInProgress());
  EXPECT_EQ(kExpectedOffset, meta.GetOffset());
  EXPECT_EQ(kExpectedKeyLength, meta.GetKeyLength());
  EXPECT_EQ(kExpectedTotalLength, meta.GetTotalLength());

  // check control bits
  uint64_t meta_uint;
  memcpy(&meta_uint, &meta, sizeof(Metadata));
  EXPECT_EQ(0, meta_uint & kControlBitsMask);

  meta = CreateInProgressMeta();

  EXPECT_FALSE(meta.IsVisible());
  EXPECT_TRUE(meta.IsInProgress());
  EXPECT_EQ(kCurrentEpoch, meta.GetOffset());
  EXPECT_EQ(kExpectedKeyLength, meta.GetKeyLength());
  EXPECT_EQ(kExpectedTotalLength, meta.GetTotalLength());

  // check control bits
  memcpy(&meta_uint, &meta, sizeof(Metadata));
  EXPECT_EQ(0, meta_uint & kControlBitsMask);
}

/*--------------------------------------------------------------------------------------------------
 * Getter tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(MetadataFixture, IsDeleted_ActiveRecord_ReturnFalse)
{
  EXPECT_FALSE(CreateActiveMeta().IsDeleted());
}

TEST_F(MetadataFixture, IsDeleted_DeletedRecord_ReturnTrue)
{
  EXPECT_TRUE(CreateActiveMeta().Delete().IsDeleted());
}

TEST_F(MetadataFixture, IsDeleted_InProgressRecord_ReturnFalse)
{
  EXPECT_FALSE(CreateInProgressMeta().IsDeleted());
}

TEST_F(MetadataFixture, IsFailedRecord_ActiveRecord_ReturnFalse)
{
  EXPECT_FALSE(CreateActiveMeta().IsFailedRecord(kCurrentEpoch));
}

TEST_F(MetadataFixture, IsFailedRecord_DeletedRecord_ReturnFalse)
{
  EXPECT_FALSE(CreateActiveMeta().Delete().IsFailedRecord(kCurrentEpoch));
}

TEST_F(MetadataFixture, IsFailedRecord_InProgressRecordWithSameEpoch_ReturnFalse)
{
  EXPECT_FALSE(CreateInProgressMeta().IsFailedRecord(kCurrentEpoch));
}

TEST_F(MetadataFixture, IsFailedRecord_InProgressRecordWithDifferentEpoch_ReturnTrue)
{
  EXPECT_TRUE(CreateInProgressMeta().IsFailedRecord(kDummyEpoch));
}

TEST_F(MetadataFixture, GetPayloadLength_ActiveRecord_ReturnCorrectPayloadLength)
{
  EXPECT_EQ(kExpectedTotalLength - kExpectedKeyLength, CreateActiveMeta().GetPayloadLength());
}

/*--------------------------------------------------------------------------------------------------
 * Utility tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(MetadataFixture, UpdateOffset_ActiveRecord_GetUpdatedOffset)
{
  const size_t updated_offset = kExpectedOffset / 2;

  auto meta = CreateActiveMeta().UpdateOffset(updated_offset);

  EXPECT_EQ(updated_offset, meta.GetOffset());
}

TEST_F(MetadataFixture, MakeVisible_InProgressRecord_RecordBecomeActive)
{
  auto meta = CreateInProgressMeta().MakeVisible(kExpectedOffset);

  EXPECT_TRUE(meta.IsVisible());
  EXPECT_FALSE(meta.IsDeleted());
  EXPECT_FALSE(meta.IsInProgress());
  EXPECT_FALSE(meta.IsFailedRecord(kCurrentEpoch));
  EXPECT_EQ(kExpectedOffset, meta.GetOffset());
}

TEST_F(MetadataFixture, MakeInvisible_InProgressRecord_RecordBecomeDeleted)
{
  auto meta = CreateInProgressMeta().MakeInvisible(kExpectedOffset);

  EXPECT_FALSE(meta.IsVisible());
  EXPECT_TRUE(meta.IsDeleted());
  EXPECT_FALSE(meta.IsInProgress());
  EXPECT_FALSE(meta.IsFailedRecord(kCurrentEpoch));
  EXPECT_EQ(kExpectedOffset, meta.GetOffset());
}

TEST_F(MetadataFixture, Delete_ActiveRecord_RecordBecomeDeleted)
{
  auto meta = CreateActiveMeta().Delete();

  EXPECT_FALSE(meta.IsVisible());
  EXPECT_TRUE(meta.IsDeleted());
  EXPECT_FALSE(meta.IsInProgress());
  EXPECT_FALSE(meta.IsFailedRecord(kCurrentEpoch));
  EXPECT_EQ(kExpectedOffset, meta.GetOffset());
}

}  // namespace dbgroup::index::bztree::component::test
