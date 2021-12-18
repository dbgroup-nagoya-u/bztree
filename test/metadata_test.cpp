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
/*################################################################################################
 * Internal constants
 *##############################################################################################*/

constexpr uint64_t kControlBitsMask = 7UL << 61;
constexpr size_t kExpectedOffset = 256;
constexpr size_t kExpectedKeyLength = 8;
constexpr size_t kExpectedTotalLength = 16;
constexpr size_t kCurrentEpoch = 1;
constexpr size_t kDummyEpoch = 2;
constexpr bool kInProgressFlag = true;

class MetadataFixture : public testing::Test
{
 protected:
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

  auto
  CreateActiveMeta()  //
      -> Metadata
  {
    return Metadata{kExpectedOffset, kExpectedKeyLength, kExpectedTotalLength};
  }

  auto
  CreateInProgressMeta()  //
      -> Metadata
  {
    return Metadata{kExpectedKeyLength, kExpectedTotalLength};
  }
};

/*--------------------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(MetadataFixture, ConstructWithNoArgumentsCreateZeroMetadata)
{
  const Metadata meta{};

  EXPECT_FALSE(meta.IsVisible());
  EXPECT_FALSE(meta.IsInProgress());
  EXPECT_EQ(0, meta.GetOffset());
  EXPECT_EQ(0, meta.GetKeyLength());
  EXPECT_EQ(0, meta.GetPayloadLength());
  EXPECT_EQ(0, meta.GetTotalLength());

  // check control bits
  uint64_t meta_uint;
  memcpy(&meta_uint, &meta, sizeof(Metadata));
  EXPECT_EQ(0, meta_uint & kControlBitsMask);
}

TEST_F(MetadataFixture, ConstructWithOffsetCreateActiveMetadata)
{
  auto meta = CreateActiveMeta();

  EXPECT_TRUE(meta.IsVisible());
  EXPECT_FALSE(meta.IsInProgress());
  EXPECT_EQ(kExpectedOffset, meta.GetOffset());
  EXPECT_EQ(kExpectedKeyLength, meta.GetKeyLength());
  EXPECT_EQ(kExpectedTotalLength - kExpectedKeyLength, meta.GetPayloadLength());
  EXPECT_EQ(kExpectedTotalLength, meta.GetTotalLength());

  // check control bits
  uint64_t meta_uint;
  memcpy(&meta_uint, &meta, sizeof(Metadata));
  EXPECT_EQ(0, meta_uint & kControlBitsMask);
}

TEST_F(MetadataFixture, ConstructWithoutOffsetCreateInProgressMetadata)
{
  auto meta = CreateInProgressMeta();

  EXPECT_TRUE(meta.IsVisible());
  EXPECT_TRUE(meta.IsInProgress());
  EXPECT_EQ(0, meta.GetOffset());
  EXPECT_EQ(kExpectedKeyLength, meta.GetKeyLength());
  EXPECT_EQ(kExpectedTotalLength - kExpectedKeyLength, meta.GetPayloadLength());
  EXPECT_EQ(kExpectedTotalLength, meta.GetTotalLength());

  // check control bits
  uint64_t meta_uint;
  memcpy(&meta_uint, &meta, sizeof(Metadata));
  EXPECT_EQ(0, meta_uint & kControlBitsMask);
}

/*--------------------------------------------------------------------------------------------------
 * Utility tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(MetadataFixture, UpdateOffsetWithActiveMetadataGetUpdatedOffset)
{
  constexpr size_t updated_offset = kExpectedOffset / 2;

  auto meta = CreateActiveMeta().UpdateOffset(updated_offset);

  EXPECT_EQ(updated_offset, meta.GetOffset());
}

TEST_F(MetadataFixture, CommitWithInProgressMetadataMakeVisibleRecord)
{
  auto meta = CreateInProgressMeta().Commit(kExpectedOffset);

  EXPECT_TRUE(meta.IsVisible());
  EXPECT_FALSE(meta.IsInProgress());
  EXPECT_EQ(kExpectedOffset, meta.GetOffset());
}

TEST_F(MetadataFixture, DeleteWithInProgressMetadataMakeInvisibleRecord)
{
  auto meta = CreateInProgressMeta().Delete(kExpectedOffset);

  EXPECT_FALSE(meta.IsVisible());
  EXPECT_FALSE(meta.IsInProgress());
  EXPECT_EQ(kExpectedOffset, meta.GetOffset());
}

TEST_F(MetadataFixture, DeleteWithActiveMetadataMakeInvisibleRecord)
{
  auto meta = CreateActiveMeta().Delete();

  EXPECT_FALSE(meta.IsVisible());
  EXPECT_FALSE(meta.IsInProgress());
  EXPECT_EQ(kExpectedOffset, meta.GetOffset());
}

}  // namespace dbgroup::index::bztree::component::test
