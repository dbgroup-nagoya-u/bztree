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

#include "bztree/component/status_word.hpp"

#include "gtest/gtest.h"

namespace dbgroup::index::bztree::component::test
{
/*######################################################################################
 * Global constants
 *####################################################################################*/

constexpr uint64_t kControlBitsMask = 7UL << 61UL;
constexpr size_t kExpectedRecordCount = 1;
constexpr size_t kExpectedBlockSize = 16;
constexpr size_t kMaxBlockSize = kPageSize - kHeaderLen;
constexpr size_t kMaxUsedSize = kMaxBlockSize - kMinFreeSpaceSize;
constexpr size_t kMinUsedSize = kMinNodeSize - kHeaderLen;
constexpr size_t kMaxMergedBlockSize = kMaxMergedSize - kHeaderLen;

class StatusWordFixture : public testing::Test
{
 protected:
  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }
};

/*######################################################################################
 * Constructor tests
 *####################################################################################*/

TEST_F(StatusWordFixture, ConstructWithoutArgumentsCreateZeroStatusWord)
{
  const StatusWord status{};

  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(0, status.GetRecordCount());
  EXPECT_EQ(0, status.GetBlockSize());

  // check control bits
  uint64_t status_uint{};
  memcpy(&status_uint, &status, sizeof(StatusWord));
  EXPECT_EQ(0, status_uint & kControlBitsMask);
}

TEST_F(StatusWordFixture, ConstructWithArgumentsCreateInitializedStatusWord)
{
  const StatusWord status{kExpectedRecordCount, kExpectedBlockSize};

  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(kExpectedRecordCount, status.GetRecordCount());
  EXPECT_EQ(kExpectedBlockSize, status.GetBlockSize());

  // check control bits
  uint64_t status_uint{};
  memcpy(&status_uint, &status, sizeof(StatusWord));
  EXPECT_EQ(0, status_uint & kControlBitsMask);
}

/*######################################################################################
 * Operator tests
 *####################################################################################*/

TEST_F(StatusWordFixture, EQWithSameStatusWordsReturnTrue)
{
  const StatusWord status_a{kExpectedRecordCount, kExpectedBlockSize};
  const StatusWord status_b{kExpectedRecordCount, kExpectedBlockSize};

  EXPECT_TRUE(status_a == status_b);
}

TEST_F(StatusWordFixture, EQWithDifferentStatusWordsReturnFalse)
{
  const StatusWord status_a{kExpectedRecordCount, kExpectedBlockSize};
  const StatusWord status_b{};

  EXPECT_FALSE(status_a == status_b);
}

TEST_F(StatusWordFixture, NEQWithSameStatusWordsReturnFalse)
{
  const StatusWord status_a{kExpectedRecordCount, kExpectedBlockSize};
  const StatusWord status_b{kExpectedRecordCount, kExpectedBlockSize};

  EXPECT_FALSE(status_a != status_b);
}

TEST_F(StatusWordFixture, NEQWithDifferentStatusWordsReturnTrue)
{
  const StatusWord status_a{kExpectedRecordCount, kExpectedBlockSize};
  const StatusWord status_b{};

  EXPECT_TRUE(status_a != status_b);
}

/*######################################################################################
 * Utility tests
 *####################################################################################*/

TEST_F(StatusWordFixture, NeedConsolidateionWithFewUnsoretedRecordsReturnFalse)
{
  const StatusWord stat{kMaxDeltaRecNum, 0};

  EXPECT_FALSE(stat.NeedConsolidation(0));
}

TEST_F(StatusWordFixture, NeedConsolidateionWithManyUnsoretedRecordsReturnTrue)
{
  const StatusWord stat{kMaxDeltaRecNum + 1, 0};

  EXPECT_TRUE(stat.NeedConsolidation(0));
}

TEST_F(StatusWordFixture, NeedConsolidateionWithSmallBlockSizeReturnFalse)
{
  const StatusWord stat{0, kMaxBlockSize};

  EXPECT_FALSE(stat.NeedConsolidation(0));
}

TEST_F(StatusWordFixture, NeedConsolidateionWithLargeBlockSizeReturnTrue)
{
  const StatusWord stat{0, kMaxBlockSize + 1};

  EXPECT_TRUE(stat.NeedConsolidation(0));
}

TEST_F(StatusWordFixture, NeedConsolidateionWithSmallDeletedSizeReturnFalse)
{
  const auto stat = StatusWord{0, 0}.Delete(kMaxDeletedSpaceSize);

  EXPECT_FALSE(stat.NeedConsolidation(0));
}

TEST_F(StatusWordFixture, NeedConsolidateionWithLargeDeletedSizeReturnTrue)
{
  const auto stat = StatusWord{0, 0}.Delete(kMaxDeletedSpaceSize + 1);

  EXPECT_TRUE(stat.NeedConsolidation(0));
}

TEST_F(StatusWordFixture, NeedSplitWithSmallBlockSizeReturnFalse)
{
  const StatusWord stat{0, kMaxUsedSize};
  const auto need_split = stat.template NeedSplit<size_t, size_t>();

  EXPECT_FALSE(need_split);
}

TEST_F(StatusWordFixture, NeedSplitWithLargeBlockSizeReturnTrue)
{
  const StatusWord stat{0, kMaxUsedSize + 1};
  const auto need_split = stat.template NeedSplit<size_t, size_t>();

  EXPECT_TRUE(need_split);
}

TEST_F(StatusWordFixture, NeedMergeWithSmallBlockSizeReturnTrue)
{
  const StatusWord stat{0, kMinUsedSize - 1};

  EXPECT_TRUE(stat.NeedMerge());
}

TEST_F(StatusWordFixture, NeedMergeWithLargeBlockSizeReturnFalse)
{
  const StatusWord stat{0, kMinUsedSize};

  EXPECT_FALSE(stat.NeedMerge());
}

TEST_F(StatusWordFixture, CanMergeWithSmallSiblingNodeReturnTrue)
{
  const StatusWord stat{0, 0};
  const StatusWord sib_stat{0, kMaxMergedBlockSize - 1};

  EXPECT_TRUE(stat.CanMergeWith(sib_stat));
}

TEST_F(StatusWordFixture, CanMergeWithLargeSiblingNodeReturnFalse)
{
  const StatusWord stat{0, 0};
  const StatusWord sib_stat{0, kMaxMergedBlockSize};

  EXPECT_FALSE(stat.CanMergeWith(sib_stat));
}

TEST_F(StatusWordFixture, FreezeWithUnfrozenStatusMakeFrozenStatus)
{
  const StatusWord stat{kExpectedRecordCount, kExpectedBlockSize};
  const auto frozen_stat = stat.Freeze();

  EXPECT_TRUE(frozen_stat.IsFrozen());
  EXPECT_EQ(stat.GetRecordCount(), frozen_stat.GetRecordCount());
  EXPECT_EQ(stat.GetBlockSize(), frozen_stat.GetBlockSize());
}

TEST_F(StatusWordFixture, UnfreezeWithFrozenStatusMakeUnfrozenStatus)
{
  const auto frozen_stat = StatusWord{kExpectedRecordCount, kExpectedBlockSize}.Freeze();
  const auto stat = frozen_stat.Unfreeze();

  EXPECT_FALSE(stat.IsFrozen());
  EXPECT_EQ(stat.GetRecordCount(), frozen_stat.GetRecordCount());
  EXPECT_EQ(stat.GetBlockSize(), frozen_stat.GetBlockSize());
}

TEST_F(StatusWordFixture, AddWithEmptyStatusIncreaseRecordCountAndBlockSize)
{
  const StatusWord stat{};
  const auto added_stat = stat.Add(kExpectedBlockSize);

  EXPECT_EQ(1, added_stat.GetRecordCount());
  EXPECT_EQ(kExpectedBlockSize, added_stat.GetBlockSize());
}

}  // namespace dbgroup::index::bztree::component::test
