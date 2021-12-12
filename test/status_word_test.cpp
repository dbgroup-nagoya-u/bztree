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
class StatusWordFixture : public testing::Test
{
 protected:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr uint64_t kControlBitsMask = 7UL << 61;
  static constexpr size_t kExpectedRecordCount = 1;
  static constexpr size_t kExpectedBlockSize = 16;
  static constexpr size_t kExpectedDeletedSize = 8;

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

  StatusWord
  CreateStatusWord()
  {
    return StatusWord{kExpectedRecordCount, kExpectedBlockSize};
  }
};

/*--------------------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(StatusWordFixture, Construct_NoArguments_CorrectlyInitialized)
{
  StatusWord status{};

  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(0, status.GetRecordCount());
  EXPECT_EQ(0, status.GetBlockSize());
  EXPECT_EQ(0, status.GetDeletedSize());

  // check control bits
  uint64_t status_uint;
  memcpy(&status_uint, &status, sizeof(StatusWord));
  EXPECT_EQ(0, status_uint & kControlBitsMask);
}

TEST_F(StatusWordFixture, Construct_WithArguments_CorrectlyInitialized)
{
  auto status = CreateStatusWord();

  EXPECT_FALSE(status.IsFrozen());
  EXPECT_EQ(kExpectedRecordCount, status.GetRecordCount());
  EXPECT_EQ(kExpectedBlockSize, status.GetBlockSize());
  EXPECT_EQ(0, status.GetDeletedSize());

  // check control bits
  uint64_t status_uint;
  memcpy(&status_uint, &status, sizeof(StatusWord));
  EXPECT_EQ(0, status_uint & kControlBitsMask);
}

/*--------------------------------------------------------------------------------------------------
 * Operator tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(StatusWordFixture, EQ_SameStatusWords_ReturnTrue)
{
  auto status_a = CreateStatusWord(), status_b = CreateStatusWord();

  EXPECT_TRUE(status_a == status_b);
}

TEST_F(StatusWordFixture, EQ_DifferentStatusWords_ReturnFalse)
{
  auto status_a = CreateStatusWord(), status_b = StatusWord{};

  EXPECT_FALSE(status_a == status_b);
}

TEST_F(StatusWordFixture, NEQ_SameStatusWords_ReturnFalse)
{
  auto status_a = CreateStatusWord(), status_b = CreateStatusWord();

  EXPECT_FALSE(status_a != status_b);
}

TEST_F(StatusWordFixture, NEQ_DifferentStatusWords_ReturnTrue)
{
  auto status_a = CreateStatusWord(), status_b = StatusWord{};

  EXPECT_TRUE(status_a != status_b);
}

/*--------------------------------------------------------------------------------------------------
 * Getter tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(StatusWordFixture, GetOccupiedSize_EmptyStatusWord_GetCorrectSize)
{
  StatusWord status{};

  EXPECT_EQ(kHeaderLength, status.GetUsedSize());
}

TEST_F(StatusWordFixture, GetOccupiedSize_ActiveStatusWord_GetCorrectSize)
{
  auto status = CreateStatusWord();

  auto expected_occupied_size =
      kHeaderLength + (kWordLength * kExpectedRecordCount) + kExpectedBlockSize;

  EXPECT_EQ(expected_occupied_size, status.GetUsedSize());
}

TEST_F(StatusWordFixture, GetLiveDataSize_EmptyStatusWord_GetCorrectSize)
{
  StatusWord status{};

  EXPECT_EQ(0, status.GetLiveDataSize());
}

TEST_F(StatusWordFixture, GetLiveDataSize_ActiveStatusWord_GetCorrectSize)
{
  auto status = CreateStatusWord().Delete(kExpectedDeletedSize);

  auto expected_live_data_size =
      (kWordLength * kExpectedRecordCount) + kExpectedBlockSize - kExpectedDeletedSize;

  EXPECT_EQ(expected_live_data_size, status.GetLiveDataSize());
}

/*--------------------------------------------------------------------------------------------------
 * Utility tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(StatusWordFixture, Freeze_EmptyStatus_StatusFrozen)
{
  auto status = StatusWord{}.Freeze();

  EXPECT_TRUE(status.IsFrozen());
}

TEST_F(StatusWordFixture, Add_EmptyStatus_StatusInfoCorrectlyUpdated)
{
  auto status = StatusWord{};

  for (size_t i = 1; i <= 10; ++i) {
    status = status.Add(kExpectedRecordCount, kExpectedBlockSize);

    EXPECT_EQ(i * kExpectedRecordCount, status.GetRecordCount());
    EXPECT_EQ(i * kExpectedBlockSize, status.GetBlockSize());
  }
}

TEST_F(StatusWordFixture, Delete_EmptyStatus_StatusInfoCorrectlyUpdated)
{
  auto status = StatusWord{};

  for (size_t i = 1; i <= 10; ++i) {
    status = status.Delete(kExpectedDeletedSize);

    EXPECT_EQ(i * kExpectedDeletedSize, status.GetDeletedSize());
  }
}

}  // namespace dbgroup::index::bztree::component::test
