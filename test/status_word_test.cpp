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
  EXPECT_TRUE(frozen_status.IsFrozen());
  EXPECT_EQ(0, frozen_status.GetRecordCount());
  EXPECT_EQ(0, frozen_status.GetBlockSize());
  EXPECT_EQ(0, frozen_status.GetDeletedSize());
}

TEST_F(StatusWordFixture, AddDelete_InitialStatus_AddInfoWithoutSideEffect)
{
  const StatusWord status;
  const size_t record_count = 16, block_size = 8, deleted_size = 4;
  const auto updated_status = status.Add(record_count, block_size).Delete(deleted_size);

  EXPECT_EQ(kWordLength, sizeof(updated_status));
  EXPECT_FALSE(updated_status.IsFrozen());
  EXPECT_EQ(record_count, updated_status.GetRecordCount());
  EXPECT_EQ(block_size, updated_status.GetBlockSize());
  EXPECT_EQ(deleted_size, updated_status.GetDeletedSize());
  EXPECT_EQ(kHeaderLength + (kWordLength * 16) + block_size, updated_status.GetOccupiedSize());
  EXPECT_EQ((kWordLength * 16) + block_size - deleted_size, updated_status.GetLiveDataSize());
}

}  // namespace dbgroup::index::bztree::component::test
