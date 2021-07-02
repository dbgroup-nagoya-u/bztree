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

#include "bztree/components/record.hpp"

#include <memory>
#include <utility>

#include "gtest/gtest.h"

namespace dbgroup::index::bztree
{
class RecordFixture : public ::testing::Test
{
 public:
  static constexpr size_t kIntLength = sizeof(uint64_t);
  static constexpr uint64_t kIntKey = 10;
  static constexpr uint64_t kIntPayload = 20;
  static constexpr size_t kCStrLength = 5;
  static constexpr const char* kCStrKey = "test";
  static constexpr const char* kCStrPayload = "hoge";

  struct IntKeyIntPayloadRecord {
    std::byte data[kIntLength + kIntLength];

    IntKeyIntPayloadRecord()
    {
      memcpy(data, &kIntKey, kIntLength);
      memcpy(data + kIntLength, &kIntPayload, kIntLength);
    }
  };

  struct IntKeyCStrPayloadRecord {
    std::byte data[kIntLength + kCStrLength];

    IntKeyCStrPayloadRecord()
    {
      memcpy(data, &kIntKey, kIntLength);
      memcpy(data + kIntLength, kCStrPayload, kCStrLength);
    }
  };

  struct CStrPayloadIntKeyRecord {
    std::byte data[kCStrLength + kIntLength];

    CStrPayloadIntKeyRecord()
    {
      memcpy(data, kCStrKey, kCStrLength);
      memcpy(data + kCStrLength, &kIntPayload, kIntLength);
    }
  };

  struct CStrKeyCStrPayloadRecord {
    std::byte data[kCStrLength + kCStrLength];

    CStrKeyCStrPayloadRecord()
    {
      memcpy(data, kCStrKey, kCStrLength);
      memcpy(data + kCStrLength, kCStrPayload, kCStrLength);
    }
  };

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

TEST_F(RecordFixture, Create_IntKeyIntPayload_RecordHasCorrectData)
{
  auto src = IntKeyIntPayloadRecord{};
  auto record = Record<uint64_t, uint64_t>::Create(&src, kIntLength, kIntLength);

  EXPECT_EQ(kIntKey, record->GetKey());
  EXPECT_EQ(kIntPayload, record->GetPayload());
}

TEST_F(RecordFixture, Create_IntKeyCStrPayload_RecordHasCorrectData)
{
  auto src = IntKeyCStrPayloadRecord{};
  auto record = Record<uint64_t, const char*>::Create(&src, kIntLength, kCStrLength);

  EXPECT_EQ(kIntKey, record->GetKey());
  EXPECT_STREQ(kCStrPayload, record->GetPayload());
}

TEST_F(RecordFixture, Create_CStrKeyIntPayload_RecordHasCorrectData)
{
  auto src = CStrPayloadIntKeyRecord{};
  auto record = Record<const char*, uint64_t>::Create(&src, kCStrLength, kIntLength);

  EXPECT_STREQ(kCStrKey, record->GetKey());
  EXPECT_EQ(kIntPayload, record->GetPayload());
}

TEST_F(RecordFixture, Create_CStrKeyCStrPayload_RecordHasCorrectData)
{
  auto src = CStrKeyCStrPayloadRecord{};
  auto record = Record<const char*, const char*>::Create(&src, kCStrLength, kCStrLength);

  EXPECT_STREQ(kCStrKey, record->GetKey());
  EXPECT_STREQ(kCStrPayload, record->GetPayload());
}

TEST_F(RecordFixture, Copy_IntKeyIntPayload_RecordCorrectlyCopied)
{
  auto src = IntKeyIntPayloadRecord{};
  auto record_ptr = Record<uint64_t, uint64_t>::Create(&src, kIntLength, kIntLength);
  auto orig_record = *record_ptr;

  auto copy_by_costructor = Record{orig_record};

  EXPECT_EQ(record_ptr->GetKey(), copy_by_costructor.GetKey());
  EXPECT_EQ(record_ptr->GetPayload(), copy_by_costructor.GetPayload());

  auto copy_by_operator = orig_record;

  EXPECT_EQ(record_ptr->GetKey(), copy_by_operator.GetKey());
  EXPECT_EQ(record_ptr->GetPayload(), copy_by_operator.GetPayload());
}

TEST_F(RecordFixture, Move_IntKeyIntPayload_RecordCorrectlyMoved)
{
  auto src = IntKeyIntPayloadRecord{};
  auto record_ptr = Record<uint64_t, uint64_t>::Create(&src, kIntLength, kIntLength);

  auto orig_record = *record_ptr;
  auto move_by_costructor = Record{std::move(orig_record)};

  EXPECT_EQ(record_ptr->GetKey(), move_by_costructor.GetKey());
  EXPECT_EQ(record_ptr->GetPayload(), move_by_costructor.GetPayload());

  orig_record = *record_ptr;
  auto move_by_operator = std::move(orig_record);

  EXPECT_EQ(record_ptr->GetKey(), move_by_operator.GetKey());
  EXPECT_EQ(record_ptr->GetPayload(), move_by_operator.GetPayload());
}

}  // namespace dbgroup::index::bztree
