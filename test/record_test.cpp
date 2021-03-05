// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "bztree/components/record.hpp"

#include <gtest/gtest.h>

namespace dbgroup::index::bztree
{
class RecordFixture : public testing::Test
{
 public:
  static constexpr size_t kIntLength = sizeof(uint64_t);
  static constexpr uint64_t kIntKey = 10;
  static constexpr uint64_t kIntPayload = 20;
  static constexpr size_t kCStrLength = 5;
  static constexpr const char* kCStrKey = "test";
  static constexpr const char* kCStrPayload = "hoge";

  struct UInt64UInt64Record {
    std::byte data[kIntLength + kIntLength];

    UInt64UInt64Record()
    {
      memcpy(data, &kIntKey, kIntLength);
      memcpy(data + kIntLength, &kIntPayload, kIntLength);
    }
  };

  struct UInt64CStringRecord {
    std::byte data[kIntLength + kCStrLength];

    UInt64CStringRecord()
    {
      memcpy(data, &kIntKey, kIntLength);
      memcpy(data + kIntLength, kCStrPayload, kCStrLength);
    }
  };

  struct CStringUInt64Record {
    std::byte data[kCStrLength + kIntLength];

    CStringUInt64Record()
    {
      memcpy(data, kCStrKey, kCStrLength);
      memcpy(data + kCStrLength, &kIntPayload, kIntLength);
    }
  };

  struct CStringCStringRecord {
    std::byte data[kCStrLength + kCStrLength];

    CStringCStringRecord()
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

TEST_F(RecordFixture, Create_DefaultConstructor_CorrectlyInitialized)
{
  auto src_rec = UInt64UInt64Record{};
  auto record = Record<uint64_t, uint64_t>::Create(&src_rec, kIntLength, kIntLength);

  EXPECT_EQ(kIntKey, record->GetKey());
  EXPECT_EQ(kIntPayload, record->GetPayload());
}

TEST_F(RecordFixture, Create_IntKeyCStrPayload_CorrectlyInitialized)
{
  auto src_rec = UInt64CStringRecord{};
  auto record = Record<uint64_t, const char*>::Create(&src_rec, kIntLength, kCStrLength);

  EXPECT_EQ(kIntKey, record->GetKey());
  EXPECT_STREQ(kCStrPayload, record->GetPayload());
}

TEST_F(RecordFixture, Create_CStrKeyIntPayload_CorrectlyInitialized)
{
  auto src_rec = CStringUInt64Record{};
  auto record = Record<const char*, uint64_t>::Create(&src_rec, kCStrLength, kIntLength);

  EXPECT_STREQ(kCStrKey, record->GetKey());
  EXPECT_EQ(kIntPayload, record->GetPayload());
}

TEST_F(RecordFixture, Create_CStrKeyCStrPayload_CorrectlyInitialized)
{
  auto src_rec = CStringCStringRecord{};
  auto record = Record<const char*, const char*>::Create(&src_rec, kCStrLength, kCStrLength);

  EXPECT_STREQ(kCStrKey, record->GetKey());
  EXPECT_STREQ(kCStrPayload, record->GetPayload());
}

}  // namespace dbgroup::index::bztree
