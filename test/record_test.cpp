// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "bztree/components/record.hpp"

#include <gtest/gtest.h>

#include <memory>

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

  /**
   * @brief A wrapper API to create \c std::unique_ptr<Record> .
   *
   * @tparam Key
   * @tparam Payload
   * @param src_addr
   * @param key_length
   * @param payload_length
   * @return \c std::unique_ptr<Record>
   */
  template <class Key, class Payload>
  std::unique_ptr<Record<Key, Payload>>
  CreateRecord(  //
      const void* src_addr,
      const size_t key_length,
      const size_t payload_length)
  {
    return std::unique_ptr<Record<Key, Payload>>(
        Record<Key, Payload>::Create(src_addr, key_length, payload_length));
  }

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
  auto record = CreateRecord<uint64_t, uint64_t>(&src, kIntLength, kIntLength);

  EXPECT_EQ(kIntKey, record->GetKey());
  EXPECT_EQ(kIntPayload, record->GetPayload());
}

TEST_F(RecordFixture, Create_IntKeyCStrPayload_RecordHasCorrectData)
{
  auto src = IntKeyCStrPayloadRecord{};
  auto record = CreateRecord<uint64_t, const char*>(&src, kIntLength, kCStrLength);

  EXPECT_EQ(kIntKey, record->GetKey());
  EXPECT_STREQ(kCStrPayload, record->GetPayload());
}

TEST_F(RecordFixture, Create_CStrKeyIntPayload_RecordHasCorrectData)
{
  auto src = CStrPayloadIntKeyRecord{};
  auto record = CreateRecord<const char*, uint64_t>(&src, kCStrLength, kIntLength);

  EXPECT_STREQ(kCStrKey, record->GetKey());
  EXPECT_EQ(kIntPayload, record->GetPayload());
}

TEST_F(RecordFixture, Create_CStrKeyCStrPayload_RecordHasCorrectData)
{
  auto src = CStrKeyCStrPayloadRecord{};
  auto record = CreateRecord<const char*, const char*>(&src, kCStrLength, kCStrLength);

  EXPECT_STREQ(kCStrKey, record->GetKey());
  EXPECT_STREQ(kCStrPayload, record->GetPayload());
}

}  // namespace dbgroup::index::bztree
