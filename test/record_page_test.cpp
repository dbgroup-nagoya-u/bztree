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

#include "bztree/component/record_page.hpp"

#include <memory>
#include <utility>

#include "bztree/bztree.hpp"
#include "bztree/component/record_iterator.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bztree::component::test
{
// use a supper template to define key-payload pair templates
template <class KeyType, class PayloadType, class CompareType>
struct KeyPayloadPair {
  using Key = KeyType;
  using Payload = PayloadType;
  using Compare = CompareType;
};

template <class KeyPayloadPair>
class RecordPageFixture : public ::testing::Test
{
  // extract key-payload types
  using Key = typename KeyPayloadPair::Key;
  using Payload = typename KeyPayloadPair::Payload;
  using Compare = typename KeyPayloadPair::Compare;

  // define type aliases for simplicity
  using RecordPage_t = RecordPage<Key, Payload>;
  using RecordIterator_t = RecordIterator<Key, Payload, Compare>;

 protected:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr size_t kKeyNumForTest = 128;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // actual keys and payloads
  size_t key_length;
  size_t payload_length;
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];

  /// a test target
  RecordPage_t *page;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    // prepare keys
    if constexpr (std::is_same_v<Key, char *>) {
      // variable-length keys
      key_length = 7;
      for (size_t i = 0; i < kKeyNumForTest; ++i) {
        auto key = new char[kWordLength];
        snprintf(key, kWordLength, "%06lu", i);
        keys[i] = key;
      }
    } else {
      // static-length keys
      key_length = sizeof(Key);
      for (size_t i = 0; i < kKeyNumForTest; ++i) {
        keys[i] = i;
      }
    }

    // prepare payloads
    if constexpr (std::is_same_v<Payload, char *>) {
      // variable-length payloads
      payload_length = 7;
      for (size_t i = 0; i < kKeyNumForTest; ++i) {
        auto payload = new char[kWordLength];
        snprintf(payload, kWordLength, "%06lu", i);
        payloads[i] = payload;
      }
    } else {
      // static-length payloads
      payload_length = sizeof(Payload);
      for (size_t i = 0; i < kKeyNumForTest; ++i) {
        payloads[i] = i;
      }
    }

    // initialize an empty page
    page = nullptr;
    PreparePage(0);
  }

  void
  TearDown() override
  {
    if constexpr (std::is_same_v<Key, char *>) {
      for (size_t i = 0; i < kKeyNumForTest; ++i) {
        delete[] keys[i];
      }
    }
    if constexpr (std::is_same_v<Payload, char *>) {
      for (size_t i = 0; i < kKeyNumForTest; ++i) {
        delete[] payloads[i];
      }
    }

    ::dbgroup::memory::Delete(page);
  }

  /*################################################################################################
   * Utility functions
   *##############################################################################################*/

  void
  PreparePage(const size_t record_num)
  {
    if (page != nullptr) {
      ::dbgroup::memory::Delete(page);
    }

    page = ::dbgroup::memory::New<RecordPage_t>();

    auto cur_addr = reinterpret_cast<std::byte *>(page) + kHeaderLength;

    for (size_t i = 0; i < record_num; ++i) {
      if constexpr (std::is_same_v<Key, char *>) {
        *(reinterpret_cast<uint32_t *>(cur_addr)) = key_length;
        cur_addr += sizeof(uint32_t);
      }
      if constexpr (std::is_same_v<Payload, char *>) {
        *(reinterpret_cast<uint32_t *>(cur_addr)) = payload_length;
        cur_addr += sizeof(uint32_t);
      }

      if constexpr (std::is_same_v<Key, char *>) {
        memcpy(cur_addr, keys[i], key_length);
      } else {
        memcpy(cur_addr, &keys[i], key_length);
      }
      cur_addr += key_length;
      if constexpr (std::is_same_v<Payload, char *>) {
        memcpy(cur_addr, payloads[i], payload_length);
      } else {
        memcpy(cur_addr, &payloads[i], payload_length);
      }
      cur_addr += payload_length;
    }

    page->SetEndAddress(cur_addr);
    page->SetLastKeyAddress(cur_addr - (key_length + payload_length));
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyKey(  //
      const Key key,
      const size_t expected_id)
  {
    if constexpr (std::is_same_v<Key, char *>) {
      EXPECT_STREQ(keys[expected_id], key);
    } else {
      EXPECT_EQ(keys[expected_id], key);
    }
  }

  void
  VerifyPayload(  //
      const Payload payload,
      const size_t expected_id)
  {
    if constexpr (std::is_same_v<Payload, char *>) {
      EXPECT_STREQ(payloads[expected_id], payload);
    } else {
      EXPECT_EQ(payloads[expected_id], payload);
    }
  }

  void
  VerifyGetLastKey(const size_t rec_num)
  {
    VerifyKey(page->GetLastKey(), rec_num - 1);
  }

  void
  VerifyStarOperator(  //
      const RecordIterator_t &iter,
      const size_t expected_id)
  {
    const auto [key, payload] = *iter;
    VerifyKey(key, expected_id);
    VerifyPayload(payload, expected_id);
  }

  void
  VerifyPlusPlusOperator(const size_t rec_num)
  {
    auto iter = RecordIterator_t{nullptr, nullptr, false, page, true};
    page = nullptr;

    size_t count = 0;
    for (; iter.HasNext(); ++iter, ++count) {
      ASSERT_LT(count, rec_num);

      const auto [key, payload] = *iter;
      VerifyKey(key, count);
      VerifyPayload(payload, count);
    }

    EXPECT_EQ(count, rec_num);
  }
};

/*##################################################################################################
 * Preparation for typed testing
 *################################################################################################*/

using UInt64Comp = std::less<uint64_t>;
using CStrComp = dbgroup::index::bztree::CompareAsCString;

using KeyPayloadPairs = ::testing::Types<KeyPayloadPair<uint64_t, uint64_t, UInt64Comp>,
                                         KeyPayloadPair<char *, uint64_t, CStrComp>,
                                         KeyPayloadPair<uint64_t, char *, UInt64Comp>,
                                         KeyPayloadPair<char *, char *, CStrComp>>;
TYPED_TEST_CASE(RecordPageFixture, KeyPayloadPairs);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TYPED_TEST(RecordPageFixture, GetLastKey_NotEmptyPage_ReturnExpectedKey)
{  //
  TestFixture::PreparePage(TestFixture::kKeyNumForTest);
  TestFixture::VerifyGetLastKey(TestFixture::kKeyNumForTest);
}

TYPED_TEST(RecordPageFixture, PlusPlusOperator_NotEmptyPage_ReadEveryRecord)
{  //
  TestFixture::PreparePage(TestFixture::kKeyNumForTest);
  TestFixture::VerifyPlusPlusOperator(TestFixture::kKeyNumForTest);
}

}  // namespace dbgroup::index::bztree::component::test
