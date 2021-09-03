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
#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bztree::component::test
{
using ::dbgroup::memory::MallocNew;

// use a supper template to define key-payload pair templates
template <class KeyType, class PayloadType, class KeyComparator, class PayloadComparator>
struct KeyPayload {
  using Key = KeyType;
  using Payload = PayloadType;
  using KeyComp = KeyComparator;
  using PayloadComp = PayloadComparator;
};

template <class KeyPayload>
class RecordPageFixture : public ::testing::Test
{
  // extract key-payload types
  using Key = typename KeyPayload::Key;
  using Payload = typename KeyPayload::Payload;
  using KeyComp = typename KeyPayload::KeyComp;
  using PayloadComp = typename KeyPayload::PayloadComp;

  // define type aliases for simplicity
  using RecordPage_t = RecordPage<Key, Payload>;
  using RecordIterator_t = RecordIterator<Key, Payload, KeyComp>;

 protected:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr size_t kKeyNumForTest = 128;
  static constexpr size_t kKeyLength = kWordLength;
  static constexpr size_t kPayloadLength = kWordLength;

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
    key_length = (IsVariableLengthData<Key>()) ? 7 : sizeof(Key);
    PrepareTestData(keys, kKeyNumForTest, key_length);

    // prepare payloads
    payload_length = (IsVariableLengthData<Payload>()) ? 7 : sizeof(Payload);
    PrepareTestData(payloads, kKeyNumForTest, payload_length);

    // initialize an empty page
    page = nullptr;
    PreparePage(0);
  }

  void
  TearDown() override
  {
    ReleaseTestData(keys, kKeyNumForTest);
    ReleaseTestData(payloads, kKeyNumForTest);

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
      if constexpr (IsVariableLengthData<Key>()) {
        *(reinterpret_cast<uint32_t *>(cur_addr)) = key_length;
        cur_addr += sizeof(uint32_t);
      }
      if constexpr (IsVariableLengthData<Payload>()) {
        *(reinterpret_cast<uint32_t *>(cur_addr)) = payload_length;
        cur_addr += sizeof(uint32_t);
      }

      if constexpr (IsVariableLengthData<Key>()) {
        memcpy(cur_addr, keys[i], key_length);
      } else {
        memcpy(cur_addr, &keys[i], key_length);
      }
      cur_addr += key_length;
      if constexpr (IsVariableLengthData<Payload>()) {
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
    EXPECT_TRUE(IsEqual<KeyComp>(keys[expected_id], key));
  }

  void
  VerifyPayload(  //
      const Payload payload,
      const size_t expected_id)
  {
    EXPECT_TRUE(IsEqual<PayloadComp>(payloads[expected_id], payload));
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

using KeyPayloadPairs = ::testing::Types<KeyPayload<uint64_t, uint64_t, UInt64Comp, UInt64Comp>,
                                         KeyPayload<char *, uint64_t, CStrComp, UInt64Comp>,
                                         KeyPayload<uint64_t, char *, UInt64Comp, CStrComp>,
                                         KeyPayload<char *, char *, CStrComp, CStrComp>>;
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
