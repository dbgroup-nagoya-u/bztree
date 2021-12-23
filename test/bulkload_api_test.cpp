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

#include <functional>
#include <memory>
#include <thread>

#include "bztree/bztree.hpp"
#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bztree::test
{
// use a supper template to define key-payload pair templates
template <class KeyType, class PayloadType, class KeyComparator, class PayloadComparator>
struct KeyPayload {
  using Key = KeyType;
  using Payload = PayloadType;
  using KeyComp = KeyComparator;
  using PayloadComp = PayloadComparator;
};

template <class KeyPayload>
class BzTreeFixture : public testing::Test
{
  // extract key-payload types
  using Key = typename KeyPayload::Key;
  using Payload = typename KeyPayload::Payload;
  using KeyComp = typename KeyPayload::KeyComp;
  using PayloadComp = typename KeyPayload::PayloadComp;

  // define type aliases for simplicity
  using Node_t = component::Node<Key, Payload, KeyComp>;
  using BzTree_t = BzTree<Key, Payload, KeyComp>;
  using RecordPage_t = component::RecordPage<Key, Payload>;
  using BulkloadEntry_t = BulkloadEntry<Key, Payload>;
  using EntryArray = std::vector<BulkloadEntry_t>;

 protected:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  // constant values for testing
  static constexpr size_t kIndexEpoch = 1;
  static constexpr size_t kSmallKeyNum = 16;
  static constexpr size_t kMediumKeyNum = kSmallKeyNum + 8388608;
  static constexpr size_t kLargeKeyNum = kMediumKeyNum + 16;
  static constexpr size_t kKeyLength = kWordLength;
  static constexpr size_t kPayloadLength = kWordLength;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // actual keys and payloads
  size_t key_length;
  size_t payload_length;
  Key keys[kLargeKeyNum];
  Payload payloads[kLargeKeyNum];

  // a test target BzTree
  BzTree_t bztree = BzTree_t{1000};

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp()
  {
    // prepare keys
    key_length = (IsVariableLengthData<Key>()) ? 7 : sizeof(Key);
    PrepareTestData(keys, kLargeKeyNum, key_length);

    // prepare payloads
    payload_length = (IsVariableLengthData<Payload>()) ? 7 : sizeof(Payload);
    PrepareTestData(payloads, kLargeKeyNum, payload_length);
  }

  void
  TearDown()
  {
    ReleaseTestData(keys, kLargeKeyNum);
    ReleaseTestData(payloads, kLargeKeyNum);
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyRead(  //
      const size_t key_id,
      const size_t expected_id,
      const bool expect_fail = false)
  {
    const auto [rc, actual] = bztree.Read(keys[key_id]);

    if (expect_fail) {
      EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(ReturnCode::kSuccess, rc);
      if constexpr (IsVariableLengthData<Payload>()) {
        EXPECT_TRUE(component::IsEqual<PayloadComp>(payloads[expected_id], actual.get()));
      } else {
        EXPECT_TRUE(component::IsEqual<PayloadComp>(payloads[expected_id], actual));
      }
    }
  }

  void
  VerifyWrite(  //
      const size_t key_id,
      const size_t payload_id)
  {
    auto rc = bztree.Write(keys[key_id], payloads[payload_id], key_length, payload_length);

    EXPECT_EQ(ReturnCode::kSuccess, rc);
  }

  void
  VerifyBulkLoadForSingleThread(  //
      const size_t begin_key_id,
      const size_t end_key_id)
  {
    EntryArray entries;
    for (size_t i = begin_key_id; i < end_key_id; ++i) {
      entries.emplace_back(BulkloadEntry_t{keys[i], payloads[i]});
    }

    auto rc = bztree.BulkLoadForSingleThread(entries);

    EXPECT_EQ(ReturnCode::kSuccess, rc);
  }
};

/*##################################################################################################
 * Preparation for typed testing
 *################################################################################################*/

using KeyPayloadPairs = ::testing::Types<KeyPayload<uint64_t, uint64_t, UInt64Comp, UInt64Comp>,
                                         KeyPayload<char *, uint64_t, CStrComp, UInt64Comp>,
                                         KeyPayload<uint64_t, char *, UInt64Comp, CStrComp>,
                                         KeyPayload<char *, char *, CStrComp, CStrComp>,
                                         KeyPayload<uint32_t, uint64_t, UInt32Comp, UInt64Comp>,
                                         KeyPayload<uint64_t, uint64_t *, UInt64Comp, PtrComp>,
                                         KeyPayload<uint64_t, MyClass, UInt64Comp, MyClassComp>,
                                         KeyPayload<uint64_t, int64_t, UInt64Comp, Int64Comp>>;
TYPED_TEST_CASE(BzTreeFixture, KeyPayloadPairs);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

/*--------------------------------------------------------------------------------------------------
 * BulkLoadForSingleThread operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, BulkloadForST_EmptyEntries)
{
  TestFixture::VerifyBulkLoadForSingleThread(0, 0);
}

TYPED_TEST(BzTreeFixture, BulkloadForST_WithoutSplit)
{
  TestFixture::VerifyBulkLoadForSingleThread(0, TestFixture::kSmallKeyNum);

  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

// The following tests are all time-consuming and are not recommended to be run simultaneously.

/*TYPED_TEST(BzTreeFixture, BulkloadForST_WithSplit)
{
  TestFixture::VerifyBulkLoadForSingleThread(0, TestFixture::kMediumKeyNum);

  for (size_t i = 0; i < TestFixture::kMediumKeyNum; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}*/

/*TYPED_TEST(BzTreeFixture, BulkloadForST_AdditionalRWrite)
{
  TestFixture::VerifyBulkLoadForSingleThread(0, TestFixture::kMediumKeyNum);

  for (size_t i = TestFixture::kMediumKeyNum; i < TestFixture::kLargeKeyNum; ++i) {
    TestFixture::VerifyWrite(i, i);
  }

  for (size_t i = 0; i < TestFixture::kLargeKeyNum; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}*/

/*TYPED_TEST(BzTreeFixture, BulkloadForST_AdditionalLWrite)
{
  TestFixture::VerifyBulkLoadForSingleThread(TestFixture::kSmallKeyNum, TestFixture::kMediumKeyNum);

  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyWrite(i, i);
  }

  for (size_t i = 0; i < TestFixture::kMediumKeyNum; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}*/

}  // namespace dbgroup::index::bztree::test