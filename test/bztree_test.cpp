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

#include "bztree/bztree.hpp"

#include <functional>
#include <memory>
#include <thread>

#include "gtest/gtest.h"

using std::byte;

namespace dbgroup::index::bztree
{
// use a supper template to define key-payload pair templates
template <class KeyType, class PayloadType, class KeyComparator, class PayloadComparator>
struct KeyPayloadPair {
  using Key = KeyType;
  using Payload = PayloadType;
  using KeyComp = KeyComparator;
  using PayloadComp = PayloadComparator;
};

template <class KeyPayloadPair>
class BzTreeFixture : public testing::Test
{
 protected:
  // extract key-payload types
  using Key = typename KeyPayloadPair::Key;
  using Payload = typename KeyPayloadPair::Payload;
  using KeyComp = typename KeyPayloadPair::KeyComp;
  using PayloadComp = typename KeyPayloadPair::PayloadComp;

  // define type aliases for simplicity
  using Record_t = Record<Key, Payload>;
  using BaseNode_t = BaseNode<Key, Payload, KeyComp>;
  using LeafNode_t = LeafNode<Key, Payload, KeyComp>;
  using InternalNode_t = InternalNode<Key, Payload, KeyComp>;
  using BzTree_t = BzTree<Key, Payload, KeyComp>;
  using NodeReturnCode = typename BaseNode_t::NodeReturnCode;
  using KeyExistence = typename BaseNode_t::KeyExistence;

  // constant values for testing
  static constexpr size_t kIndexEpoch = 1;
  static constexpr size_t kKeyNumForTest = 10240;
  static constexpr size_t kSmallKeyNum = 16;
  static constexpr size_t kLargeKeyNum = 2048;
  static constexpr size_t kKeyLength = kWordLength;
  static constexpr size_t kPayloadLength = kWordLength;

  // actual keys and payloads
  size_t key_length;
  size_t payload_length;
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];

  // a test target BzTree
  BzTree_t bztree = BzTree_t{};

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp()
  {
    // prepare keys
    if constexpr (std::is_same_v<Key, char *>) {
      // variable-length keys
      key_length = 7;
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        auto key = new char[kKeyLength];
        snprintf(key, kKeyLength, "%06lu", index);
        keys[index] = key;
      }
    } else {
      // static-length keys
      key_length = sizeof(Key);
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        keys[index] = index;
      }
    }

    // prepare payloads
    if constexpr (std::is_same_v<Payload, char *>) {
      // variable-length payloads
      payload_length = 7;
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        auto payload = new char[kPayloadLength];
        snprintf(payload, kPayloadLength, "%06lu", index);
        payloads[index] = payload;
      }
    } else {
      // static-length payloads
      payload_length = sizeof(Payload);
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        payloads[index] = index;
      }
    }
  }

  void
  TearDown()
  {
    if constexpr (std::is_same_v<Key, char *>) {
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        delete[] keys[index];
      }
    }
    if constexpr (std::is_same_v<Payload, char *>) {
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        delete[] payloads[index];
      }
    }
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
    auto [rc, actual] = bztree.Read(keys[key_id]);

    if (expect_fail) {
      EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(ReturnCode::kSuccess, rc);
      if constexpr (std::is_same_v<Payload, char *>) {
        EXPECT_TRUE(IsEqual<PayloadComp>(payloads[expected_id], actual.get()));
      } else {
        EXPECT_TRUE(IsEqual<PayloadComp>(payloads[expected_id], actual));
      }
    }
  }

  void
  VerifyWrite(  //
      const size_t key_id,
      const size_t payload_id)
  {
    auto rc = bztree.Write(keys[key_id], key_length, payloads[payload_id], payload_length);

    EXPECT_EQ(ReturnCode::kSuccess, rc);
  }

  void
  VerifyInsert(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_fail = false)
  {
    auto rc = bztree.Insert(keys[key_id], key_length, payloads[payload_id], payload_length);

    if (expect_fail) {
      EXPECT_EQ(ReturnCode::kKeyExist, rc);
    } else {
      EXPECT_EQ(ReturnCode::kSuccess, rc);
    }
  }

  void
  VerifyUpdate(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_fail = false)
  {
    auto rc = bztree.Update(keys[key_id], key_length, payloads[payload_id], payload_length);

    if (expect_fail) {
      EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(ReturnCode::kSuccess, rc);
    }
  }

  void
  VerifyDelete(  //
      const size_t key_id,
      const bool expect_fail = false)
  {
    auto rc = bztree.Delete(keys[key_id], key_length);

    if (expect_fail) {
      EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(ReturnCode::kSuccess, rc);
    }
  }
};

/*##################################################################################################
 * Preparation for typed testing
 *################################################################################################*/

using Int32Comp = std::less<int32_t>;
using Int64Comp = std::less<int64_t>;
using CStrComp = dbgroup::index::bztree::CompareAsCString;

using KeyPayloadPairs = ::testing::Types<KeyPayloadPair<int64_t, int64_t, Int64Comp, Int64Comp>,
                                         KeyPayloadPair<char *, int64_t, CStrComp, Int64Comp>,
                                         KeyPayloadPair<int64_t, char *, Int64Comp, CStrComp>,
                                         KeyPayloadPair<int32_t, int32_t, Int32Comp, Int32Comp>,
                                         KeyPayloadPair<char *, int32_t, CStrComp, Int32Comp>,
                                         KeyPayloadPair<int32_t, char *, Int32Comp, CStrComp>,
                                         KeyPayloadPair<char *, char *, CStrComp, CStrComp>>;
TYPED_TEST_CASE(BzTreeFixture, KeyPayloadPairs);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

/*--------------------------------------------------------------------------------------------------
 * Read operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, Read_NotPresentKey_ReadFail)
{  //
  TestFixture::VerifyRead(0, 0, true);
}

// /*--------------------------------------------------------------------------------------------------
//  * Scan operation
//  *------------------------------------------------------------------------------------------------*/

// TYPED_TEST(BzTreeFixture, Scan_EmptyNode_NoResult)
// {
//   auto [rc, scan_results] = bztree.Scan(keys[1], true, keys[10], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TYPED_TEST(BzTreeFixture, Scan_BothClosed_ScanTargetValues)
// {
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.Scan(keys[4], true, keys[6], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(3, scan_results.size());
//   VerifyKey(keys[4], scan_results[0]->GetKey());
//   VerifyPayload(payloads[4], scan_results[0]->GetPayload());
//   VerifyKey(keys[5], scan_results[1]->GetKey());
//   VerifyPayload(payloads[5], scan_results[1]->GetPayload());
//   VerifyKey(keys[6], scan_results[2]->GetKey());
//   VerifyPayload(payloads[6], scan_results[2]->GetPayload());
// }

// TYPED_TEST(BzTreeFixture, Scan_LeftClosed_ScanTargetValues)
// {
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.Scan(keys[8], true, keys[10], false);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[8], scan_results[0]->GetKey());
//   VerifyPayload(payloads[8], scan_results[0]->GetPayload());
//   VerifyKey(keys[9], scan_results[1]->GetKey());
//   VerifyPayload(payloads[9], scan_results[1]->GetPayload());
// }

// TYPED_TEST(BzTreeFixture, Scan_RightClosed_ScanTargetValues)
// {
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.Scan(keys[8], false, keys[10], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[9], scan_results[0]->GetKey());
//   VerifyPayload(payloads[9], scan_results[0]->GetPayload());
//   VerifyKey(keys[10], scan_results[1]->GetKey());
//   VerifyPayload(payloads[10], scan_results[1]->GetPayload());
// }

// TYPED_TEST(BzTreeFixture, Scan_BothOpened_ScanTargetValues)
// {
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.Scan(keys[8], false, keys[10], false);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(1, scan_results.size());
//   VerifyKey(keys[9], scan_results[0]->GetKey());
//   VerifyPayload(payloads[9], scan_results[0]->GetPayload());
// }

// TYPED_TEST(BzTreeFixture, ScanLess_EndClosed_ScanTargetValues)
// {
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.ScanLess(keys[2], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[1], scan_results[0]->GetKey());
//   VerifyPayload(payloads[1], scan_results[0]->GetPayload());
//   VerifyKey(keys[2], scan_results[1]->GetKey());
//   VerifyPayload(payloads[2], scan_results[1]->GetPayload());
// }

// TYPED_TEST(BzTreeFixture, ScanGreater_BeginClosed_ScanTargetValues)
// {
//   WriteOrderedKeys(&bztree, 1, 10);

//   auto [rc, scan_results] = bztree.ScanGreater(keys[9], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[9], scan_results[0]->GetKey());
//   VerifyPayload(payloads[9], scan_results[0]->GetPayload());
//   VerifyKey(keys[10], scan_results[1]->GetKey());
//   VerifyPayload(payloads[10], scan_results[1]->GetPayload());
// }

// TYPED_TEST(BzTreeFixture, ScanLess_LeftOutsideRange_NoResults)
// {
//   WriteOrderedKeys(&bztree, 5, 10);

//   auto [rc, scan_results] = bztree.ScanLess(keys[3], false);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TYPED_TEST(BzTreeFixture, ScanGreater_RightOutsideRange_NoResults)
// {
//   WriteOrderedKeys(&bztree, 1, 4);

//   auto [rc, scan_results] = bztree.ScanGreater(keys[5], false);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(0, scan_results.size());
// }

// TYPED_TEST(BzTreeFixture, Scan_WithUpdateDelete_ScanLatestValues)
// {
//   WriteOrderedKeys(&bztree, 1, 5);
//   bztree.Update(keys[2], kKeyLength, payloads[0], kPayloadLength);
//   bztree.Delete(keys[3], kKeyLength);

//   auto [rc, scan_results] = bztree.Scan(keys[2], true, keys[4], true);

//   EXPECT_EQ(ReturnCode::kSuccess, rc);
//   EXPECT_EQ(2, scan_results.size());
//   VerifyKey(keys[2], scan_results[0]->GetKey());
//   VerifyPayload(payloads[0], scan_results[0]->GetPayload());
//   VerifyKey(keys[4], scan_results[1]->GetKey());
//   VerifyPayload(payloads[4], scan_results[1]->GetPayload());
// }

/*--------------------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, Write_UniqueKeys_ReadWrittenValues)
{
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BzTreeFixture, Write_DuplicateKeys_ReadLatestValue)
{
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyWrite(i, i + 1);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

TYPED_TEST(BzTreeFixture, Write_UniqueKeysWithSMOs_ReadWrittenValues)
{
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BzTreeFixture, Write_DuplicateKeysWithSMOs_ReadWrittenValues)
{
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyWrite(i, i + 1);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, Insert_UniqueKeys_ReadInsertedValues)
{
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BzTreeFixture, Insert_DuplicateKeys_InsertFail)
{
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyInsert(i, i + 1, true);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BzTreeFixture, Insert_DeletedKeys_InsertSucceed)
{
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyDelete(i);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyInsert(i, i + 1);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

TYPED_TEST(BzTreeFixture, Insert_UniqueKeysWithSMOs_ReadInsertedValues)
{
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BzTreeFixture, Insert_DuplicateKeysWithSMOs_InsertFail)
{
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyInsert(i, i + 1, true);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BzTreeFixture, Insert_DeletedKeysWithSMOs_InsertSucceed)
{
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyDelete(i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyInsert(i, i + 1);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

/*--------------------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, Update_DuplicateKeys_ReadUpdatedValues)
{
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyUpdate(i, i + 1);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

TYPED_TEST(BzTreeFixture, Update_NotInsertedKeys_UpdateFail)
{
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyUpdate(i, i, true);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyRead(i, i, true);
  }
}

TYPED_TEST(BzTreeFixture, Update_DeletedKeys_UpdateFail)
{
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyDelete(i);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyUpdate(i, i, true);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyRead(i, i, true);
  }
}

TYPED_TEST(BzTreeFixture, Update_DuplicateKeysWithSMOs_ReadUpdatedValues)
{
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyUpdate(i, i + 1);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

TYPED_TEST(BzTreeFixture, Update_NotInsertedKeysWithSMOs_UpdateFail)
{
  for (size_t i = TestFixture::kLargeKeyNum; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyUpdate(i, i, true);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyRead(i, i, true);
  }
}

TYPED_TEST(BzTreeFixture, Update_DeletedKeysWithSMOs_UpdateFail)
{
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyDelete(i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyUpdate(i, i, true);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyRead(i, i, true);
  }
}

/*--------------------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, Delete_DuplicateKeys_DeleteSucceed)
{
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyDelete(i);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyRead(i, i, true);
  }
}

TYPED_TEST(BzTreeFixture, Delete_NotInsertedKeys_DeleteFail)
{
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyDelete(i, true);
  }
}

TYPED_TEST(BzTreeFixture, Delete_DeletedKeys_DeleteFail)
{
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyDelete(i);
  }
  for (size_t i = 0; i < TestFixture::kSmallKeyNum; ++i) {
    TestFixture::VerifyDelete(i, true);
  }
}

TYPED_TEST(BzTreeFixture, Delete_DuplicateKeysWithSMOs_DeleteSucceed)
{
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyDelete(i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyRead(i, i, true);
  }
}

TYPED_TEST(BzTreeFixture, Delete_NotInsertedKeysWithSMOs_DeleteFail)
{
  for (size_t i = TestFixture::kLargeKeyNum; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyDelete(i, true);
  }
}

TYPED_TEST(BzTreeFixture, Delete_DeletedKeysWithSMOs_DeleteFail)
{
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyDelete(i);
  }
  for (size_t i = 0; i < TestFixture::kLargeKeyNum - 1; ++i) {
    TestFixture::VerifyDelete(i, true);
  }
}

}  // namespace dbgroup::index::bztree
