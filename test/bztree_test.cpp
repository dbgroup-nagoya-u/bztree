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

 protected:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  // constant values for testing
  static constexpr size_t kIndexEpoch = 1;
  static constexpr size_t kKeyNumForTest = 4096;
  static constexpr size_t kSmallKeyNum = 16;
  static constexpr size_t kLargeKeyNum = 2048;
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

  // a test target BzTree
  BzTree_t bztree = BzTree_t{1000};

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
    } else if constexpr (std::is_same_v<Payload, uint64_t *>) {
      // pointer payloads
      payload_length = sizeof(Payload);
      for (size_t index = 0; index < kKeyNumForTest; ++index) {
        auto payload = new uint64_t{index};
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
    if constexpr (std::is_same_v<Payload, char *> || std::is_same_v<Payload, uint64_t *>) {
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
    const auto [rc, actual] = bztree.Read(keys[key_id]);

    if (expect_fail) {
      EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(ReturnCode::kSuccess, rc);
      if constexpr (std::is_same_v<Payload, char *>) {
        EXPECT_TRUE(component::IsEqual<PayloadComp>(payloads[expected_id], actual.get()));
      } else {
        EXPECT_TRUE(component::IsEqual<PayloadComp>(payloads[expected_id], actual));
      }
    }
  }

  void
  VerifyScan(  //
      const size_t begin_key_id,
      const bool begin_null,
      bool begin_closed,
      const size_t end_key_id,
      const bool end_null,
      const bool end_closed,
      const std::vector<size_t> &expected_keys,
      const std::vector<size_t> &expected_payloads)
  {
    Key begin_key{};
    Key *begin_ptr = nullptr, *end_ptr = nullptr;
    if (!begin_null) {
      begin_key = keys[begin_key_id];
      begin_ptr = &begin_key;
    }
    if (!end_null) end_ptr = &keys[end_key_id];

    size_t count = 0;
    RecordPage_t page;
    bztree.Scan(page, begin_ptr, begin_closed, end_ptr, end_closed);
    while (!page.empty()) {
      for (auto &&[key, payload] : page) {
        EXPECT_TRUE(component::IsEqual<KeyComp>(keys[expected_keys[count]], key));
        EXPECT_TRUE(component::IsEqual<PayloadComp>(payloads[expected_payloads[count]], payload));
        ++count;
      }
      begin_key = page.GetLastKey();
      begin_ptr = &begin_key;
      begin_closed = false;

      bztree.Scan(page, begin_ptr, begin_closed, end_ptr, end_closed);
    }

    EXPECT_EQ(expected_keys.size(), count);
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
  VerifyInsert(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_fail = false)
  {
    auto rc = bztree.Insert(keys[key_id], payloads[payload_id], key_length, payload_length);

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
    auto rc = bztree.Update(keys[key_id], payloads[payload_id], key_length, payload_length);

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

using UInt32Comp = std::less<uint32_t>;
using UInt64Comp = std::less<uint64_t>;
using CStrComp = dbgroup::index::bztree::CompareAsCString;
using PtrComp = std::less<uint64_t *>;

using KeyPayloadPairs = ::testing::Types<KeyPayload<uint64_t, uint64_t, UInt64Comp, UInt64Comp>,
                                         KeyPayload<char *, uint64_t, CStrComp, UInt64Comp>,
                                         KeyPayload<uint64_t, char *, UInt64Comp, CStrComp>,
                                         KeyPayload<uint32_t, uint32_t, UInt32Comp, UInt32Comp>,
                                         KeyPayload<char *, uint32_t, CStrComp, UInt32Comp>,
                                         KeyPayload<uint32_t, char *, UInt32Comp, CStrComp>,
                                         KeyPayload<char *, char *, CStrComp, CStrComp>,
                                         KeyPayload<uint64_t, uint64_t *, UInt64Comp, PtrComp>>;
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

/*--------------------------------------------------------------------------------------------------
 * Scan operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BzTreeFixture, Scan_EmptyNode_ScanEmptyPage)
{  //
  std::vector<size_t> expected_ids;
  TestFixture::VerifyScan(0, true, true, 0, true, true, expected_ids, expected_ids);
}

TYPED_TEST(BzTreeFixture, Scan_UniqueKeys_ScanInsertedRecords)
{  //
  std::vector<size_t> expected_ids;
  for (size_t i = 0; i < TestFixture::kKeyNumForTest; ++i) {
    TestFixture::VerifyInsert(i, i);
    expected_ids.emplace_back(i);
  }

  TestFixture::VerifyScan(0, true, true, 0, true, true, expected_ids, expected_ids);
}

TYPED_TEST(BzTreeFixture, Scan_DuplicateKeys_ScanUpdatedRecords)
{  //
  std::vector<size_t> expected_keys;
  std::vector<size_t> expected_payloads;
  for (size_t i = 0; i < TestFixture::kKeyNumForTest - 1; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kKeyNumForTest - 1; ++i) {
    TestFixture::VerifyUpdate(i, i + 1);
    expected_keys.emplace_back(i);
    expected_payloads.emplace_back(i + 1);
  }

  TestFixture::VerifyScan(0, true, true, 0, true, true, expected_keys, expected_payloads);
}

TYPED_TEST(BzTreeFixture, Scan_DeletedKeys_ScanEmptyPage)
{  //
  std::vector<size_t> expected_ids;
  for (size_t i = 0; i < TestFixture::kKeyNumForTest; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kKeyNumForTest; ++i) {
    TestFixture::VerifyDelete(i);
  }

  TestFixture::VerifyScan(0, true, true, 0, true, true, expected_ids, expected_ids);
}

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
  for (size_t i = TestFixture::kLargeKeyNum; i < TestFixture::kLargeKeyNum * 2 - 1; ++i) {
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
  for (size_t i = TestFixture::kLargeKeyNum; i < TestFixture::kLargeKeyNum * 2 - 1; ++i) {
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

}  // namespace dbgroup::index::bztree::test
