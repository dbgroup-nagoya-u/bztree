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

#include "bztree/components/leaf_node.hpp"

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

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
class LeafNodeFixture : public testing::Test
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
  using NodeReturnCode = typename BaseNode_t::NodeReturnCode;

  // constant values for testing
  static constexpr size_t kIndexEpoch = 1;
  static constexpr size_t kKeyNumForTest = 10000;
  static constexpr size_t kKeyLength = kWordLength;
  static constexpr size_t kPayloadLength = kWordLength;

  // actual keys and payloads
  size_t key_length;
  size_t payload_length;
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];

  // the length of a record and its maximum number
  size_t record_length;
  size_t max_record_num;

  // a leaf node and its statistics
  std::unique_ptr<BaseNode_t> node;
  size_t expected_record_count;
  size_t expected_block_size;
  size_t expected_deleted_block_size;
  size_t expected_deleted_rec_count;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp()
  {
    // initialize a leaf node and expected statistics
    node.reset(BaseNode_t::CreateEmptyNode(kLeafFlag));
    expected_record_count = 0;
    expected_block_size = 0;
    expected_deleted_block_size = 0;
    expected_deleted_rec_count = 0;

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

    // set a record length and its maximum number
    if constexpr (!std::is_same_v<Payload, char *> && sizeof(Payload) == kWordLength) {
      record_length = 2 * kWordLength;
    } else {
      record_length = key_length + payload_length;
    }
    max_record_num = (kPageSize - kHeaderLength) / (record_length + kWordLength);
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
   * Operation wrappers
   *##############################################################################################*/

  auto
  Write(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_full = false)
  {
    if (!expect_full) {
      expected_record_count += 1;
      expected_block_size += record_length;
    }

    return LeafNode_t::Write(node.get(), keys[key_id], key_length, payloads[payload_id],
                             payload_length);
  }

  auto
  Insert(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_exist = false,
      const bool expect_full = false)
  {
    if (!expect_exist && !expect_full) {
      expected_record_count += 1;
      expected_block_size += record_length;
    }

    return LeafNode_t::Insert(node.get(), keys[key_id], key_length, payloads[payload_id],
                              payload_length);
  }

  auto
  Update(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_not_exist = false,
      const bool expect_full = false)
  {
    if (!expect_not_exist && !expect_full) {
      expected_record_count += 1;
      expected_block_size += record_length;
      expected_deleted_rec_count += 1;
      expected_deleted_block_size += record_length;
    }

    return LeafNode_t::Update(node.get(), keys[key_id], key_length, payloads[payload_id],
                              payload_length);
  }

  auto
  Delete(  //
      const size_t key_id,
      const bool expect_not_exist = false,
      const bool expect_full = false)
  {
    if (!expect_not_exist && !expect_full) {
      expected_record_count += 1;
      expected_block_size += key_length;
      expected_deleted_rec_count += 2;
      expected_deleted_block_size += record_length + key_length;
    }

    return LeafNode_t::Delete(node.get(), keys[key_id], key_length);
  }

  /*################################################################################################
   * Utility functions
   *##############################################################################################*/

  void
  WriteNullKey(const size_t write_num)
  {
    for (size_t i = 0; i < write_num; ++i) {
      Write(0, 0);
    }
  }

  void
  WriteOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    assert(begin_index > 0);
    assert(end_index < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      Write(index, index);
    }
  }

  void
  PrepareConsolidatedNode(  //
      const size_t begin_index,
      const size_t end_index)
  {
    WriteOrderedKeys(begin_index, end_index);
    auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());
    node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));
  }

  bool
  NodeIsFull()
  {
    const auto expected_occupied_size =
        kHeaderLength + (expected_record_count * kWordLength) + expected_block_size;
    return kPageSize - expected_occupied_size < kWordLength + key_length;
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyMetadata(  //
      const Metadata meta,
      const bool record_is_visible = true)
  {
    if (record_is_visible) {
      EXPECT_TRUE(meta.IsVisible());
      EXPECT_FALSE(meta.IsDeleted());
      EXPECT_EQ(payload_length, meta.GetPayloadLength());
    } else {
      EXPECT_FALSE(meta.IsVisible());
      EXPECT_TRUE(meta.IsDeleted());
      EXPECT_EQ(0, meta.GetPayloadLength());
    }
    EXPECT_EQ(key_length, meta.GetKeyLength());
  }

  void
  VerifyStatusWord(  //
      const StatusWord status,
      const bool status_is_frozen = false)
  {
    const auto expected_deleted_size =
        expected_deleted_block_size + kWordLength * expected_deleted_rec_count;

    EXPECT_EQ(status, node->GetStatusWord());
    if (status_is_frozen) {
      EXPECT_TRUE(status.IsFrozen());
    } else {
      EXPECT_FALSE(status.IsFrozen());
    }
    EXPECT_EQ(expected_record_count, status.GetRecordCount());
    EXPECT_EQ(expected_block_size, status.GetBlockSize());
    EXPECT_EQ(expected_deleted_size, status.GetDeletedSize());
  }

  void
  VerifyRead(  //
      const size_t key_id,
      const size_t expected_id,
      const bool expect_fail = false)
  {
    auto [rc, actual] = LeafNode_t::Read(node.get(), keys[key_id]);

    if (expect_fail) {
      EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(NodeReturnCode::kSuccess, rc);
      if constexpr (std::is_same_v<Payload, char *>) {
        EXPECT_TRUE(IsEqual<PayloadComp>(payloads[expected_id], actual.get()));
      } else {
        EXPECT_TRUE(IsEqual<PayloadComp>(payloads[expected_id], actual));
      }
    }
  }

  void
  VerifyScan(  //
      const size_t begin_id,
      const bool begin_is_closed,
      const size_t end_id,
      const bool end_is_closed,
      const std::vector<size_t> &expected_keys,
      const std::vector<size_t> &expected_payloads,
      const bool begin_is_inf = false,
      const bool end_is_inf = false)
  {
    Key *begin_key = nullptr, *end_key = nullptr;
    if (!begin_is_inf) {
      begin_key = &keys[begin_id];
    }
    if (!end_is_inf) {
      end_key = &keys[end_id];
    }

    auto [rc, results] =
        LeafNode_t::Scan(node.get(), begin_key, begin_is_closed, end_key, end_is_closed);

    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    EXPECT_EQ(expected_keys.size(), results.size());
    for (size_t i = 0; i < expected_keys.size(); ++i) {
      const auto key_id = expected_keys[i];
      const auto payload_id = expected_payloads[i];
      if constexpr (std::is_same_v<Key, char *> || std::is_same_v<Payload, char *>) {
        EXPECT_TRUE(IsEqual<KeyComp>(keys[key_id], results[i]->GetKey()));
        EXPECT_TRUE(IsEqual<PayloadComp>(payloads[payload_id], results[i]->GetPayload()));
      } else {
        EXPECT_TRUE(IsEqual<KeyComp>(keys[key_id], results[i].GetKey()));
        EXPECT_TRUE(IsEqual<PayloadComp>(payloads[payload_id], results[i].GetPayload()));
      }
    }
  }

  void
  VerifyWrite(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_full = false)
  {
    auto [rc, status] = Write(key_id, payload_id, expect_full);

    if (expect_full) {
      EXPECT_EQ(NodeReturnCode::kNoSpace, rc);
    } else {
      EXPECT_EQ(NodeReturnCode::kSuccess, rc);
      VerifyMetadata(node->GetMetadata(expected_record_count - 1));
      VerifyStatusWord(status);
    }
  }

  void
  VerifyInsert(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_exist = false,
      const bool expect_full = false)
  {
    auto [rc, status] = Insert(key_id, payload_id, expect_exist, expect_full);

    if (expect_full) {
      EXPECT_EQ(NodeReturnCode::kNoSpace, rc);
    } else if (expect_exist) {
      EXPECT_EQ(NodeReturnCode::kKeyExist, rc);
    } else {
      EXPECT_EQ(NodeReturnCode::kSuccess, rc);
      VerifyMetadata(node->GetMetadata(expected_record_count - 1));
      VerifyStatusWord(status);
    }
  }

  void
  VerifyUpdate(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_not_exist = false,
      const bool expect_full = false)
  {
    auto [rc, status] = Update(key_id, payload_id, expect_not_exist, expect_full);

    if (expect_full) {
      EXPECT_EQ(NodeReturnCode::kNoSpace, rc);
    } else if (expect_not_exist) {
      EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(NodeReturnCode::kSuccess, rc);
      VerifyMetadata(node->GetMetadata(expected_record_count - 1));
      VerifyStatusWord(status);
    }
  }

  void
  VerifyDelete(  //
      const size_t key_id,
      const bool expect_not_exist = false,
      const bool expect_full = false)
  {
    auto [rc, status] = Delete(key_id, expect_not_exist, expect_full);

    if (expect_full) {
      EXPECT_EQ(NodeReturnCode::kNoSpace, rc);
    } else if (expect_not_exist) {
      EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(NodeReturnCode::kSuccess, rc);
      VerifyMetadata(node->GetMetadata(expected_record_count - 1), false);
      VerifyStatusWord(status);
    }
  }

  void
  VerifyGatherSortedLiveMetadata(std::vector<size_t> &expected_ids)
  {
    auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());

    EXPECT_EQ(expected_ids.size(), meta_vec.size());
    for (size_t i = 0; i < expected_ids.size(); ++i) {
      const auto key_id = expected_ids[i];
      const auto [actual_key, meta] = meta_vec[i];

      EXPECT_TRUE(IsEqual<KeyComp>(keys[key_id], actual_key));
      VerifyMetadata(meta);
    }
  }

  void
  VerifyConsolidation()
  {
    auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());
    node.reset(LeafNode_t::Consolidate(node.get(), meta_vec));

    expected_record_count -= expected_deleted_rec_count;
    expected_block_size -= expected_deleted_block_size;
    expected_deleted_rec_count = 0;
    expected_deleted_block_size = 0;

    VerifyStatusWord(node->GetStatusWord());
  }

  void
  VerifySplit(  //
      const size_t begin_index,
      const size_t end_index,
      const size_t left_record_count,
      const bool target_is_left)
  {
    WriteOrderedKeys(begin_index, end_index);
    auto meta_vec = LeafNode_t::GatherSortedLiveMetadata(node.get());
    auto [left_node, right_node] = LeafNode_t::Split(node.get(), meta_vec, left_record_count);

    if (target_is_left) {
      node.reset(left_node);
      delete right_node;
    } else {
      node.reset(right_node);
      delete left_node;
    }

    expected_record_count = left_record_count;
    expected_block_size = expected_record_count * record_length;

    VerifyStatusWord(node->GetStatusWord());
  }

  void
  VerifyMerge(  //
      const size_t target_begin,
      const size_t target_end,
      const size_t sibling_begin,
      const size_t sibling_end,
      const bool sibling_is_left)
  {
    WriteOrderedKeys(target_begin, target_end);
    auto target_meta = LeafNode_t::GatherSortedLiveMetadata(node.get());

    auto sibling = std::unique_ptr<BaseNode_t>(BaseNode_t::CreateEmptyNode(kLeafFlag));
    for (size_t id = sibling_begin; id <= sibling_end; ++id) {
      LeafNode_t::Write(sibling.get(), keys[id], key_length, payloads[id], payload_length);
    }
    auto sibling_meta = LeafNode_t::GatherSortedLiveMetadata(sibling.get());

    node.reset(
        LeafNode_t::Merge(node.get(), target_meta, sibling.get(), sibling_meta, sibling_is_left));

    expected_record_count = (target_end - target_begin + 1) + (sibling_end - sibling_begin + 1);
    expected_block_size = expected_record_count * record_length;

    VerifyStatusWord(node->GetStatusWord());
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
TYPED_TEST_CASE(LeafNodeFixture, KeyPayloadPairs);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

/*--------------------------------------------------------------------------------------------------
 * Read operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(LeafNodeFixture, Read_NotInsertedKey_ReadFail)
{  //
  TestFixture::VerifyRead(1, 1, true);
}

TYPED_TEST(LeafNodeFixture, Read_DeletedKey_ReadFail)
{
  TestFixture::Insert(1, 1);
  TestFixture::Delete(1);

  TestFixture::VerifyRead(1, 1, true);
}

/*--------------------------------------------------------------------------------------------------
 * Scan operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(LeafNodeFixture, Scan_EmptyNode_NoResult)
{
  std::vector<size_t> expected_ids = {};
  TestFixture::VerifyScan(1, true, 10, true, expected_ids, expected_ids);
}

TYPED_TEST(LeafNodeFixture, Scan_BothClosed_ScanTargetValues)
{
  TestFixture::WriteOrderedKeys(1, 10);

  std::vector<size_t> expected_ids = {2, 3, 4, 5, 6, 7, 8};
  TestFixture::VerifyScan(2, true, 8, true, expected_ids, expected_ids);
}

TYPED_TEST(LeafNodeFixture, Scan_LeftClosed_ScanTargetValues)
{
  TestFixture::WriteOrderedKeys(1, 10);

  std::vector<size_t> expected_ids = {2, 3, 4, 5, 6, 7};
  TestFixture::VerifyScan(2, true, 8, false, expected_ids, expected_ids);
}

TYPED_TEST(LeafNodeFixture, Scan_RightClosed_ScanTargetValues)
{
  TestFixture::WriteOrderedKeys(1, 10);

  std::vector<size_t> expected_ids = {3, 4, 5, 6, 7, 8};
  TestFixture::VerifyScan(2, false, 8, true, expected_ids, expected_ids);
}

TYPED_TEST(LeafNodeFixture, Scan_BothOpened_ScanTargetValues)
{
  TestFixture::WriteOrderedKeys(1, 10);

  std::vector<size_t> expected_ids = {3, 4, 5, 6, 7};
  TestFixture::VerifyScan(2, false, 8, false, expected_ids, expected_ids);
}

TYPED_TEST(LeafNodeFixture, Scan_LeftInfinity_ScanTargetValues)
{
  TestFixture::WriteOrderedKeys(1, 10);

  std::vector<size_t> expected_ids = {1, 2, 3, 4, 5, 6, 7, 8};
  TestFixture::VerifyScan(2, true, 8, true, expected_ids, expected_ids, true);
}

TYPED_TEST(LeafNodeFixture, Scan_RightInfinity_ScanTargetValues)
{
  TestFixture::WriteOrderedKeys(1, 10);

  std::vector<size_t> expected_ids = {2, 3, 4, 5, 6, 7, 8, 9, 10};
  TestFixture::VerifyScan(2, true, 8, true, expected_ids, expected_ids, false, true);
}

TYPED_TEST(LeafNodeFixture, Scan_LeftOutsideRange_NoResults)
{
  TestFixture::WriteOrderedKeys(6, 10);

  std::vector<size_t> expected_ids = {};
  TestFixture::VerifyScan(1, true, 5, true, expected_ids, expected_ids);
}

TYPED_TEST(LeafNodeFixture, Scan_RightOutsideRange_NoResults)
{
  TestFixture::WriteOrderedKeys(1, 5);

  std::vector<size_t> expected_ids = {};
  TestFixture::VerifyScan(6, true, 10, true, expected_ids, expected_ids);
}

TYPED_TEST(LeafNodeFixture, Scan_WithUpdateDelete_ScanLatestValues)
{
  TestFixture::WriteOrderedKeys(1, 10);
  TestFixture::Update(5, 11);
  TestFixture::Delete(7);

  std::vector<size_t> expected_keys = {2, 3, 4, 5, 6, 8};
  std::vector<size_t> expected_payloads = {2, 3, 4, 11, 6, 8};
  TestFixture::VerifyScan(2, true, 8, true, expected_keys, expected_payloads);
}

TYPED_TEST(LeafNodeFixture, Scan_BothClosedWithConsolidatedNode_ScanTargetValues)
{
  TestFixture::PrepareConsolidatedNode(4, 7);
  TestFixture::WriteOrderedKeys(1, 3);
  TestFixture::WriteOrderedKeys(8, 10);

  std::vector<size_t> expected_ids = {2, 3, 4, 5, 6, 7, 8};
  TestFixture::VerifyScan(2, true, 8, true, expected_ids, expected_ids);
}

TYPED_TEST(LeafNodeFixture, Scan_LeftClosedWithConsolidatedNode_ScanTargetValues)
{
  TestFixture::PrepareConsolidatedNode(4, 7);
  TestFixture::WriteOrderedKeys(1, 3);
  TestFixture::WriteOrderedKeys(8, 10);

  std::vector<size_t> expected_ids = {2, 3, 4, 5, 6, 7};
  TestFixture::VerifyScan(2, true, 8, false, expected_ids, expected_ids);
}

TYPED_TEST(LeafNodeFixture, Scan_RightClosedWithConsolidatedNode_ScanTargetValues)
{
  TestFixture::PrepareConsolidatedNode(4, 7);
  TestFixture::WriteOrderedKeys(1, 3);
  TestFixture::WriteOrderedKeys(8, 10);

  std::vector<size_t> expected_ids = {3, 4, 5, 6, 7, 8};
  TestFixture::VerifyScan(2, false, 8, true, expected_ids, expected_ids);
}

TYPED_TEST(LeafNodeFixture, Scan_BothOpenedWithConsolidatedNode_ScanTargetValues)
{
  TestFixture::PrepareConsolidatedNode(4, 7);
  TestFixture::WriteOrderedKeys(1, 3);
  TestFixture::WriteOrderedKeys(8, 10);

  std::vector<size_t> expected_ids = {3, 4, 5, 6, 7};
  TestFixture::VerifyScan(2, false, 8, false, expected_ids, expected_ids);
}

TYPED_TEST(LeafNodeFixture, Scan_LeftInfinityWithConsolidatedNode_ScanTargetValues)
{
  TestFixture::PrepareConsolidatedNode(4, 7);
  TestFixture::WriteOrderedKeys(1, 3);
  TestFixture::WriteOrderedKeys(8, 10);

  std::vector<size_t> expected_ids = {1, 2, 3, 4, 5, 6, 7, 8};
  TestFixture::VerifyScan(2, true, 8, true, expected_ids, expected_ids, true);
}

TYPED_TEST(LeafNodeFixture, Scan_RightInfinityWithConsolidatedNode_ScanTargetValues)
{
  TestFixture::PrepareConsolidatedNode(4, 7);
  TestFixture::WriteOrderedKeys(1, 3);
  TestFixture::WriteOrderedKeys(8, 10);

  std::vector<size_t> expected_ids = {2, 3, 4, 5, 6, 7, 8, 9, 10};
  TestFixture::VerifyScan(2, true, 8, true, expected_ids, expected_ids, false, true);
}

TYPED_TEST(LeafNodeFixture, Scan_LeftOutsideRangeWithConsolidatedNode_NoResults)
{
  TestFixture::PrepareConsolidatedNode(6, 10);

  std::vector<size_t> expected_ids = {};
  TestFixture::VerifyScan(1, true, 5, true, expected_ids, expected_ids);
}

TYPED_TEST(LeafNodeFixture, Scan_RightOutsideRangeWithConsolidatedNode_NoResults)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  std::vector<size_t> expected_ids = {};
  TestFixture::VerifyScan(6, true, 10, true, expected_ids, expected_ids);
}

TYPED_TEST(LeafNodeFixture, Scan_WithUpdateDeleteWithConsolidatedNode_ScanLatestValues)
{
  TestFixture::PrepareConsolidatedNode(4, 7);
  TestFixture::WriteOrderedKeys(1, 3);
  TestFixture::WriteOrderedKeys(8, 10);
  TestFixture::Update(5, 11);
  TestFixture::Delete(7);

  std::vector<size_t> expected_keys = {2, 3, 4, 5, 6, 8};
  std::vector<size_t> expected_payloads = {2, 3, 4, 11, 6, 8};
  TestFixture::VerifyScan(2, true, 8, true, expected_keys, expected_payloads);
}

/*--------------------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(LeafNodeFixture, Write_UniqueKeys_ReadWrittenValues)
{
  for (size_t i = 1; i <= TestFixture::max_record_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
}

TYPED_TEST(LeafNodeFixture, Write_DuplicateKey_ReadLatestValue)
{
  TestFixture::Write(1, 1);
  TestFixture::Write(1, 2);

  TestFixture::VerifyRead(1, 2);
}

TYPED_TEST(LeafNodeFixture, Write_FilledNode_GetCorrectReturnCodes)
{
  TestFixture::WriteNullKey(TestFixture::max_record_num - 1);

  TestFixture::VerifyWrite(1, 1);
  TestFixture::VerifyWrite(2, 2, true);
}

TYPED_TEST(LeafNodeFixture, Write_ConsolidatedNode_ReadWrittenValue)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  TestFixture::VerifyWrite(6, 6);
}

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(LeafNodeFixture, Insert_UniqueKeys_ReadInsertedValues)
{
  for (size_t i = 1; i <= TestFixture::max_record_num; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
}

TYPED_TEST(LeafNodeFixture, Insert_DuplicateKey_InsertionFail)
{
  TestFixture::Insert(1, 1);

  TestFixture::VerifyInsert(1, 2, true);
  TestFixture::VerifyRead(1, 1);
}

TYPED_TEST(LeafNodeFixture, Insert_FilledNode_GetCorrectReturnCodes)
{
  TestFixture::WriteNullKey(TestFixture::max_record_num - 1);

  TestFixture::VerifyInsert(1, 1);
  TestFixture::VerifyInsert(2, 2, false, true);
}

TYPED_TEST(LeafNodeFixture, Insert_ConsolidatedNode_ReadInsertedValue)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  TestFixture::VerifyInsert(6, 6);
}

TYPED_TEST(LeafNodeFixture, Insert_ConsolidatedNodeWithDuplicateKey_InsertionFail)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  TestFixture::VerifyInsert(1, 2, true);
}

/*--------------------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(LeafNodeFixture, Update_DuplicateKey_ReadUpdatedValue)
{
  TestFixture::Insert(1, 1);

  TestFixture::VerifyUpdate(1, 2);
  TestFixture::VerifyUpdate(1, 3);
}

TYPED_TEST(LeafNodeFixture, Update_NotInsertedKey_UpdatedFail)
{
  TestFixture::VerifyUpdate(1, 2, true);
}

TYPED_TEST(LeafNodeFixture, Update_DeletedKey_UpdateFail)
{
  TestFixture::Insert(1, 1);
  TestFixture::Delete(1);

  TestFixture::VerifyUpdate(1, 2, true);
}

TYPED_TEST(LeafNodeFixture, Update_FilledNode_GetCorrectReturnCodes)
{
  TestFixture::WriteOrderedKeys(1, TestFixture::max_record_num - 1);

  TestFixture::VerifyUpdate(1, 2);
  TestFixture::VerifyUpdate(2, 3, false, true);
}

TYPED_TEST(LeafNodeFixture, Update_ConsolidatedNode_ReadUpdatedValue)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  TestFixture::VerifyUpdate(1, 2);
  TestFixture::VerifyUpdate(1, 3);
}

TYPED_TEST(LeafNodeFixture, Update_ConsolidatedNodeWithNotInsertedKey_UpdatedFail)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  TestFixture::VerifyUpdate(6, 1, true);
}

TYPED_TEST(LeafNodeFixture, Update_ConsolidatedNodeWithDeletedKey_UpdatedFail)
{
  TestFixture::PrepareConsolidatedNode(1, 5);
  TestFixture::Delete(1);

  TestFixture::VerifyUpdate(1, 2, true);
}

/*--------------------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(LeafNodeFixture, Delete_InsertedKey_DeleteSucceed)
{
  TestFixture::Insert(1, 1);

  TestFixture::VerifyDelete(1);
}

TYPED_TEST(LeafNodeFixture, Delete_NotInsertedKey_DeleteFail)
{
  TestFixture::VerifyDelete(1, true);
}

TYPED_TEST(LeafNodeFixture, Delete_DeletedKey_DeleteFail)
{
  TestFixture::Insert(1, 1);
  TestFixture::Delete(1);

  TestFixture::VerifyDelete(1, true);
}

TYPED_TEST(LeafNodeFixture, Delete_FilledNode_GetCorrectReturnCodes)
{
  TestFixture::WriteOrderedKeys(1, TestFixture::max_record_num);

  for (size_t index = 1; index <= 5; ++index) {
    TestFixture::VerifyDelete(index, false, TestFixture::NodeIsFull());
  }
}

TYPED_TEST(LeafNodeFixture, Delete_ConsolidatedNode_DeleteSucceed)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  TestFixture::VerifyDelete(1);
}

TYPED_TEST(LeafNodeFixture, Delete_ConsolidatedNodeWithNotInsertedKey_DeleteFail)
{
  TestFixture::PrepareConsolidatedNode(1, 5);

  TestFixture::VerifyDelete(6, true);
}

TYPED_TEST(LeafNodeFixture, Delete_ConsolidatedNodeWithDeletedKey_DeleteFail)
{
  TestFixture::PrepareConsolidatedNode(1, 5);
  TestFixture::Delete(1);

  TestFixture::VerifyDelete(1, true);
}

/*--------------------------------------------------------------------------------------------------
 * Consolide operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(LeafNodeFixture, GatherSortedLiveMetadata_UnsortedKeys_GatherCorrectMetadata)
{
  std::vector<size_t> ids = {2, 3, 1, 5, 4};
  for (auto &&id : ids) {
    TestFixture::Insert(id, id);
  }
  std::sort(ids.begin(), ids.end());

  TestFixture::VerifyGatherSortedLiveMetadata(ids);
}

TYPED_TEST(LeafNodeFixture, GatherSortedLiveMetadata_UnsortedKeysWithUpdate_GatherCorrectMetadata)
{
  std::vector<size_t> ids = {2, 3, 1, 5, 4};
  for (auto &&id : ids) {
    TestFixture::Insert(id, id);
  }
  TestFixture::Update(3, 4);
  std::sort(ids.begin(), ids.end());

  TestFixture::VerifyGatherSortedLiveMetadata(ids);
}

TYPED_TEST(LeafNodeFixture, GatherSortedLiveMetadata_UnsortedKeysWithDelete_GatherCorrectMetadata)
{
  std::vector<size_t> ids = {2, 3, 1, 5, 4};
  for (auto &&id : ids) {
    TestFixture::Insert(id, id);
  }
  TestFixture::Delete(3);
  ids.erase(ids.begin() + 1);
  std::sort(ids.begin(), ids.end());

  TestFixture::VerifyGatherSortedLiveMetadata(ids);
}

TYPED_TEST(LeafNodeFixture, Consolidate_UnsortedKeys_NodeHasCorrectStatus)
{
  std::vector<size_t> ids = {2, 3, 1, 5, 4};
  for (auto &&id : ids) {
    TestFixture::Insert(id, id);
  }

  TestFixture::VerifyConsolidation();
}

TYPED_TEST(LeafNodeFixture, Consolidate_UnsortedKeysWithUpdate_NodeHasCorrectStatus)
{
  std::vector<size_t> ids = {2, 3, 1, 5, 4};
  for (auto &&id : ids) {
    TestFixture::Insert(id, id);
  }
  TestFixture::Update(3, 4);
  std::sort(ids.begin(), ids.end());

  TestFixture::VerifyConsolidation();
}

TYPED_TEST(LeafNodeFixture, Consolidate_UnsortedKeysWithDelete_NodeHasCorrectStatus)
{
  std::vector<size_t> ids = {2, 3, 1, 5, 4};
  for (auto &&id : ids) {
    TestFixture::Insert(id, id);
  }
  TestFixture::Delete(3);
  ids.erase(ids.begin() + 1);
  std::sort(ids.begin(), ids.end());

  TestFixture::VerifyConsolidation();
}

/*--------------------------------------------------------------------------------------------------
 * Split operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(LeafNodeFixture, Split_SplitLeftNode_NodesHaveCorrectKeyPayloads)
{
  TestFixture::VerifySplit(1, 10, 5, true);
  for (size_t id = 1; id <= 10; ++id) {
    const bool expect_not_exist = id > 5;
    TestFixture::VerifyRead(id, id, expect_not_exist);
  }
}

TYPED_TEST(LeafNodeFixture, Split_SplitRightNode_NodesHaveCorrectKeyPayloads)
{
  TestFixture::VerifySplit(1, 10, 5, false);
  for (size_t id = 1; id <= 10; ++id) {
    const bool expect_not_exist = id <= 5;
    TestFixture::VerifyRead(id, id, expect_not_exist);
  }
}

/*--------------------------------------------------------------------------------------------------
 * Merge operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(LeafNodeFixture, Merge_LeftSiblingNode_NodesHaveCorrectKeyPayloads)
{
  TestFixture::VerifyMerge(6, 10, 1, 5, true);
  for (size_t id = 1; id <= 10; ++id) {
    TestFixture::VerifyRead(id, id);
  }
}

TYPED_TEST(LeafNodeFixture, Merge_RightSiblingNode_NodesHaveCorrectKeyPayloads)
{
  TestFixture::VerifyMerge(1, 5, 6, 10, false);
  for (size_t id = 1; id <= 10; ++id) {
    TestFixture::VerifyRead(id, id);
  }
}

}  // namespace dbgroup::index::bztree
