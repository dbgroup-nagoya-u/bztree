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

#include "bztree/components/base_node.hpp"

#include <gtest/gtest.h>

#include <memory>

#include "bztree/components/leaf_node.hpp"

using std::byte;

namespace dbgroup::index::bztree
{
using Key = uint64_t;
using Payload = uint64_t;
using Record_t = Record<Key, Payload>;
using BaseNode_t = BaseNode<Key, Payload>;
using LeafNode_t = LeafNode<Key, Payload>;
using NodeReturnCode = BaseNode<Key, Payload>::NodeReturnCode;
using KeyExistence = BaseNode<Key, Payload>::KeyExistence;

static constexpr size_t kNodeSize = 256;
static constexpr size_t kIndexEpoch = 0;
static constexpr size_t kKeyNumForTest = 10000;
static constexpr size_t kKeyLength = sizeof(Key);
static constexpr size_t kPayloadLength = sizeof(Payload);
static constexpr size_t kRecordLength = kKeyLength + kPayloadLength;

class BaseNodeFixture : public testing::Test
{
 public:
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];
  Key key_null = 0;          // null key must have 8 bytes to fill a node
  Payload payload_null = 0;  // null payload must have 8 bytes to fill a node

  std::unique_ptr<BaseNode_t> node;

 protected:
  void
  SetUp() override
  {
    node.reset(BaseNode_t::CreateEmptyNode(kLeafFlag));

    for (size_t index = 0; index < kKeyNumForTest; index++) {
      keys[index] = index + 1;
      payloads[index] = index + 1;
    }
  }

  void
  TearDown() override
  {
  }

  void
  WriteNullKey(  //
      BaseNode_t* target_node,
      const size_t write_num)
  {
    for (size_t index = 0; index < write_num; ++index) {
      LeafNode_t::Write(target_node, key_null, kKeyLength, payload_null, kPayloadLength);
    }
  }

  void
  WriteOrderedKeys(  //
      BaseNode_t* target_node,
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      LeafNode_t::Write(target_node, keys[index], kKeyLength, payloads[index], kPayloadLength);
    }
  }

  BaseNode_t*
  CreateSortedLeafNodeWithOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    auto tmp_leaf_node = BaseNode_t::CreateEmptyNode(kLeafFlag);
    WriteOrderedKeys(tmp_leaf_node, begin_index, end_index);
    auto tmp_meta = LeafNode_t::GatherSortedLiveMetadata(tmp_leaf_node);
    return LeafNode_t::Consolidate(tmp_leaf_node, tmp_meta);
  }
};

TEST_F(BaseNodeFixture, New_EmptyNode_CorrectlyInitialized)
{
  auto status = *CastAddress<StatusWord*>(ShiftAddress(node.get(), kWordLength));

  EXPECT_EQ(0, node->GetSortedCount());
  EXPECT_EQ(status, node->GetStatusWord());
}

TEST_F(BaseNodeFixture, Freeze_NotFrozenNode_FreezeNode)
{
  auto rc = node->Freeze();
  auto status = node->GetStatusWord();

  EXPECT_EQ(NodeReturnCode::kSuccess, rc);
  EXPECT_TRUE(status.IsFrozen());
}

TEST_F(BaseNodeFixture, Freeze_FrozenNode_FreezeFailed)
{
  node->Freeze();
  auto rc = node->Freeze();
  auto status = node->GetStatusWord();

  EXPECT_EQ(NodeReturnCode::kFrozen, rc);
  EXPECT_TRUE(status.IsFrozen());
}

TEST_F(BaseNodeFixture, SearchSortedMeta_SearchPresentKeyWithClosedRange_FindKeyIndex)
{
  node.reset(CreateSortedLeafNodeWithOrderedKeys(0, 9));
  auto target_index = 7;
  auto target_key = keys[target_index];

  auto [rc, index] = node->SearchSortedMetadata(target_key, true);

  EXPECT_EQ(KeyExistence::kExist, rc);
  EXPECT_EQ(target_index, index);

  target_index = 3;
  target_key = keys[target_index];

  std::tie(rc, index) = node->SearchSortedMetadata(target_key, true);

  EXPECT_EQ(KeyExistence::kExist, rc);
  EXPECT_EQ(target_index, index);
}

TEST_F(BaseNodeFixture, SearchSortedMeta_SearchPresentKeyWithOpenedRange_FindNextIndex)
{
  node.reset(CreateSortedLeafNodeWithOrderedKeys(0, 9));
  auto target_index = 7;
  auto target_key = keys[target_index];

  auto [rc, index] = node->SearchSortedMetadata(target_key, false);

  EXPECT_EQ(KeyExistence::kExist, rc);
  EXPECT_EQ(target_index + 1, index);

  target_index = 3;
  target_key = keys[target_index];

  std::tie(rc, index) = node->SearchSortedMetadata(target_key, false);

  EXPECT_EQ(KeyExistence::kExist, rc);
  EXPECT_EQ(target_index + 1, index);
}

TEST_F(BaseNodeFixture, SearchSortedMeta_SearchNotPresentKey_FindNextIndex)
{
  // prepare a target node
  auto tmp_node = std::unique_ptr<BaseNode_t>(BaseNode_t::CreateEmptyNode(kLeafFlag));
  LeafNode_t::Write(tmp_node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);
  LeafNode_t::Write(tmp_node.get(), keys[2], kKeyLength, payloads[2], kPayloadLength);
  LeafNode_t::Write(tmp_node.get(), keys[4], kKeyLength, payloads[4], kPayloadLength);
  LeafNode_t::Write(tmp_node.get(), keys[5], kKeyLength, payloads[5], kPayloadLength);
  LeafNode_t::Write(tmp_node.get(), keys[7], kKeyLength, payloads[7], kPayloadLength);
  LeafNode_t::Write(tmp_node.get(), keys[8], kKeyLength, payloads[8], kPayloadLength);
  auto tmp_meta = LeafNode_t::GatherSortedLiveMetadata(tmp_node.get());

  node.reset(LeafNode_t::Consolidate(tmp_node.get(), tmp_meta));

  // perform tests
  auto target_index = 3;
  auto target_key = keys[target_index];

  auto [rc, index] = node->SearchSortedMetadata(target_key, true);

  EXPECT_EQ(KeyExistence::kNotExist, rc);
  EXPECT_EQ(target_index - 1, index);

  target_index = 6;
  target_key = keys[target_index];

  std::tie(rc, index) = node->SearchSortedMetadata(target_key, false);

  EXPECT_EQ(KeyExistence::kNotExist, rc);
  EXPECT_EQ(target_index - 2, index);
}

TEST_F(BaseNodeFixture, SearchSortedMeta_SearchOutOfNodeKey_FindBorderIndex)
{
  node.reset(CreateSortedLeafNodeWithOrderedKeys(0, 9));

  auto [rc, index] = node->SearchSortedMetadata(key_null, true);

  EXPECT_EQ(KeyExistence::kNotExist, rc);
  EXPECT_EQ(0, index);

  auto target_index = 10;
  auto target_key = keys[target_index];

  std::tie(rc, index) = node->SearchSortedMetadata(target_key, false);

  EXPECT_EQ(KeyExistence::kNotExist, rc);
  EXPECT_EQ(target_index, index);
}

}  // namespace dbgroup::index::bztree
