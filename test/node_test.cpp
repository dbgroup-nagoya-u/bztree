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

#include "bztree/component/node.hpp"

#include <functional>
#include <memory>

#include "bztree/component/leaf_node_api.hpp"
#include "gtest/gtest.h"

using std::byte;

namespace dbgroup::index::bztree::component::test
{
using Key = uint64_t;
using Payload = uint64_t;
using Node_t = Node<Key, Payload>;

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

  std::unique_ptr<Node_t> node;

 protected:
  void
  SetUp() override
  {
    node.reset(Node_t::CreateEmptyNode(kLeafFlag));

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
      Node_t* target_node,
      const size_t write_num)
  {
    for (size_t index = 0; index < write_num; ++index) {
      leaf::Write(target_node, key_null, kKeyLength, payload_null, kPayloadLength);
    }
  }

  void
  WriteOrderedKeys(  //
      Node_t* target_node,
      const size_t begin_index,
      const size_t end_index)
  {
    assert(end_index < kKeyNumForTest);

    for (size_t index = begin_index; index <= end_index; ++index) {
      leaf::Write(target_node, keys[index], kKeyLength, payloads[index], kPayloadLength);
    }
  }

  Node_t*
  CreateSortedLeafNodeWithOrderedKeys(  //
      const size_t begin_index,
      const size_t end_index)
  {
    auto tmp_leaf_node = Node_t::CreateEmptyNode(kLeafFlag);
    WriteOrderedKeys(tmp_leaf_node, begin_index, end_index);
    auto [tmp_meta, rec_count] = leaf::GatherSortedLiveMetadata(tmp_leaf_node);
    return leaf::Consolidate(tmp_leaf_node, tmp_meta, rec_count);
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
  auto tmp_node = std::unique_ptr<Node_t>(Node_t::CreateEmptyNode(kLeafFlag));
  leaf::Write(tmp_node.get(), keys[1], kKeyLength, payloads[1], kPayloadLength);
  leaf::Write(tmp_node.get(), keys[2], kKeyLength, payloads[2], kPayloadLength);
  leaf::Write(tmp_node.get(), keys[4], kKeyLength, payloads[4], kPayloadLength);
  leaf::Write(tmp_node.get(), keys[5], kKeyLength, payloads[5], kPayloadLength);
  leaf::Write(tmp_node.get(), keys[7], kKeyLength, payloads[7], kPayloadLength);
  leaf::Write(tmp_node.get(), keys[8], kKeyLength, payloads[8], kPayloadLength);
  auto [tmp_meta, rec_count] = leaf::GatherSortedLiveMetadata(tmp_node.get());

  node.reset(leaf::Consolidate(tmp_node.get(), tmp_meta, rec_count));

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

}  // namespace dbgroup::index::bztree::component::test