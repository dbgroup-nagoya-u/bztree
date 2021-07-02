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

#pragma once

#include <future>
#include <memory>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include "bztree/components/leaf_node.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bztree
{
class LeafNodeFixture : public testing::Test
{
 public:
  using NodeReturnCode = BaseNode<Key, Payload, Compare>::NodeReturnCode;
  using Record_t = Record<Key, Payload>;
  using BaseNode_t = BaseNode<Key, Payload, Compare>;
  using LeafNode_t = LeafNode<Key, Payload, Compare>;
  using RunResult = std::pair<std::vector<size_t>, std::vector<size_t>>;

  enum WriteType
  {
    kWrite,
    kInsert,
    kUpdate,
    kDelete,
    kMixed
  };

#ifdef BZTREE_TEST_THREAD_NUM
  static constexpr size_t kThreadNum = BZTREE_TEST_THREAD_NUM;
#else
  static constexpr size_t kThreadNum = 8;
#endif
  static constexpr size_t kRandSeed = 10;
  static constexpr size_t kKeyNumForTest = 10000;
  static constexpr size_t kRecordLength = kKeyLength + kPayloadLength;
  static constexpr size_t kIndexEpoch = 1;
  static constexpr size_t kMaxRecordNum =
      (kPageSize - kHeaderLength) / (kRecordLength + kWordLength);
  static constexpr size_t kWriteNumPerThread = kMaxRecordNum / kThreadNum;

  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];

  std::unique_ptr<BaseNode_t> node;

  void
  WriteRandomKeys(  //
      const size_t write_num,
      size_t rand_seed,
      const WriteType w_type,
      std::promise<RunResult> p)
  {
    if (w_type == WriteType::kInsert) {
      rand_seed = 0;  // use duplicate keys for insert test
    }

    std::mt19937_64 rand_engine(rand_seed);

    std::vector<size_t> written_indexes, failed_indexes;
    written_indexes.reserve(write_num);
    failed_indexes.reserve(write_num);

    for (size_t count = 0; count < write_num; ++count) {
      const auto index = rand_engine() % (kKeyNumForTest - 1);
      NodeReturnCode rc{};
      StatusWord s{};
      switch (w_type) {
        case kWrite:
          std::tie(rc, s) = LeafNode_t::Write(node.get(), keys[index], kKeyLength, payloads[index],
                                              kPayloadLength);
          break;
        case kInsert:
          std::tie(rc, s) = LeafNode_t::Insert(node.get(), keys[index], kKeyLength, payloads[index],
                                               kPayloadLength);
          break;
        case kUpdate:
          std::tie(rc, s) = LeafNode_t::Update(node.get(), keys[index], kKeyLength,
                                               payloads[index + 1], kPayloadLength);
          break;
        case kDelete:
          std::tie(rc, s) = LeafNode_t::Delete(node.get(), keys[index], kKeyLength);
          break;
        case kMixed:
          switch (index % 3) {
            case 0:
              LeafNode_t::Insert(node.get(), keys[0], kKeyLength, payloads[0], kPayloadLength);
              break;
            case 1:
              LeafNode_t::Update(node.get(), keys[0], kKeyLength, payloads[1], kPayloadLength);
              break;
            default:
              LeafNode_t::Delete(node.get(), keys[0], kKeyLength);
              break;
          }
          break;
      }
      if (rc == NodeReturnCode::kSuccess) {
        written_indexes.emplace_back(index);
      } else {
        failed_indexes.emplace_back(index);
      }
    }

    p.set_value(std::make_pair(std::move(written_indexes), std::move(failed_indexes)));
  }

 protected:
  LeafNodeFixture() : node{BaseNode_t::CreateEmptyNode(kLeafFlag)} {}

  void SetUp() override;

  void TearDown() override;

  RunResult
  RunOverMultiThread(  //
      const size_t write_num_per_thread,
      const size_t thread_num,
      const WriteType w_type,
      void (dbgroup::index::bztree::LeafNodeFixture::*func)(
          size_t, size_t, WriteType, std::promise<RunResult>))
  {
    std::mt19937_64 rand_engine(kRandSeed);

    // run a function over multi-threads with promise
    std::vector<std::future<RunResult>> futures;
    for (size_t thread = 0; thread < thread_num; ++thread) {
      std::promise<RunResult> p;
      futures.emplace_back(p.get_future());
      const auto rand_seed = rand_engine();
      std::thread{func, this, write_num_per_thread, rand_seed, w_type, std::move(p)}.detach();
    }

    // gather results via promise-future
    std::vector<size_t> written_indexes, failed_indexes;
    written_indexes.reserve(write_num_per_thread * thread_num);
    for (auto&& future : futures) {
      auto [tmp_written, tmp_failed] = future.get();
      written_indexes.insert(written_indexes.end(), tmp_written.begin(), tmp_written.end());
      failed_indexes.insert(failed_indexes.end(), tmp_failed.begin(), tmp_failed.end());
    }

    return {written_indexes, failed_indexes};
  }

  void
  VerifyKey(  //
      const Key expected,
      const Key actual)
  {
    EXPECT_TRUE(IsEqual<Compare>(expected, actual));
  }

  void
  VerifyPayload(  //
      const Payload expected,
      const Payload actual)
  {
    EXPECT_TRUE(IsEqual<PayloadComparator>(expected, actual));
  }
};

TEST_F(LeafNodeFixture, Write_MultiThreads_ReadWrittenPayloads)
{
  auto [written_indexes, failed_indexes] =
      RunOverMultiThread(kWriteNumPerThread, kThreadNum, kWrite, &LeafNodeFixture::WriteRandomKeys);

  EXPECT_EQ(kWriteNumPerThread * kThreadNum, written_indexes.size());
  for (auto&& index : written_indexes) {
    auto [rc, record] = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[index]);
    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index], record->GetPayload());
  }
}

TEST_F(LeafNodeFixture, Insert_MultiThreads_ReadWrittenPayloads)
{
  auto [written_indexes, failed_indexes] = RunOverMultiThread(
      kWriteNumPerThread, kThreadNum, kInsert, &LeafNodeFixture::WriteRandomKeys);

  EXPECT_LE(written_indexes.size(), kWriteNumPerThread);
  EXPECT_EQ(kWriteNumPerThread * kThreadNum, written_indexes.size() + failed_indexes.size());
  for (auto&& index : written_indexes) {
    auto [rc, record] = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[index]);
    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index], record->GetPayload());
  }
  for (auto&& index : failed_indexes) {
    auto [rc, record] = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[index]);
    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index], record->GetPayload());
  }
}

TEST_F(LeafNodeFixture, Update_MultiThreads_ReadWrittenPayloads)
{
  constexpr size_t kWriteNumHalf = kWriteNumPerThread * 0.5;

  RunOverMultiThread(kWriteNumHalf, kThreadNum, kWrite, &LeafNodeFixture::WriteRandomKeys);
  auto [written_indexes, failed_indexes] =
      RunOverMultiThread(kWriteNumHalf, kThreadNum, kUpdate, &LeafNodeFixture::WriteRandomKeys);

  EXPECT_EQ(kWriteNumHalf * kThreadNum, written_indexes.size());
  for (auto&& index : written_indexes) {
    auto [rc, record] = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[index]);
    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index + 1], record->GetPayload());
  }
}

TEST_F(LeafNodeFixture, Delete_MultiThreads_KeysDeleted)
{
  constexpr size_t kWriteNumHalf = kWriteNumPerThread * 0.5;

  RunOverMultiThread(kWriteNumHalf, kThreadNum, kWrite, &LeafNodeFixture::WriteRandomKeys);
  auto [written_indexes, failed_indexes] =
      RunOverMultiThread(kWriteNumHalf, kThreadNum, kDelete, &LeafNodeFixture::WriteRandomKeys);

  EXPECT_EQ(kWriteNumHalf * kThreadNum, written_indexes.size() + failed_indexes.size());
  for (auto&& index : written_indexes) {
    auto [rc, record] = LeafNode_t::Read(reinterpret_cast<BaseNode_t*>(node.get()), keys[index]);
    EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
  }
}

TEST_F(LeafNodeFixture, InsertUpdateDelete_MultiThreads_ConcurrencyControlCorrupted)
{
  constexpr size_t kWriteNumForTwoThreads = kMaxRecordNum / 2;
  for (size_t i = 0; i < 100; ++i) {
    // insert/update/delete the same key by multi-threads
    node.reset(BaseNode_t::CreateEmptyNode(kLeafFlag));
    RunOverMultiThread(kWriteNumForTwoThreads, 2, kMixed, &LeafNodeFixture::WriteRandomKeys);
    bool previous_is_update = false;
    bool concurrency_is_corrupted = false;

    const auto status = node->GetStatusWord();
    // check inserted/updated/deleted records linearly
    for (int64_t index = status.GetRecordCount() - 1; index >= 0; --index) {
      const auto meta = node->GetMetadata(index);
      if (meta.IsVisible()) {
        // an inserted or updated record
        const auto record = node->GetRecord(meta);
        if (IsEqual<PayloadComparator>(payloads[1], record->GetPayload())) {
          // 1 is an updated value
          previous_is_update = true;
        } else {
          // 0 is an inserted value
          previous_is_update = false;
        }
      } else if (meta.IsDeleted()) {
        // a deleted record
        if (previous_is_update) {
          // updating a deleted value is invalid
          concurrency_is_corrupted = true;
        }
        previous_is_update = false;
      }
    }

    EXPECT_FALSE(concurrency_is_corrupted);
    if (concurrency_is_corrupted) break;
  }
}

}  // namespace dbgroup::index::bztree
