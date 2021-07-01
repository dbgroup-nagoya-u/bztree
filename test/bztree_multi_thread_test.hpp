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

#include "bztree/bztree.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bztree
{
class BzTreeFixture : public testing::Test
{
 public:
  using Record_t = Record<Key, Payload>;
  using BaseNode_t = BaseNode<Key, Payload, Compare>;
  using LeafNode_t = LeafNode<Key, Payload, Compare>;
  using InternalNode_t = InternalNode<Key, Payload, Compare>;
  using BzTree_t = BzTree<Key, Payload, Compare>;
  using NodeReturnCode = BaseNode<Key, Payload, Compare>::NodeReturnCode;
  using KeyExistence = BaseNode<Key, Payload, Compare>::KeyExistence;
  using RunResult = std::pair<std::vector<size_t>, std::vector<size_t>>;

  enum WriteType
  {
    kWrite,
    kInsert,
    kUpdate,
    kDelete
  };

#ifdef BZTREE_TEST_THREAD_NUM
  static constexpr size_t kThreadNum = BZTREE_TEST_THREAD_NUM;
#else
  static constexpr size_t kThreadNum = 8;
#endif
  static constexpr size_t kRandSeed = 10;
  static constexpr size_t kKeyNumForTest = 10000;
  static constexpr size_t kRecordLength = kKeyLength + kPayloadLength;
  static constexpr size_t kMaxRecordNum =
      (kPageSize - kHeaderLength) / (kRecordLength + kWordLength);
  static constexpr size_t kWriteNumPerThread = kMaxRecordNum / kThreadNum;
  static constexpr size_t kIndexEpoch = 1;

  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];

  std::unique_ptr<BzTree_t> bztree;

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
      ReturnCode rc{};
      switch (w_type) {
        case kWrite:
          rc = bztree->Write(keys[index], kKeyLength, payloads[index], kPayloadLength);
          break;
        case kInsert:
          rc = bztree->Insert(keys[index], kKeyLength, payloads[index], kPayloadLength);
          break;
        case kUpdate:
          rc = bztree->Update(keys[index], kKeyLength, payloads[index + 1], kPayloadLength);
          break;
        case kDelete:
          rc = bztree->Delete(keys[index], kKeyLength);
          break;
      }
      if (rc == ReturnCode::kSuccess) {
        written_indexes.emplace_back(index);
      } else {
        failed_indexes.emplace_back(index);
      }
    }

    p.set_value(std::make_pair(std::move(written_indexes), std::move(failed_indexes)));
  }

 protected:
  BzTreeFixture() : bztree{new BzTree_t{}} {}

  void SetUp() override;

  void TearDown() override;

  RunResult
  RunOverMultiThread(  //
      const size_t write_num_per_thread,
      const size_t thread_num,
      const WriteType w_type,
      void (dbgroup::index::bztree::BzTreeFixture::*func)(
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

TEST_F(BzTreeFixture, Write_MultiThreads_ReadWrittenPayloads)
{
  auto [written_indexes, failed_indexes] =
      RunOverMultiThread(kWriteNumPerThread, kThreadNum, kWrite, &BzTreeFixture::WriteRandomKeys);

  EXPECT_EQ(kWriteNumPerThread * kThreadNum, written_indexes.size());
  for (auto&& index : written_indexes) {
    auto [rc, record] = bztree->Read(keys[index]);
    EXPECT_EQ(ReturnCode::kSuccess, rc);
    if (record != nullptr) {
      VerifyPayload(payloads[index], record->GetPayload());
    }
  }
}

TEST_F(BzTreeFixture, Insert_MultiThreads_ReadWrittenPayloads)
{
  auto [written_indexes, failed_indexes] =
      RunOverMultiThread(kWriteNumPerThread, kThreadNum, kInsert, &BzTreeFixture::WriteRandomKeys);

  EXPECT_LE(written_indexes.size(), kWriteNumPerThread);
  EXPECT_EQ(kWriteNumPerThread * kThreadNum, written_indexes.size() + failed_indexes.size());
  for (auto&& index : written_indexes) {
    auto [rc, record] = bztree->Read(keys[index]);
    EXPECT_EQ(ReturnCode::kSuccess, rc);
    if (record != nullptr) {
      VerifyPayload(payloads[index], record->GetPayload());
    }
  }
  for (auto&& index : failed_indexes) {
    auto [rc, record] = bztree->Read(keys[index]);
    EXPECT_EQ(ReturnCode::kSuccess, rc);
    if (record != nullptr) {
      VerifyPayload(payloads[index], record->GetPayload());
    }
  }
}

TEST_F(BzTreeFixture, Update_MultiThreads_ReadWrittenPayloads)
{
  RunOverMultiThread(kWriteNumPerThread * 0.5, kThreadNum, kWrite, &BzTreeFixture::WriteRandomKeys);
  auto [written_indexes, failed_indexes] = RunOverMultiThread(
      kWriteNumPerThread * 0.5, kThreadNum, kUpdate, &BzTreeFixture::WriteRandomKeys);

  EXPECT_EQ(kWriteNumPerThread * kThreadNum * 0.5, written_indexes.size());
  for (auto&& index : written_indexes) {
    auto [rc, record] = bztree->Read(keys[index]);
    EXPECT_EQ(ReturnCode::kSuccess, rc);
    if (record != nullptr) {
      VerifyPayload(payloads[index + 1], record->GetPayload());
    }
  }
}

TEST_F(BzTreeFixture, Delete_MultiThreads_KeysDeleted)
{
  RunOverMultiThread(kWriteNumPerThread * 0.5, kThreadNum, kWrite, &BzTreeFixture::WriteRandomKeys);
  auto [written_indexes, failed_indexes] = RunOverMultiThread(
      kWriteNumPerThread * 0.5, kThreadNum, kDelete, &BzTreeFixture::WriteRandomKeys);

  EXPECT_EQ(kWriteNumPerThread * kThreadNum * 0.5, written_indexes.size() + failed_indexes.size());
  for (auto&& index : written_indexes) {
    auto [rc, record] = bztree->Read(keys[index]);
    EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
  }
}

}  // namespace dbgroup::index::bztree
