// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include "bztree/components/leaf_node.hpp"

namespace dbgroup::index::bztree
{
class LeafNodeFixture : public testing::Test
{
 public:
  using NodeReturnCode = BaseNode<Key, Payload, Compare>::NodeReturnCode;
  using Record_t = Record<Key, Payload>;
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
#ifdef BZTREE_TEST_WRITE_NUM
  static constexpr size_t kWriteNumPerThread = BZTREE_TEST_WRITE_NUM;
#else
  static constexpr size_t kWriteNumPerThread = 3000;
#endif
  static constexpr size_t kRandSeed = 10;
  static constexpr size_t kKeyNumForTest = 10000;
  static constexpr size_t kRecordLength = kKeyLength + kPayloadLength;
  static constexpr size_t kNodeSize =
      kHeaderLength + (kWordLength + kRecordLength) * (kWriteNumPerThread * kThreadNum);
  static constexpr size_t kIndexEpoch = 0;

  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];

  std::unique_ptr<LeafNode_t> node;

  void
  WriteRandomKeys(  //
      const size_t write_num,
      const size_t rand_seed,
      const WriteType w_type,
      std::promise<RunResult> p)
  {
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
          std::tie(rc, s) = node->Write(keys[index], kKeyLength, payloads[index], kPayloadLength);
          break;
        case kInsert:
          std::tie(rc, s) = node->Insert(keys[index], kKeyLength, payloads[index], kPayloadLength);
          break;
        case kUpdate:
          std::tie(rc, s) =
              node->Update(keys[index], kKeyLength, payloads[index + 1], kPayloadLength);
          break;
        case kDelete:
          std::tie(rc, s) = node->Delete(keys[index], kKeyLength);
          break;
        case kMixed:
          switch (index % 3) {
            case 0:
              node->Insert(keys[0], kKeyLength, payloads[0], kPayloadLength);
              break;
            case 1:
              node->Update(keys[0], kKeyLength, payloads[1], kPayloadLength);
              break;
            default:
              node->Delete(keys[0], kKeyLength);
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
  LeafNodeFixture() : node{LeafNode_t::CreateEmptyNode(kNodeSize)} {}

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
    auto [rc, record] = node->Read(keys[index]);
    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index], record->GetPayload());
  }
}

TEST_F(LeafNodeFixture, Insert_MultiThreads_ReadWrittenPayloads)
{
  auto [written_indexes, failed_indexes] = RunOverMultiThread(
      kWriteNumPerThread, kThreadNum, kInsert, &LeafNodeFixture::WriteRandomKeys);

  EXPECT_EQ(kWriteNumPerThread * kThreadNum, written_indexes.size() + failed_indexes.size());
  for (auto&& index : written_indexes) {
    auto [rc, record] = node->Read(keys[index]);
    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index], record->GetPayload());
  }
  for (auto&& index : failed_indexes) {
    auto [rc, record] = node->Read(keys[index]);
    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index], record->GetPayload());
  }
}

TEST_F(LeafNodeFixture, Update_MultiThreads_ReadWrittenPayloads)
{
  RunOverMultiThread(kWriteNumPerThread * 0.5, kThreadNum, kWrite,
                     &LeafNodeFixture::WriteRandomKeys);
  auto [written_indexes, failed_indexes] = RunOverMultiThread(
      kWriteNumPerThread * 0.5, kThreadNum, kUpdate, &LeafNodeFixture::WriteRandomKeys);

  EXPECT_EQ(kWriteNumPerThread * kThreadNum * 0.5, written_indexes.size());
  for (auto&& index : written_indexes) {
    auto [rc, record] = node->Read(keys[index]);
    EXPECT_EQ(NodeReturnCode::kSuccess, rc);
    VerifyPayload(payloads[index + 1], record->GetPayload());
  }
}

TEST_F(LeafNodeFixture, Delete_MultiThreads_KeysDeleted)
{
  RunOverMultiThread(kWriteNumPerThread * 0.5, kThreadNum, kWrite,
                     &LeafNodeFixture::WriteRandomKeys);
  auto [written_indexes, failed_indexes] = RunOverMultiThread(
      kWriteNumPerThread * 0.5, kThreadNum, kDelete, &LeafNodeFixture::WriteRandomKeys);

  EXPECT_EQ(kWriteNumPerThread * kThreadNum * 0.5, written_indexes.size() + failed_indexes.size());
  for (auto&& index : written_indexes) {
    auto [rc, record] = node->Read(keys[index]);
    EXPECT_EQ(NodeReturnCode::kKeyNotExist, rc);
  }
}

TEST_F(LeafNodeFixture, InsertUpdateDelete_MultiThreads_ConcurrencyControlCorrupted)
{
  RunOverMultiThread(kWriteNumPerThread, kThreadNum, kMixed, &LeafNodeFixture::WriteRandomKeys);

  const auto status = node->GetStatusWord();
  bool previous_is_update{false};
  bool concurrency_is_corrupted{false};
  for (int64_t index = status.GetRecordCount() - 1; index >= 0; --index) {
    const auto meta = node->GetMetadata(index);
    if (meta.IsVisible()) {  // insert or update
      const auto record = node->GetRecord(meta);
      if (IsEqual<PayloadComparator>(payloads[1], record->GetPayload())) {
        previous_is_update = true;
      } else {
        previous_is_update = false;
      }
    } else if (meta.IsDeleted()) {  // delete
      if (previous_is_update) {
        concurrency_is_corrupted = true;
      }
      previous_is_update = false;
    }
  }

  EXPECT_TRUE(concurrency_is_corrupted);
}

}  // namespace dbgroup::index::bztree
