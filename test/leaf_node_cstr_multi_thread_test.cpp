// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "bztree/components/common.hpp"

using Key = char*;
using Payload = char*;
using Compare = dbgroup::index::bztree::CompareAsCString;
using PayloadComparator = dbgroup::index::bztree::CompareAsCString;
constexpr size_t kKeyLength = 7;
constexpr size_t kPayloadLength = 7;

#include "leaf_node_multi_thread_test.hpp"

namespace dbgroup::index::bztree
{
void
LeafNodeFixture::SetUp()
{
  for (size_t index = 0; index < kKeyNumForTest; ++index) {
    auto key = new char[kKeyLength];
    auto payload = new char[kPayloadLength];
    snprintf(key, kKeyLength, "%06lu", index + 1);
    snprintf(payload, kPayloadLength, "%06lu", index + 1);

    keys[index] = key;
    payloads[index] = payload;
  }

  auto key = new char[kKeyLength];
  auto payload = new char[kPayloadLength];
  snprintf(key, kKeyLength, "%06d", 0);
  snprintf(payload, kPayloadLength, "%06d", 0);
}

void
LeafNodeFixture::TearDown()
{
  for (size_t index = 0; index < kKeyNumForTest; ++index) {
    delete[] keys[index];
    delete[] payloads[index];
  }
}

}  // namespace dbgroup::index::bztree
