// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "bztree/components/common.hpp"

using Key = uint64_t;
using Payload = uint64_t;
using Compare = std::less<Key>;
using PayloadComparator = std::less<Payload>;
constexpr size_t kKeyLength = sizeof(Key);
constexpr size_t kPayloadLength = sizeof(Payload);

#include "leaf_node_multi_thread_test.hpp"

namespace dbgroup::index::bztree
{
void
LeafNodeFixture::SetUp()
{
  for (size_t index = 0; index < kKeyNumForTest; ++index) {
    keys[index] = index + 1;
    payloads[index] = index + 1;
  }
}

void
LeafNodeFixture::TearDown()
{
}

}  // namespace dbgroup::index::bztree
