// Copyright (c) DB Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "status_word.hpp"

#include <gtest/gtest.h>

#include <bit>
#include <memory>

namespace bztree
{
class StatusWordFixture : public testing::Test
{
 protected:
  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }
};

TEST_F(StatusWordFixture, New_DefaultNodeSize_GetInitializedHeader) {}

}  // namespace bztree
