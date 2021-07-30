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

#include <memory>
#include <utility>

#include "common.hpp"

namespace dbgroup::index::bztree::component
{
/**
 * @brief A class to represent scan results.
 *
 * @tparam Key a target key class
 * @tparam Payload a target payload class
 */
template <class Key, class Payload>
class RecordPage
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// an address of the end of this page
  std::byte* end_addr_;

  /// an address of the last record's key
  std::byte* last_key_addr_;

  /// scan result records
  std::byte record_block_[kPageSize - kHeaderLength];

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  constexpr RecordPage() { static_assert(sizeof(RecordPage) == kPageSize); }

  ~RecordPage() = default;

  RecordPage(const RecordPage&) = delete;
  RecordPage& operator=(const RecordPage&) = delete;
  RecordPage(RecordPage&&) = delete;
  RecordPage& operator=(RecordPage&&) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr Key
  GetLastKey() const
  {
    if constexpr (std::is_same_v<Key, char*>) {
      return Cast<Key>(last_key_addr_);
    } else {
      return *Cast<Key*>(last_key_addr_);
    }
  }

  constexpr void
  SetEndAddress(const std::byte* end_addr)
  {
    end_addr_ = const_cast<std::byte*>(end_addr);
  }

  constexpr void
  SetLastKeyAddress(const std::byte* last_key_addr)
  {
    last_key_addr_ = const_cast<std::byte*>(last_key_addr);
  }

  constexpr bool
  Empty() const
  {
    return end_addr_ == record_block_;
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  constexpr std::byte*
  GetBeginAddr()
  {
    if constexpr (std::is_same_v<Key, char*> && std::is_same_v<Payload, char*>) {
      return record_block_ + (sizeof(uint32_t) + sizeof(uint32_t));
    } else if constexpr (std::is_same_v<Key, char*> || std::is_same_v<Payload, char*>) {
      return record_block_ + sizeof(uint32_t);
    } else {
      return record_block_;
    }
  }

  constexpr std::byte*
  GetEndAddr() const
  {
    return end_addr_;
  }

  constexpr uint32_t
  GetBeginKeyLength() const
  {
    if constexpr (std::is_same_v<Key, char*>) {
      return *Cast<uint32_t*>(record_block_);
    } else {
      return sizeof(Key);
    }
  }

  constexpr uint32_t
  GetBeginPayloadLength() const
  {
    if constexpr (std::is_same_v<Key, char*> && std::is_same_v<Payload, char*>) {
      return *Cast<uint32_t*>(record_block_ + sizeof(uint32_t));
    } else if constexpr (std::is_same_v<Payload, char*>) {
      return *Cast<uint32_t*>(record_block_);
    } else {
      return sizeof(Payload);
    }
  }
};

}  // namespace dbgroup::index::bztree::component
