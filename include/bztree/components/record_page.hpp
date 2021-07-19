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

#include <utility>

#include "common.hpp"
#include "record_iterator.hpp"

namespace dbgroup::index::bztree
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
  using RecordIterator_t = RecordIterator<Key, Payload>;

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// an address of the end of this page
  std::byte* end_addr_;

  /// an address of the last record's key
  Key* last_key_addr_;

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
  constexpr RecordPage(RecordPage&&) = default;
  constexpr RecordPage& operator=(RecordPage&&) = default;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  constexpr Key
  GetLastKey() const
  {
    return *last_key_addr_;
  }

  constexpr void
  SetEndAddress(const std::byte* end_addr)
  {
    end_addr_ = end_addr;
  }

  constexpr void
  SetLastKeyAddress(const void* last_key_addr)
  {
    last_key_addr_ = reinterpret_cast<Key*>(const_cast<void*>(last_key_addr));
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  constexpr RecordIterator_t
  begin() const
  {
    if (this->empty()) return this->end();

    if constexpr (std::is_same_v<Key, char*> && std::is_same_v<Payload, char*>) {
      size_t key_length{}, payload_length{};
      memcpy(&key_length, record_block_, sizeof(size_t));
      memcpy(&payload_length, record_block_ + key_length, sizeof(size_t));
      return RecordIterator_t{record_block_ + key_length + payload_length, end_addr_,
                              std::move(key_length), std::move(payload_length)};
    } else if (std::is_same_v<Key, char*> && !std::is_same_v<Payload, char*>) {
      size_t key_length{};
      memcpy(&key_length, record_block_, sizeof(size_t));
      return RecordIterator_t{record_block_ + key_length, end_addr_,  //
                              std::move(key_length), sizeof(Payload)};
    } else if (!std::is_same_v<Key, char*> && std::is_same_v<Payload, char*>) {
      size_t payload_length{};
      memcpy(&payload_length, record_block_, sizeof(size_t));
      return RecordIterator_t{record_block_ + payload_length, end_addr_,  //
                              sizeof(Key), std::move(payload_length)};
    } else {
      return RecordIterator_t{record_block_, end_addr_, sizeof(Key), sizeof(Payload)};
    }
  }

  constexpr RecordIterator_t
  end() const
  {
    return RecordIterator_t{end_addr_, end_addr_, sizeof(Key), sizeof(Payload)};
  }

  constexpr bool
  empty() const
  {
    return end_addr_ == record_block_;
  }
};

}  // namespace dbgroup::index::bztree
