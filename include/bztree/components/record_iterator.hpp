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

namespace dbgroup::index::bztree
{
/**
 * @brief A class to represent a iterator for scan results.
 *
 * @tparam Key a target key class
 * @tparam Payload a target payload class
 */
template <class Key, class Payload>
class RecordIterator
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// an address of a current record
  std::byte* current_addr_;

  /// an address of the end of this page
  const std::byte* end_addr_;

  /// the length of a current key
  size_t key_length_;

  /// the length of a current payload
  size_t payload_length_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new object.
   *
   * @param src_addr a source address to copy record data
   */
  constexpr RecordIterator(  //
      const std::byte* src_addr,
      const std::byte* end_addr,
      const size_t key_length,
      const size_t payload_length)
      : current_addr_{const_cast<std::byte*>(src_addr)},
        end_addr_{end_addr},
        key_length_{key_length},
        payload_length_{payload_length}
  {
  }

  ~RecordIterator() = default;

  constexpr RecordIterator(const RecordIterator&) = default;
  constexpr RecordIterator& operator=(const RecordIterator&) = default;
  constexpr RecordIterator(RecordIterator&&) = default;
  constexpr RecordIterator& operator=(RecordIterator&&) = default;

  /*################################################################################################
   * Public operators for a iterator
   *##############################################################################################*/

  constexpr std::pair<Key, Payload>
  operator*() const
  {
    return {GetKey(), GetPayload()};
  }

  constexpr void
  operator++()
  {
    current_addr_ += GetKeyLength() + GetPayloadLength();
    if constexpr (std::is_same_v<Key, char*>) {
      if (current_addr_ != end_addr_) {
        memcpy(&key_length_, current_addr_, sizeof(size_t));
        current_addr_ += sizeof(size_t);
      }
    }
    if constexpr (std::is_same_v<Payload, char*>) {
      if (current_addr_ != end_addr_) {
        memcpy(&payload_length_, current_addr_, sizeof(size_t));
        current_addr_ += sizeof(size_t);
      }
    }
  }

  constexpr bool
  operator==(const RecordIterator& obj) const
  {
    return current_addr_ == obj.current_addr_;
  }

  constexpr bool
  operator!=(const RecordIterator& obj) const
  {
    return current_addr_ != obj.current_addr_;
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return a key of a current record
   */
  constexpr Key
  GetKey() const
  {
    if constexpr (std::is_same_v<Key, char*>) {
      return reinterpret_cast<Key>(current_addr_);
    } else {
      return *reinterpret_cast<Key*>(current_addr_);
    }
  }

  /**
   * @return a payload of a current record
   */
  constexpr Payload
  GetPayload() const
  {
    if constexpr (std::is_same_v<Payload, char*>) {
      return reinterpret_cast<Payload>(current_addr_ + GetKeyLength());
    } else {
      return *reinterpret_cast<Payload*>(current_addr_ + GetKeyLength());
    }
  }

  /**
   * @return the length of a current kay
   */
  constexpr size_t
  GetKeyLength() const
  {
    if constexpr (std::is_same_v<Key, char*>) {
      return key_length_;
    } else {
      return sizeof(Key);
    }
  }

  /**
   * @return the length of a current payload
   */
  constexpr size_t
  GetPayloadLength() const
  {
    if constexpr (std::is_same_v<Payload, char*>) {
      return payload_length_;
    } else {
      return sizeof(Payload);
    }
  }
};

}  // namespace dbgroup::index::bztree
