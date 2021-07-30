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
#include "record_page.hpp"

namespace dbgroup::index::bztree
{
template <class Key, class Payload, class Compare>
class BzTree;

namespace component
{
/**
 * @brief A class to represent a iterator for scan results.
 *
 * @tparam Key a target key class
 * @tparam Payload a target payload class
 */
template <class Key, class Payload, class Compare>
class RecordIterator
{
  using BzTree_t = BzTree<Key, Payload, Compare>;
  using RecordPage_t = RecordPage<Key, Payload>;

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  BzTree_t* bztree_;

  const Key* end_key_;

  bool end_is_closed_;

  /// an address of a current record
  std::byte* current_addr_;

  /// an address of the end of this page
  const std::byte* end_addr_;

  /// the length of a current key
  uint32_t key_length_;

  /// the length of a current payload
  uint32_t payload_length_;

  ///
  bool scan_finished_;

  /// copied keys and payloads
  std::unique_ptr<RecordPage_t> page_;

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
      BzTree_t* bztree,
      const Key* end_key,
      const bool end_is_closed,
      RecordPage_t* page,
      const bool scan_finished)
      : bztree_{bztree},
        end_key_{end_key},
        end_is_closed_{end_is_closed},
        current_addr_{page->GetBeginAddr()},
        end_addr_{page->GetEndAddr()},
        key_length_{page->GetBeginKeyLength()},
        payload_length_{page->GetBeginPayloadLength()},
        scan_finished_{scan_finished},
        page_{page}
  {
  }

  ~RecordIterator() = default;

  constexpr RecordIterator(const RecordIterator&) = delete;
  constexpr RecordIterator& operator=(const RecordIterator&) = delete;
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

  void
  operator++()
  {
    current_addr_ += GetKeyLength() + GetPayloadLength();
    if constexpr (std::is_same_v<Key, char*>) {
      if (current_addr_ != end_addr_) {
        memcpy(&key_length_, current_addr_, sizeof(uint32_t));
        current_addr_ += sizeof(uint32_t);
      }
    }
    if constexpr (std::is_same_v<Payload, char*>) {
      if (current_addr_ != end_addr_) {
        memcpy(&payload_length_, current_addr_, sizeof(uint32_t));
        current_addr_ += sizeof(uint32_t);
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

  bool
  HasNext()
  {
    if (current_addr_ < end_addr_) return true;
    if (scan_finished_ || page_->Empty()) return false;

    const auto begin_key = page_->GetLastKey();
    auto page = page_.get();
    page_.release();

    *this = bztree_->Scan(&begin_key, false, end_key_, end_is_closed_, page);

    return HasNext();
  }

  /**
   * @return a key of a current record
   */
  constexpr Key
  GetKey() const
  {
    if constexpr (std::is_same_v<Key, char*>) {
      return Cast<Key>(current_addr_);
    } else {
      return *Cast<Key*>(current_addr_);
    }
  }

  /**
   * @return a payload of a current record
   */
  constexpr Payload
  GetPayload() const
  {
    if constexpr (std::is_same_v<Payload, char*>) {
      return Cast<Payload>(current_addr_ + GetKeyLength());
    } else {
      return *Cast<Payload*>(current_addr_ + GetKeyLength());
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

}  // namespace component
}  // namespace dbgroup::index::bztree
