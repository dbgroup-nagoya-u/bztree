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
// forward declaration to perform scanning from iterators
template <class Key, class Payload, class Compare>
class BzTree;

namespace component
{
/**
 * @brief A class to represent a iterator for scan results.
 *
 * @tparam Key a target key class
 * @tparam Payload a target payload class
 * @tparam Compare a key-comparator class
 */
template <class Key, class Payload, class Compare>
class RecordIterator
{
  using BzTree_t = BzTree<Key, Payload, Compare>;
  using RecordPage_t = RecordPage<Key, Payload>;
  using RecordPage_p = std::unique_ptr<RecordPage_t, Deleter<RecordPage_t>>;

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a pointer to BzTree to perform continuous scan
  BzTree_t* bztree_;

  /// the end of range scan
  const Key* end_key_;

  /// a flag to specify whether the end of range is closed
  bool end_is_closed_;

  /// an address of a current record
  std::byte* current_addr_;

  /// an address of the end of this page
  const std::byte* end_addr_;

  /// the length of a current key
  uint32_t key_length_;

  /// the length of a current payload
  uint32_t payload_length_;

  /// a flag to indicate the end of range scan
  bool scan_finished_;

  /// copied keys and payloads
  RecordPage_p page_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new Record Iterator object
   *
   * @param bztree a pointer to BzTree to perform continuous scan
   * @param end_key the end of range scan
   * @param end_is_closed a flag to specify whether the end of range is closed
   * @param page a pointer to a page containing keys and payloads
   * @param scan_finished a flag to indicate the end of range scan
   */
  RecordIterator(  //
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
        page_{page, Deleter<RecordPage_t>{}}
  {
  }

  ~RecordIterator() = default;

  RecordIterator(const RecordIterator&) = delete;
  RecordIterator& operator=(const RecordIterator&) = delete;
  constexpr RecordIterator(RecordIterator&&) = default;
  constexpr RecordIterator& operator=(RecordIterator&&) = default;

  /*################################################################################################
   * Public operators for iterators
   *##############################################################################################*/

  /**
   * @return std::pair<Key, Payload>: a current key and payload pair
   */
  constexpr std::pair<Key, Payload>
  operator*() const
  {
    return {GetKey(), GetPayload()};
  }

  /**
   * @brief Forward an iterator.
   *
   */
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

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @brief Check if there are any records left.
   *
   * Note that a BzTree's scan function copies a target leaf node one by one, so this
   * function may call a scan function internally to get a next leaf node.
   *
   * @retval true if there are any records left.
   * @retval false if there are no records left.
   */
  bool
  HasNext()
  {
    if (current_addr_ < end_addr_) return true;
    if (scan_finished_ || page_->Empty()) return false;

    // search a next leaf node to continue scanning
    const auto begin_key = page_->GetLastKey();
    auto page = page_.get();
    page_.release();  // reuse an allocated page instance

    *this = bztree_->Scan(&begin_key, false, end_key_, end_is_closed_, page);

    return HasNext();
  }

  /**
   * @return Key: a key of a current record
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
   * @return Payload: a payload of a current record
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
   * @return size_t: the length of a current kay
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
   * @return size_t: the length of a current payload
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
