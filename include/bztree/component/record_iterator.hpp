/*
 * Copyright 2023 Database Group, Nagoya University
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

#ifndef BZTREE_COMPONENT_RECORD_ITERATOR_HPP
#define BZTREE_COMPONENT_RECORD_ITERATOR_HPP

// local sources
#include "bztree/component/common.hpp"
#include "bztree/component/node.hpp"

namespace dbgroup::index::bztree
{
// forward declaratoin
template <class K, class V, class C>
class BzTree;

namespace component
{
/**
 * @brief A class to represent a iterator for scan results.
 *
 * @tparam Key a target key class
 * @tparam Payload a target payload class
 * @tparam Comp a key-comparator class
 */
template <class Key, class Payload, class Comp>
class RecordIterator
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;
  using Node_t = Node<Key, Comp>;
  using BzTree_t = BzTree<Key, Payload, Comp>;

 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  RecordIterator(  //
      BzTree_t *bztree,
      Node_t *node,
      const size_t begin_pos,
      const size_t end_pos,
      const ScanKey end_key,
      const bool is_right_end)
      : bztree_{bztree},
        node_{node},
        record_count_{end_pos},
        current_pos_{begin_pos},
        current_meta_{node->GetMetadata(current_pos_)},
        end_key_{std::move(end_key)},
        is_right_end_{is_right_end}
  {
  }

  constexpr RecordIterator &
  operator=(RecordIterator &&obj) noexcept
  {
    node_ = obj.node_;
    record_count_ = obj.record_count_;
    current_pos_ = obj.current_pos_;
    current_meta_ = obj.current_meta_;
    is_right_end_ = obj.is_right_end_;

    return *this;
  }

  RecordIterator(const RecordIterator &) = delete;
  RecordIterator &operator=(const RecordIterator &) = delete;
  RecordIterator(RecordIterator &&) = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~RecordIterator() = default;

  /*####################################################################################
   * Public operators for iterators
   *##################################################################################*/

  /**
   * @retval true if this iterator indicates a live record.
   * @retval false otherwise.
   */
  explicit
  operator bool()
  {
    return HasRecord();
  }

  /**
   * @return a current key and payload pair
   */
  constexpr auto
  operator*() const  //
      -> std::pair<Key, Payload>
  {
    return {GetKey(), GetPayload()};
  }

  /**
   * @brief Forward an iterator.
   *
   */
  constexpr void
  operator++()
  {
    ++current_pos_;
    current_meta_ = node_->GetMetadata(current_pos_);
  }

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @brief Check if there are any records left.
   *
   * function may call a scan function internally to get a next leaf node.
   *
   * @retval true if there are any records or next node left.
   * @retval false if there are no records and node left.
   */
  auto
  HasRecord()  //
      -> bool
  {
    while (true) {
      if (current_pos_ < record_count_) return true;  // records remain in this node
      if (is_right_end_) return false;                // this node is the end of range-scan

      // update this iterator with the next scan results
      const auto &next_key = node_->GetHighKey();
      *this = bztree_->Scan(std::make_tuple(next_key, 0, kClosed), end_key_);
    }
  }

  /**
   * @return a key of a current record
   */
  [[nodiscard]] constexpr auto
  GetKey() const  //
      -> Key
  {
    return node_->GetKey(current_meta_);
  }

  /**
   * @return a payload of a current record
   */
  [[nodiscard]] constexpr auto
  GetPayload() const  //
      -> Payload
  {
    return node_->template GetPayload<Payload>(current_meta_);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a pointer to BwTree to perform continuous scan
  BzTree_t *bztree_{nullptr};

  /// the pointer to a node that includes partial scan results
  Node_t *node_{nullptr};

  /// the number of records in this node
  size_t record_count_{0};

  /// the position of a current record
  size_t current_pos_{0};

  /// the metadata of a current record
  Metadata current_meta_{};

  /// the end key given from a user
  ScanKey end_key_{};

  /// a flag for indicating whether scan has finished
  bool is_right_end_{};
};

}  // namespace component
}  // namespace dbgroup::index::bztree

#endif  // BZTREE_COMPONENT_RECORD_ITERATOR_HPP
