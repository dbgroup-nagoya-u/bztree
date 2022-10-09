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

#ifndef BZTREE_BZTREE_HPP
#define BZTREE_BZTREE_HPP

#include <array>
#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <utility>

#include "component/node.hpp"
#include "memory/epoch_based_gc.hpp"
#include "utility.hpp"

namespace dbgroup::index::bztree
{
/**
 * @brief A class to represent BzTree.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 */
template <class Key, class Payload, class Compare = std::less<Key>>
class BzTree
{
  using Metadata = component::Metadata;
  using StatusWord = component::StatusWord;
  using Node_t = component::Node<Key, Compare>;
  using NodeRC = component::NodeRC;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<Node_t>;
  using NodeStack = std::vector<std::pair<Node_t *, size_t>>;
  using MwCASDescriptor = component::MwCASDescriptor;
  using KeyExistence = component::KeyExistence;
  template <class Entry>
  using BulkIter = typename std::vector<Entry>::const_iterator;
  using BulkResult = std::pair<size_t, std::vector<Node_t *>>;
  using BulkPromise = std::promise<BulkResult>;
  using BulkFuture = std::future<BulkResult>;

 public:
  /*####################################################################################
   * Public classes
   *##################################################################################*/

  /**
   * @brief A class to represent a iterator for scan results.
   *
   * @tparam Key a target key class
   * @tparam Payload a target payload class
   * @tparam Compare a key-comparator class
   */
  class RecordIterator
  {
    using BzTree_t = BzTree<Key, Payload, Compare>;

   public:
    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    RecordIterator(  //
        BzTree_t *bztree,
        Node_t *node,
        const size_t begin_pos,
        const size_t end_pos,
        const std::optional<std::pair<const Key &, bool>> end_key,
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

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    ~RecordIterator() = default;

    /*##################################################################################
     * Public operators for iterators
     *################################################################################*/

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

    /*##################################################################################
     * Public getters/setters
     *################################################################################*/

    /**
     * @brief Check if there are any records left.
     *
     * function may call a scan function internally to get a next leaf node.
     *
     * @retval true if there are any records or next node left.
     * @retval false if there are no records and node left.
     */
    bool
    HasNext()
    {
      while (true) {
        if (current_pos_ < record_count_) return true;  // records remain in this node
        if (is_right_end_) return false;                // this node is the end of range-scan

        // update this iterator with the next scan results
        const auto &next_key = node_->GetHighKey();
        *this = bztree_->Scan(std::make_pair(next_key, false), end_key_);

        if constexpr (IsVariableLengthData<Key>()) {
          // release a dynamically allocated key
          delete next_key;
        }
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
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

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
    std::optional<std::pair<const Key &, bool>> end_key_{};

    /// a flag for indicating whether scan has finished
    bool is_right_end_{};
  };

  /*####################################################################################
   * Public constants
   *##################################################################################*/

  static constexpr size_t kDefaultGCTime = 100000;

  static constexpr size_t kDefaultGCThreadNum = 1;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new BzTree object.
   *
   * @param gc_interval_microsec GC internal [us]
   */
  explicit BzTree(  //
      const size_t gc_interval_microsec = 10000,
      const size_t gc_thread_num = 1)
      : gc_{gc_interval_microsec, gc_thread_num, true}
  {
    // create an initial root node
    auto *leaf = CreateNewNode<Payload>();
    root_.store(leaf, std::memory_order_release);
  }

  BzTree(const BzTree &) = delete;
  BzTree &operator=(const BzTree &) = delete;
  BzTree(BzTree &&) = delete;
  BzTree &operator=(BzTree &&) = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the BzTree object.
   *
   */
  ~BzTree()
  {
    gc_.StopGC();
    DeleteChildren(GetRoot());
  }

  /*####################################################################################
   * Public read APIs
   *##################################################################################*/

  /**
   * @brief Read the payload corresponding to a given key if it exists.
   *
   * @param key a target key.
   * @retval the payload of a given key wrapped with std::optional if it is in this tree.
   * @retval std::nullopt otherwise.
   */
  auto
  Read(const Key &key)  //
      -> std::optional<Payload>
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    const auto *node = SearchLeafNode(key, true);

    Payload payload{};
    const auto rc = node->Read(key, payload);
    if (rc == NodeRC::kSuccess) return std::make_optional(payload);
    return std::nullopt;
  }

  /**
   * @brief Perform a range scan with given keys.
   *
   * @param begin_key a pair of a begin key and its openness (true=closed).
   * @param end_key a pair of an end key and its openness (true=closed).
   * @return an iterator to access scanned records.
   */
  auto
  Scan(  //
      const std::optional<std::pair<const Key &, bool>> &begin_key = std::nullopt,
      const std::optional<std::pair<const Key &, bool>> &end_key = std::nullopt)  //
      -> RecordIterator
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    thread_local std::unique_ptr<Node_t> page{CreateNewNode<Payload>()};
    page->InitForScanning();

    // sort records in a target node
    size_t begin_pos = 0;
    if (begin_key) {
      const auto &[b_key, b_closed] = *begin_key;
      const auto *node = SearchLeafNode(b_key, b_closed);
      page->template Consolidate<Payload>(node);

      // check the begin position for scanning
      auto [rc, pos] = page->SearchSortedRecord(b_key);
      begin_pos = (rc == KeyExistence::kNotExist || b_closed) ? pos : pos + 1;
    } else {
      const auto *node = SearchLeftEdgeLeaf();
      page->template Consolidate<Payload>(node);
    }

    // check the end position of scanning
    const auto [is_end, end_pos] = page->SearchEndPositionFor(end_key);

    return RecordIterator{this, page.get(), begin_pos, end_pos, end_key, is_end};
  }

  /*####################################################################################
   * Public write APIs
   *##################################################################################*/

  /**
   * @brief Write (i.e., put) a given key/payload pair.
   *
   * If a given key does not exist in this tree, this function performs an insert
   * operation. If a given key has been already inserted, this function perfroms an
   * update operation. Thus, this function always returns kSuccess as a return code.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_len the length of a target key.
   * @return kSuccess.
   */
  auto
  Write(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    while (true) {
      auto *node = SearchLeafNode(key, true);
      const auto rc = node->Write(key, key_len, payload);

      switch (rc) {
        case NodeRC::kSuccess:
          return ReturnCode::kSuccess;
        case NodeRC::kNeedConsolidation:
          Consolidate(node, key);
        default:
          break;
      }
    }
  }

  /**
   * @brief Insert a given key/payload pair.
   *
   * This function performs a uniqueness check in its processing. If a given key does
   * not exist in this tree, this function inserts a target payload to this tree. If
   * there is a given key in this tree, this function does nothing and returns kKeyExist
   * as a return code.
   *
   * @param key a target key to be inserted.
   * @param payload a target payload to be inserted.
   * @param key_len the length of a target key.
   * @retval.kSuccess if inserted.
   * @retval kKeyExist otherwise.
   */
  auto
  Insert(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    while (true) {
      auto *node = SearchLeafNode(key, true);
      const auto rc = node->Insert(key, key_len, payload);

      switch (rc) {
        case NodeRC::kSuccess:
          return ReturnCode::kSuccess;
        case NodeRC::kKeyExist:
          return ReturnCode::kKeyExist;
        case NodeRC::kNeedConsolidation:
          Consolidate(node, key);
        default:
          break;
      }
    }
  }

  /**
   * @brief Update the record corresponding to a given key with a given payload.
   *
   * This function performs a uniqueness check in its processing. If there is a given
   * key in this tree, this function updates the corresponding record. If a given key
   * does not exist in this tree, this function does nothing and returns kKeyNotExist as
   * a return code.
   *
   * @param key a target key to be updated.
   * @param payload a payload for updating.
   * @param key_len the length of a target key.
   * @retval kSuccess if updated.
   * @retval kKeyNotExist otherwise.
   */
  auto
  Update(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    while (true) {
      auto *node = SearchLeafNode(key, true);
      const auto rc = node->Update(key, key_len, payload);

      switch (rc) {
        case NodeRC::kSuccess:
          return ReturnCode::kSuccess;
        case NodeRC::kKeyNotExist:
          return ReturnCode::kKeyNotExist;
        case NodeRC::kNeedConsolidation:
          Consolidate(node, key);
        default:
          break;
      }
    }
  }

  /**
   * @brief Delete the record corresponding to a given key from this tree.
   *
   * This function performs a uniqueness check in its processing. If there is a given
   * key in this tree, this function deletes it. If a given key does not exist in this
   * tree, this function does nothing and returns kKeyNotExist as a return code.
   *
   * @param key a target key to be deleted.
   * @param key_len the length of a target key.
   * @retval kSuccess if deleted.
   * @retval kKeyNotExist otherwise.
   */
  auto
  Delete(  //
      const Key &key,
      const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    while (true) {
      auto *node = SearchLeafNode(key, true);
      const auto rc = node->template Delete<Payload>(key, key_len);

      switch (rc) {
        case NodeRC::kSuccess:
          return ReturnCode::kSuccess;
        case NodeRC::kKeyNotExist:
          return ReturnCode::kKeyNotExist;
        case NodeRC::kNeedConsolidation:
          Consolidate(node, key);
        default:
          break;
      }
    }
  }

  /*####################################################################################
   * Public bulkload API
   *##################################################################################*/

  /**
   * @brief Bulkload specified kay/payload pairs.
   *
   * This function bulkloads given entries into this index. The entries are assumed to
   * be given as a vector of pairs of Key and Payload (or key/payload/key-length for
   * variable-length keys). Note that keys in records are assumed to be unique and
   * sorted.
   *
   * @param entries vector of entries to be bulkloaded.
   * @param thread_num the number of threads to perform bulkloading.
   * @return kSuccess.
   */
  template <class Entry>
  auto
  Bulkload(  //
      const std::vector<Entry> &entries,
      const size_t thread_num = 1)  //
      -> ReturnCode
  {
    assert(thread_num > 0);
    assert(entries.size() >= thread_num);

    if (entries.empty()) return ReturnCode::kSuccess;

    std::vector<Node_t *> nodes{};
    auto &&iter = entries.cbegin();
    if (thread_num == 1) {
      // bulkloading with a single thread
      nodes = BulkloadWithSingleThread<Entry>(iter, entries.size()).second;
    } else {
      // bulkloading with multi-threads
      std::vector<BulkFuture> futures{};
      futures.reserve(thread_num);

      // a lambda function for bulkloading with multi-threads
      auto loader = [&](BulkPromise p, BulkIter<Entry> iter, size_t n, bool is_rightmost) {
        p.set_value(BulkloadWithSingleThread<Entry>(iter, n, is_rightmost));
      };

      // create threads to construct partial BzTrees
      const auto rec_num = entries.size();
      const auto rightmost_id = thread_num - 1;
      for (size_t i = 0; i < thread_num; ++i) {
        // create a partial BzTree
        BulkPromise p{};
        futures.emplace_back(p.get_future());
        const size_t n = (rec_num + i) / thread_num;
        std::thread{loader, std::move(p), iter, n, i == rightmost_id}.detach();

        // forward the iterator to the next begin position
        iter += n;
      }

      // wait for the worker threads to create partial trees
      std::vector<std::pair<size_t, std::vector<Node_t *>>> partial_trees{};
      partial_trees.reserve(thread_num);
      size_t height = 1;
      for (auto &&future : futures) {
        partial_trees.emplace_back(future.get());
        const auto partial_height = partial_trees.back().first;
        height = (partial_height > height) ? partial_height : height;
      }

      // align the height of partial trees
      nodes.reserve(kInnerNodeCap * thread_num);
      for (auto &&[p_height, p_nodes] : partial_trees) {
        while (p_height < height) {  // NOLINT
          ConstructUpperLayer(p_nodes);
          ++p_height;
        }
        nodes.insert(nodes.end(), p_nodes.begin(), p_nodes.end());
      }
    }

    // create upper layers until a root node is created
    while (nodes.size() > 1) {
      ConstructUpperLayer(nodes);
    }

    // set a new root
    auto *old_root = root_.exchange(nodes.front(), std::memory_order_release);
    gc_.AddGarbage(old_root);

    return ReturnCode::kSuccess;
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// the expected length of keys for bulkloading.
  static constexpr size_t kBulkKeyLen = (IsVariableLengthData<Key>()) ? kWordSize : sizeof(Key);

  /// the expected length of highest keys in leaf nodes for bulkloading.
  static constexpr size_t kLeafHKeyLen = component::Align<Key, Payload>(kBulkKeyLen).first;

  /// the expected length of records in leaf nodes for bulkloading.
  static constexpr size_t kLeafRecLen = component::Align<Key, Payload>(kBulkKeyLen).second;

  /// the expected capacity of leaf nodes for bulkloading.
  static constexpr size_t kLeafNodeCap = (kPageSize - kHeaderLen - kLeafHKeyLen - kMinFreeSpaceSize)
                                         / (kLeafRecLen + sizeof(Metadata));

  /// the expected length of highest keys in internal nodes for bulkloading.
  static constexpr size_t kInnerHKeyLen = component::Align<Key, Node_t *>(kBulkKeyLen).first;

  /// the expected length of records in internal nodes for bulkloading.
  static constexpr size_t kInnerRecLen = component::Align<Key, Node_t *>(kBulkKeyLen).second;

  /// the expected capacity of internal nodes for bulkloading.
  static constexpr size_t kInnerNodeCap =
      (kPageSize - kHeaderLen - kInnerHKeyLen) / (kInnerRecLen + sizeof(Metadata));

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  /**
   * @brief Create a New Node accordint to a given template paramter.
   *
   * @tparam T a template paramter for indicating whether a new node is a leaf.
   * @retval an empty leaf node if Payload is given as a template.
   * @retval an empty internal node otherwise.
   */
  template <class T>
  [[nodiscard]] auto
  CreateNewNode()  //
      -> Node_t *
  {
    constexpr bool kIsLeaf = std::is_same_v<T, Payload>;

    auto *page = gc_.template GetPageIfPossible<Node_t>();
    return (page == nullptr) ? new Node_t{kIsLeaf, 0} : new (page) Node_t{kIsLeaf, 0};
  }

  /**
   * @return a current root node.
   */
  [[nodiscard]] auto
  GetRoot() const  //
      -> Node_t *
  {
    auto *root = MwCASDescriptor::Read<Node_t *>(&root_);
    std::atomic_thread_fence(std::memory_order_acquire);

    return root;
  }

  /**
   * @brief Search a leaf node with a specified key.
   *
   * @param key a target key.
   * @param range_is_closed a flag to indicate whether a key is included.
   * @return a leaf node that may contain a target key.
   */
  [[nodiscard]] auto
  SearchLeafNode(  //
      const Key &key,
      const bool range_is_closed) const  //
      -> Node_t *
  {
    auto *current_node = GetRoot();
    while (!current_node->IsLeaf()) {
      const auto pos = current_node->Search(key, range_is_closed);
      current_node = current_node->GetChild(pos);
    }

    return current_node;
  }

  /**
   * @return a leaf node on the far left.
   */
  [[nodiscard]] auto
  SearchLeftEdgeLeaf() const  //
      -> Node_t *
  {
    auto *current_node = GetRoot();
    while (!current_node->IsLeaf()) {
      current_node = current_node->GetChild(0);
    }

    return current_node;
  }

  /**
   * @brief Trace a target node and extract intermidiate nodes.
   *
   * Note that traced nodes may not have a target node because concurrent SMOs may
   * remove it.
   *
   * @param key a target key.
   * @param target_node a target node.
   * @return a stack of nodes.
   */
  auto
  TraceTargetNode(  //
      const Key &key,
      const Node_t *target_node) const  //
      -> NodeStack
  {
    // trace nodes to a target internal node
    NodeStack trace;
    size_t index = 0;
    auto *current_node = GetRoot();
    while (current_node != target_node && !current_node->IsLeaf()) {
      trace.emplace_back(current_node, index);
      index = current_node->Search(key, true);
      current_node = current_node->GetChild(index);
    }
    if (current_node == target_node) {
      trace.emplace_back(current_node, index);
    } else {
      trace.clear();
    }

    return trace;
  }

  /**
   * @brief Delete child nodes recursively.
   *
   * Note that this function assumes that there are no other threads in operation.
   *
   * @param node a target node.
   */
  static void
  DeleteChildren(Node_t *node)
  {
    if (!node->IsLeaf()) {
      // delete children nodes recursively
      for (size_t i = 0; i < node->GetSortedCount(); ++i) {
        auto *child_node = node->GetChild(i);
        DeleteChildren(child_node);
      }
    }

    delete node;
  }

  /*####################################################################################
   * Internal structure modification functoins
   *##################################################################################*/

  /**
   * @brief Consolidate a target leaf node.
   *
   * Note that this function may call split/merge functions if needed.
   *
   * @param node a target leaf node.
   * @param key a target key.
   */
  void
  Consolidate(  //
      Node_t *node,
      const Key &key)
  {
    // freeze a target node and perform consolidation
    // if (node->Freeze() != NodeRC::kSuccess) return;
    node->Freeze();

    // create a consolidated node to calculate a correct node size
    auto *consol_node = CreateNewNode<Payload>();
    consol_node->template Consolidate<Payload>(node);
    gc_.AddGarbage(node);

    // check other SMOs are needed
    const auto stat = consol_node->GetStatusWord();
    if (stat.template NeedSplit<Key, Payload>()) return Split<Payload>(consol_node, key);
    if (stat.NeedMerge() && Merge<Payload>(consol_node, key)) return;

    // install the consolidated node
    auto &&trace = TraceTargetNode(key, node);
    if (trace.empty()) return;
    InstallNewNode(trace, consol_node, key, node);
  }

  /**
   * @brief Split a target node.
   *
   * Note that this function may call a split function for internal nodes if needed.
   *
   * @param node a target node.
   * @param key a target key.
   */
  template <class T>
  void
  Split(  //
      Node_t *node,
      const Key &key)
  {
    /*----------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------*/

    NodeStack trace;
    Node_t *old_parent = nullptr;
    size_t target_pos{};
    bool root_split = false;
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(key, node);
      if (trace.empty()) {
        return;
      } else if (trace.size() == 1) {
        root_split = true;
        break;
      }
      target_pos = trace.back().second;
      trace.pop_back();

      // pre-freezing of SMO targets
      old_parent = trace.back().first;
      if (old_parent->Freeze() == NodeRC::kSuccess) break;
    }

    /*----------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------*/

    // create split nodes and its parent node
    auto *l_node = CreateNewNode<T>();
    auto *r_node = CreateNewNode<T>();
    node->template Split<T>(l_node, r_node);

    // create a new root/parent node
    bool recurse_split = false;
    auto *new_parent = CreateNewNode<Node_t *>();
    if (root_split) {
      new_parent->InitAsRoot(l_node, r_node);
    } else {
      recurse_split = new_parent->InitAsSplitParent(old_parent, l_node, r_node, target_pos);
    }

    // install new nodes to the index and register garbages
    InstallNewNode(trace, new_parent, key, old_parent);
    gc_.AddGarbage(node);
    if (!root_split) {
      gc_.AddGarbage(old_parent);
    }

    // split the new parent node if needed
    if (recurse_split) {
      Split<Node_t *>(new_parent, key);
    }
  }

  /**
   * @brief Perform left-merge for a target node.
   *
   * Note that this function may call itself recursively if needed.
   *
   * @param right_node a target node.
   * @param key a target key.
   * @retval true if merging succeeds
   * @retval false otherwise
   */
  template <class T>
  auto
  Merge(  //
      Node_t *right_node,
      const Key &key)  //
      -> bool
  {
    const auto r_stat = right_node->GetStatusWord();

    /*----------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------*/

    NodeStack trace;
    Node_t *old_parent{};
    Node_t *left_node{};
    size_t target_pos{};
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(key, right_node);
      if (trace.empty()) return false;
      target_pos = trace.back().second;
      if (target_pos <= 0) return false;  // there is no mergeable node

      // check a parent node is live
      trace.pop_back();
      old_parent = trace.back().first;
      const auto p_status = old_parent->GetStatusWord();
      if (p_status.IsFrozen()) continue;

      // check a right sibling node is live and has sufficent capacity
      left_node = old_parent->GetChild(target_pos - 1);
      const auto l_stat = left_node->GetStatusWord();
      if (!l_stat.CanMergeWith(r_stat)) return false;
      if (l_stat.IsFrozen()) continue;

      // pre-freezing of SMO targets
      MwCASDescriptor desc{};
      old_parent->SetStatusForMwCAS(desc, p_status, p_status.Freeze());
      left_node->SetStatusForMwCAS(desc, l_stat, l_stat.Freeze());
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------*/

    // create new nodes
    auto *merged_node = CreateNewNode<T>();
    merged_node->template Merge<T>(left_node, right_node);
    auto *new_parent = CreateNewNode<Node_t *>();
    bool recurse_merge = new_parent->InitAsMergeParent(old_parent, merged_node, target_pos - 1);
    if (trace.size() <= 1 && new_parent->GetSortedCount() == 1) {
      // the new root node has only one child, use the merged child as a new root
      gc_.AddGarbage(new_parent);
      new_parent = merged_node;
    }

    // install new nodes to the index and register garbages
    InstallNewNode(trace, new_parent, key, old_parent);
    gc_.AddGarbage(old_parent);
    gc_.AddGarbage(left_node);
    gc_.AddGarbage(right_node);

    // merge the new parent node if needed
    if (recurse_merge && !Merge<Node_t *>(new_parent, key)) {
      // if the parent node cannot be merged, unfreeze it
      new_parent->Unfreeze();
    }

    return true;
  }

  /**
   * @brief Install a new node by using MwCAS.
   *
   * Note that this function may do nothing if a target node has been already modified.
   *
   * @param trace the stack of nodes up to a target node.
   * @param new_node a new node to be installed.
   * @param key a target key.
   * @param target_node an old node to be swapped.
   */
  void
  InstallNewNode(  //
      NodeStack &trace,
      Node_t *new_node,
      const Key &key,
      const Node_t *target_node)
  {
    if (trace.size() <= 1) {
      // root swapping
      root_.store(new_node, std::memory_order_release);
      return;
    }

    std::atomic_thread_fence(std::memory_order_release);
    while (true) {
      // prepare installing nodes
      auto [old_node, target_pos] = trace.back();
      trace.pop_back();
      auto *parent = trace.back().first;

      // check wether related nodes are frozen
      const auto parent_status = parent->GetStatusWordProtected();
      if (!parent_status.IsFrozen()) {
        // install a new internal node by MwCAS
        MwCASDescriptor desc{};
        parent->SetStatusForMwCAS(desc, parent_status, parent_status);
        parent->SetChildForMwCAS(desc, target_pos, old_node, new_node);
        if (desc.MwCAS()) return;
      }

      // traverse again to get a modified parent
      trace = TraceTargetNode(key, target_node);
      if (trace.empty()) return;
    }
  }

  /*####################################################################################
   * Internal bulkload utilities
   *##################################################################################*/

  /**
   * @brief Bulkload specified kay/payload pairs with a single thread.
   *
   * Note that this function does not create a root node. The main process must create a
   * root node by using the nodes constructed by this function.
   *
   * @param iter the begin position of target records.
   * @param n the number of entries to be bulkloaded.
   * @param is_rightmost a flag for indicating a constructed tree is rightmost one.
   * @retval 1st: the height of a constructed tree.
   * @retval 2nd: constructed nodes in the top layer.
   */
  template <class Entry>
  auto
  BulkloadWithSingleThread(  //
      BulkIter<Entry> &iter,
      const size_t n,
      const bool is_rightmost = true)  //
      -> BulkResult
  {
    // reserve space for nodes in the leaf layer
    std::vector<Node_t *> nodes{};
    nodes.reserve((n / kLeafNodeCap) + 1);

    // load records into leaf nodes
    const auto &iter_end = iter + n;
    while (iter < iter_end) {
      auto *node = CreateNewNode<Payload>();
      node->template Bulkload<Payload, Entry>(iter, iter_end, is_rightmost);
      nodes.emplace_back(node);
    }

    // construct index layers
    size_t height = 1;
    if (nodes.size() > 1) {
      // continue until the number of internal nodes is sufficiently small
      do {
        ++height;
      } while (ConstructUpperLayer(nodes));
    }

    return {height, std::move(nodes)};
  }

  /**
   * @brief Construct internal nodes based on given child nodes.
   *
   * @param child_nodes child nodes in a lower layer.
   * @retval true if constructed nodes cannot be contained in a single node.
   * @retval false otherwise.
   */
  auto
  ConstructUpperLayer(std::vector<Node_t *> &child_nodes)  //
      -> bool
  {
    // reserve space for nodes in the upper layer
    std::vector<Node_t *> nodes{};
    nodes.reserve((child_nodes.size() / kInnerNodeCap) + 1);

    // load child nodes into parent nodes
    auto &&iter = child_nodes.cbegin();
    const auto &iter_end = child_nodes.cend();
    while (iter < iter_end) {
      auto *node = CreateNewNode<Node_t *>();
      node->Bulkload(iter, iter_end);
      nodes.emplace_back(node);
    }

    child_nodes = std::move(nodes);
    return child_nodes.size() > kInnerNodeCap;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a root node of BzTree
  std::atomic<Node_t *> root_{nullptr};

  /// garbage collector
  NodeGC_t gc_{kDefaultGCTime, kDefaultGCThreadNum, true};
};

}  // namespace dbgroup::index::bztree

#endif  // BZTREE_BZTREE_HPP
