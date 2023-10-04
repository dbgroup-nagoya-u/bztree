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

// C++ standard libraries
#include <array>
#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <utility>

// external sources
#include "memory/epoch_based_gc.hpp"

// local sources
#include "bztree/component/node.hpp"
#include "bztree/utility.hpp"

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
  using Page = component::Page;
  using Metadata = component::Metadata;
  using StatusWord = component::StatusWord;
  using Node_t = component::Node<Key, Compare>;
  using NodeRC = component::NodeRC;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<Page>;
  using NodeStack = std::vector<std::pair<Node_t *, size_t>>;
  using MwCASDescriptor = component::MwCASDescriptor;
  using KeyExistence = component::KeyExistence;
  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;

  template <class Entry>
  using BulkIter = typename std::vector<Entry>::const_iterator;
  using NodeEntry = std::tuple<Key, Node_t *, size_t>;
  using BulkResult = std::pair<size_t, std::vector<NodeEntry>>;
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

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    ~RecordIterator() = default;

    /*##################################################################################
     * Public operators for iterators
     *################################################################################*/

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
    ScanKey end_key_{};

    /// a flag for indicating whether scan has finished
    bool is_right_end_{};
  };

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new BzTree object.
   *
   * @param gc_interval_microsec GC internal [us] (default: 10ms).
   * @param gc_thread_num the number of GC threads (default: 1).
   */
  explicit BzTree(  //
      const size_t gc_interval_microsec = kDefaultGCTime,
      const size_t gc_thread_num = kDefaultGCThreadNum)
      : gc_{gc_interval_microsec, gc_thread_num}
  {
    // create an initial root node
    auto *leaf = CreateNewNode<Payload>();
    root_.store(leaf, std::memory_order_release);

    gc_.StartGC();
  }

  BzTree(const BzTree &) = delete;
  BzTree(BzTree &&) = delete;

  BzTree &operator=(const BzTree &) = delete;
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
   * @param key_len the length of the target key.
   * @retval the payload of a given key wrapped with std::optional if it is in this tree.
   * @retval std::nullopt otherwise.
   */
  auto
  Read(  //
      const Key &key,
      [[maybe_unused]] const size_t key_len = sizeof(Key))  //
      -> std::optional<Payload>
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    const auto *node = SearchLeafNode(key);

    Payload payload{};
    const auto rc = node->Read(key, payload);
    if (rc == NodeRC::kSuccess) return payload;
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
      const ScanKey &begin_key = std::nullopt,
      const ScanKey &end_key = std::nullopt)  //
      -> RecordIterator
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    thread_local std::unique_ptr<Node_t> page{CreateNewNode<Payload>()};
    page->InitForScanning();

    // sort records in a target node
    size_t begin_pos = 0;
    if (begin_key) {
      const auto &[b_key, b_key_len, b_closed] = *begin_key;
      const auto *node = SearchLeafNode(b_key);
      page->template Consolidate<Payload>(node);

      // check the begin position for scanning
      Metadata meta{};
      auto [rc, pos] = page->SearchSortedRecord(b_key, meta);
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
   * This function always overwrites a payload and can be optimized for that purpose;
   * the procedure may omit the key uniqueness check.
   *
   * @param key a target key.
   * @param payload a target payload.
   * @param key_len the length of the target key.
   * @param pay_len the length of the target payload.
   * @return kSuccess.
   */
  auto
  Write(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key),
      [[maybe_unused]] const size_t pay_len = sizeof(Payload))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    while (true) {
      auto *node = SearchLeafNode(key);
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
   * This function performs a uniqueness check on its processing. If the given key does
   * not exist in this tree, this function inserts a target payload into this tree. If
   * the given key exists in this tree, this function does nothing and returns kKeyExist.
   *
   * @param key a target key.
   * @param payload a target payload.
   * @param key_len the length of the target key.
   * @param pay_len the length of the target payload.
   * @retval kSuccess if inserted.
   * @retval kKeyExist otherwise.
   */
  auto
  Insert(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key),
      [[maybe_unused]] const size_t pay_len = sizeof(Payload))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    while (true) {
      auto *node = SearchLeafNode(key);
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
   * This function performs a uniqueness check on its processing. If the given key
   * exists in this tree, this function updates the corresponding payload. If the given
   * key does not exist in this tree, this function does nothing and returns
   * kKeyNotExist.
   *
   * @param key a target key.
   * @param payload a target payload.
   * @param key_len the length of the target key.
   * @param pay_len the length of the target payload.
   * @retval kSuccess if updated.
   * @retval kKeyNotExist otherwise.
   */
  auto
  Update(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key),
      [[maybe_unused]] const size_t pay_len = sizeof(Payload))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    while (true) {
      auto *node = SearchLeafNode(key);
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
   * This function performs a uniqueness check on its processing. If the given key
   * exists in this tree, this function deletes it. If the given key does not exist in
   * this tree, this function does nothing and returns kKeyNotExist.
   *
   * @param key a target key.
   * @param key_len the length of the target key.
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
      auto *node = SearchLeafNode(key);
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
   * This function loads the given entries into this index, assuming that the entries
   * are given as a vector of key/payload pairs (or the tuples key/payload/key-length
   * for variable-length keys). Note that keys in records are assumed to be unique and
   * sorted.
   *
   * @tparam Entry a container of a key/payload pair.
   * @param entries the vector of entries to be bulkloaded.
   * @param thread_num the number of threads used for bulk loading.
   * @return kSuccess.
   */
  template <class Entry>
  auto
  Bulkload(  //
      const std::vector<Entry> &entries,
      const size_t thread_num = 1)  //
      -> ReturnCode
  {
    if (entries.empty()) return ReturnCode::kSuccess;

    std::vector<NodeEntry> nodes{};
    auto &&iter = entries.cbegin();
    const auto rec_num = entries.size();
    if (thread_num <= 1 || rec_num < thread_num) {
      // bulkloading with a single thread
      nodes = BulkloadWithSingleThread<Entry>(iter, rec_num).second;
    } else {
      // bulkloading with multi-threads
      std::vector<BulkFuture> futures{};
      futures.reserve(thread_num);

      // a lambda function for bulkloading with multi-threads
      auto loader = [&](BulkPromise p, BulkIter<Entry> iter, size_t n) {
        p.set_value(BulkloadWithSingleThread<Entry>(iter, n));
      };

      // create threads to construct partial BzTrees
      for (size_t i = 0; i < thread_num; ++i) {
        // create a partial BzTree
        BulkPromise p{};
        futures.emplace_back(p.get_future());
        const size_t n = (rec_num + i) / thread_num;
        std::thread{loader, std::move(p), iter, n}.detach();

        // forward the iterator to the next begin position
        iter += n;
      }

      // wait for the worker threads to create partial trees
      std::vector<BulkResult> partial_trees{};
      partial_trees.reserve(thread_num);
      size_t height = 1;
      for (auto &&future : futures) {
        partial_trees.emplace_back(future.get());
        const auto partial_height = partial_trees.back().first;
        height = (partial_height > height) ? partial_height : height;
      }

      // align the height of partial trees
      nodes.reserve(kInnerNodeCap * thread_num);
      Node_t *prev_node = nullptr;
      for (auto &&[p_height, p_nodes] : partial_trees) {
        while (p_height < height) {  // NOLINT
          p_nodes = ConstructSingleLayer<NodeEntry>(p_nodes.cbegin(), p_nodes.size());
          ++p_height;
        }
        nodes.insert(nodes.end(), p_nodes.begin(), p_nodes.end());

        // set high_key of partial tree
        if (prev_node != nullptr) {
          auto *cur_node = std::get<1>(p_nodes.front());
          Node_t::template SetHighKeyOfPartialTree<Payload>(prev_node, cur_node);
        }
        prev_node = std::get<1>(p_nodes.back());
      }
    }

    // create upper layers until a root node is created
    while (nodes.size() > 1) {
      nodes = ConstructSingleLayer<NodeEntry>(nodes.cbegin(), nodes.size());
    }
    auto *new_root = std::get<1>(nodes.front());
    Node_t::RemoveLeftmostKeys(new_root);

    // set a new root
    auto *old_root = root_.exchange(new_root, std::memory_order_release);
    gc_.AddGarbage<Page>(old_root);

    return ReturnCode::kSuccess;
  }

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  /**
   * @brief Collect statistical data of this tree.
   *
   * @retval 1st: the number of nodes.
   * @retval 2nd: the actual usage in bytes.
   * @retval 3rd: the virtual usage (i.e., reserved memory) in bytes.
   */
  auto
  CollectStatisticalData()  //
      -> std::vector<std::tuple<size_t, size_t, size_t>>
  {
    std::vector<std::tuple<size_t, size_t, size_t>> stat_data{};
    auto *node = root_.load(std::memory_order_acquire);

    CollectStatisticalData(node, 0, stat_data);

    return stat_data;
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// Header length in bytes.
  static constexpr size_t kHeaderLen = component::kHeaderLen;

  /// the maximum length of keys.
  static constexpr size_t kMaxKeyLen = (IsVarLenData<Key>()) ? kMaxVarDataSize : sizeof(Key);

  /// the length of record metadata.
  static constexpr size_t kMetaLen = sizeof(Metadata);

  /// the length of payloads.
  static constexpr size_t kPayLen = sizeof(Payload);

  /// the length of child pointers.
  static constexpr size_t kPtrLen = sizeof(Node_t *);

  /// the expected length of keys for bulkloading.
  static constexpr size_t kBulkKeyLen = (IsVarLenData<Key>()) ? kWordSize : sizeof(Key);

  /// the expected length of highest keys in leaf nodes for bulkloading.
  static constexpr size_t kMaxHKeyLen = component::Pad<Payload>(kMaxKeyLen);

  /// the expected length of records in leaf nodes for bulkloading.
  static constexpr size_t kLeafRecLen = component::Pad<Payload>(kBulkKeyLen + kPayLen) + kMetaLen;

  /// the expected capacity of leaf nodes for bulkloading.
  static constexpr size_t kLeafNodeCap =
      (kPageSize - kHeaderLen - kMaxHKeyLen - kMinFreeSpaceSize) / kLeafRecLen;

  /// the expected length of records in internal nodes for bulkloading.
  static constexpr size_t kInnerRecLen = component::Pad<Node_t *>(kBulkKeyLen + kPtrLen) + kMetaLen;

  /// the expected capacity of internal nodes for bulkloading.
  static constexpr size_t kInnerNodeCap = (kPageSize - kHeaderLen - kMaxHKeyLen) / kInnerRecLen;

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
    constexpr auto kIsInner = static_cast<uint64_t>(std::is_same_v<T, Node_t *>);

    auto *page = gc_.template GetPageIfPossible<Page>();
    if (page == nullptr) {
      page = ::dbgroup::memory::Allocate<Page>();
      memset(page, 0, kPageSize);
    }
    return new (page) Node_t{kIsInner, 0};
  }

  /**
   * @return a current root node.
   */
  [[nodiscard]] auto
  GetRoot() const  //
      -> Node_t *
  {
    auto *root = MwCASDescriptor::Read<Node_t *>(&root_, std::memory_order_acquire);

    return root;
  }

  /**
   * @brief Search a leaf node with a specified key.
   *
   * @param key a target key.
   * @return a leaf node that may contain a target key.
   */
  [[nodiscard]] auto
  SearchLeafNode(const Key &key) const  //
      -> Node_t *
  {
    auto *current_node = GetRoot();
    while (!current_node->IsLeaf()) {
      const auto pos = current_node->Search(key);
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
    NodeStack trace{};
    size_t index = 0;
    auto *current_node = GetRoot();
    while (current_node != target_node && !current_node->IsLeaf()) {
      trace.emplace_back(current_node, index);
      index = current_node->Search(key);
      current_node = current_node->GetChild(index);
    }
    trace.emplace_back(current_node, index);

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

    ::dbgroup::memory::Release<Page>(node);
  }

  /**
   * @brief Collect statistical data recursively.
   *
   * @param node a target node.
   * @param level the current level in the tree.
   * @param stat_data an output statistical data.
   */
  static void
  CollectStatisticalData(  //
      Node_t *node,
      const size_t level,
      std::vector<std::tuple<size_t, size_t, size_t>> &stat_data)
  {
    // add an element for a new level
    if (stat_data.size() <= level) {
      stat_data.emplace_back(0, 0, 0);
    }

    // add statistical data of this node
    auto &[node_num, actual_usage, virtual_usage] = stat_data.at(level);
    ++node_num;
    actual_usage += node->GetNodeUsage();
    virtual_usage += kPageSize;

    // collect data recursively
    if (!node->IsLeaf()) {
      for (size_t i = 0; i < node->GetSortedCount(); ++i) {
        auto *child = node->GetChild(i);
        CollectStatisticalData(child, level + 1, stat_data);
      }
    }
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
    if (node->Freeze() != NodeRC::kSuccess) return;

    // create a consolidated node to calculate a correct node size
    auto *consol_node = CreateNewNode<Payload>();
    consol_node->template Consolidate<Payload>(node);
    gc_.AddGarbage<Page>(node);

    // check other SMOs are needed
    const auto stat = consol_node->GetStatusWord();
    if (stat.template NeedSplit<Key, Payload>()) return Split<Payload>(consol_node, key);
    if (stat.NeedMerge() && Merge<Payload>(consol_node, key, node)) return;

    // install the consolidated node
    auto &&trace = TraceTargetNode(key, node);
    InstallNewNode(trace, consol_node, key, node);
  }

  /**
   * @brief Split a target node.
   *
   * Note that this function may call a split function for internal nodes if needed.
   *
   * @tparam T a target payload class.
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

    NodeStack trace{};
    Node_t *old_parent = nullptr;
    size_t target_pos{};
    bool root_split = false;
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(key, node);
      if (trace.size() <= 1) {
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
    gc_.AddGarbage<Page>(node);
    if (!root_split) {
      gc_.AddGarbage<Page>(old_parent);
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
   * @tparam T a target payload class.
   * @param l_node a target node.
   * @param key a target key.
   * @retval true if merging succeeds
   * @retval false otherwise
   */
  template <class T>
  auto
  Merge(  //
      Node_t *l_node,
      const Key &key,      //
      Node_t *old_l_node)  //
      -> bool
  {
    const auto l_stat = l_node->GetStatusWord();

    /*----------------------------------------------------------------------------------
     * Phase 1: preparation
     *--------------------------------------------------------------------------------*/

    NodeStack trace{};
    Node_t *old_parent{};
    Node_t *r_node{};
    size_t target_pos{};
    while (true) {
      // trace and get the embedded index of a target node
      trace = TraceTargetNode(key, old_l_node);
      target_pos = trace.back().second;

      // check a parent node is live
      trace.pop_back();
      if (trace.empty()) return false;  // a root node cannot be merged
      old_parent = trace.back().first;
      if (target_pos == old_parent->GetSortedCount() - 1) return false;  // no mergeable node
      const auto p_stat = old_parent->GetStatusWord();
      if (p_stat.IsFrozen()) continue;

      // check a right sibling node is live and has sufficient capacity
      r_node = old_parent->GetChild(target_pos + 1);
      const auto r_stat = r_node->GetStatusWord();
      if (!r_stat.CanMergeWith(l_stat)) return false;  // there is no space for merging
      if (r_stat.IsFrozen()) continue;

      // pre-freezing of SMO targets
      MwCASDescriptor desc{};
      old_parent->SetStatusForMwCAS(desc, p_stat, p_stat.Freeze());
      r_node->SetStatusForMwCAS(desc, r_stat, r_stat.Freeze());
      if (desc.MwCAS()) break;
    }

    /*----------------------------------------------------------------------------------
     * Phase 2: installation
     *--------------------------------------------------------------------------------*/

    // create new nodes
    auto *merged_node = CreateNewNode<T>();
    merged_node->template Merge<T>(l_node, r_node);
    auto *new_parent = CreateNewNode<Node_t *>();
    auto recurse_merge = new_parent->InitAsMergeParent(old_parent, merged_node, target_pos);
    if (trace.size() <= 1 && new_parent->GetSortedCount() == 1) {
      // the new root node has only one child, use the merged child as a new root
      gc_.AddGarbage<Page>(new_parent);
      new_parent = merged_node;
    }

    // install new nodes to the index and register garbages
    InstallNewNode(trace, new_parent, key, old_parent);
    gc_.AddGarbage<Page>(old_parent);
    gc_.AddGarbage<Page>(l_node);
    gc_.AddGarbage<Page>(r_node);

    // merge the new parent node if needed
    if (recurse_merge && !Merge<Node_t *>(new_parent, key, new_parent)) {
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
   * @tparam Entry a container of a key/payload pair.
   * @param iter the begin position of target records.
   * @param n the number of entries to be bulkloaded.
   * @retval 1st: the height of a constructed tree.
   * @retval 2nd: constructed nodes in the top layer.
   */
  template <class Entry>
  auto
  BulkloadWithSingleThread(  //
      BulkIter<Entry> &iter,
      const size_t n)  //
      -> BulkResult
  {
    // construct a data layer (leaf nodes)
    auto &&nodes = ConstructSingleLayer<Entry>(iter, n);

    // construct index layers (inner nodes)
    size_t height = 1;
    for (auto n = nodes.size(); n > kInnerNodeCap; n = nodes.size(), ++height) {
      // continue until the number of inner nodes is sufficiently small
      nodes = ConstructSingleLayer<NodeEntry>(nodes.cbegin(), n);
    }

    return {height, std::move(nodes)};
  }

  /**
   * @brief Construct nodes based on given entries.
   *
   * @tparam Entry a pair/tuple class to be inserted.
   * @param iter the begin position of target records.
   * @param n the number of entries to be bulkloaded.
   * @return constructed nodes.
   */
  template <class Entry>
  auto
  ConstructSingleLayer(  //
      BulkIter<Entry> iter,
      const size_t n)  //
      -> std::vector<NodeEntry>
  {
    using T = std::tuple_element_t<1, Entry>;
    constexpr auto kIsInner = std::is_same_v<T, Node_t *>;

    // reserve space for nodes in the upper layer
    std::vector<NodeEntry> nodes{};
    nodes.reserve((n / (kIsInner ? kInnerNodeCap : kLeafNodeCap)) + 1);

    // construct nodes over the current level
    const auto &iter_end = iter + n;
    while (iter < iter_end) {
      auto *node = CreateNewNode<T>();
      node->template Bulkload<Entry>(iter, iter_end, nodes);
    }

    return nodes;
  }

  /*####################################################################################
   * Static assertions
   *##################################################################################*/

  /**
   * @retval true if a target key class is trivially copyable.
   * @retval false otherwise.
   */
  [[nodiscard]] static constexpr auto
  KeyIsTriviallyCopyable()  //
      -> bool
  {
    if constexpr (IsVarLenData<Key>()) {
      // check a base type is trivially copyable
      return std::is_trivially_copyable_v<std::remove_pointer_t<Key>>;
    } else {
      // check a given key type is trivially copyable
      return std::is_trivially_copyable_v<Key>;
    }
  }

  // target keys must be trivially copyable.
  static_assert(KeyIsTriviallyCopyable());

  // target payloads must be trivially copyable.
  static_assert(std::is_trivially_copyable_v<Payload>);

  // node pages have sufficient capacity for records.
  static_assert(kMaxKeyLen + kPayLen <= kPageSize / 4);

  // the bottom of a page must be aligned for in-place updating.
  static_assert(kPageSize % kWordSize == 0);

  // The member variables in Node class act as a node header.
  static_assert(sizeof(Node_t) == kHeaderLen);

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a root node of BzTree
  std::atomic<Node_t *> root_{nullptr};

  /// garbage collector
  NodeGC_t gc_{};
};

}  // namespace dbgroup::index::bztree

#endif  // BZTREE_BZTREE_HPP
