# BzTree

![example workflow name](https://github.com/dbgroup-nagoya-u/bztree/workflows/Ubuntu-20.04/badge.svg?branch=main)

This repository is an open source implementation of a [BzTree](http://www.vldb.org/pvldb/vol11/p553-arulraj.pdf)[1] for research use. The purpose of this implementation is to reproduce a BzTree and measure its performance. However, the concurrency controls proposed in the paper are insufficient, so we have modified them to guarantee consistent read/write operations. Moreover, some tuning parameters have been changed for convenience.

Note that although the original BzTree is proposed as an index for persistent memory (e.g., Intel Optane), we implemented our BzTree for volatile memory. Thus, there is no persistency support in this implementation.

> [1] J. Arulraj, J. Levandoski, U. F. Minhas, P.-A. Larson, "BzTree: A High-Performance Latch-free Range Index for Non-Volatile Memory,” PVLDB, Vol. 11, No. 5, pp. 553-565, 2018.

## Build

**Note**: this is a header only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

#### Tuning Parameters

- `BZTREE_PAGE_SIZE`: The byte length of each node page (default `8192`).
- `BZTREE_MAX_UNSORTED_REC_NUM`: Invoking consolidation if the number of unsorted records exceeds this threshold (default `64`).
- `BZTREE_MAX_DELETED_SPACE_SIZE`: Invoking consolidation if the size of deleted records exceeds this threshold (default `0.25 * BZTREE_PAGE_SIZE`).
- `BZTREE_MIN_FREE_SPACE_SIZE`: Invoking a split if the size of free space in a node exceeds this threshold (default `BZTREE_MAX_UNSORTED_REC_NUM * 24`).
- `BZTREE_MIN_SORTED_REC_NUM`: Invoking merging if the number of sorted records falls below this threshold (default `2 * BZTREE_MAX_UNSORTED_REC_NUM`).
- `BZTREE_MAX_MERGED_SIZE`: Canceling merging if the size of a merged node exceeds this threshold (default `BZTREE_PAGE_SIZE - (2 * BZTREE_MIN_FREE_SPACE_SIZE)`).

### Build Options for Unit Testing

- `BZTREE_BUILD_TESTS`: Building unit tests for this library if `ON` (default `OFF`).
- `BZTREE_TEST_THREAD_NUM`: The maximum number of threads to perform unit tests (default `8`).

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DBZTREE_BUILD_TESTS=ON ..
make -j
ctest -C Release
```

## Usage

### Linking by CMake

1. Download the files in any way you prefer (e.g., `git submodule`).

    ```bash
    cd <your_project_workspace>
    mkdir external
    git submodule add https://github.com/dbgroup-nagoya-u/bztree.git external/bztree
    ```

1. Add this library to your build in `CMakeLists.txt`.

    ```cmake
    add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/bztree")

    add_executable(
      <target_bin_name>
      [<source> ...]
    )
    target_link_libraries(
      <target_bin_name> PRIVATE
      bztree
    )
    ```

### Read/Write APIs

If you use fixed-length types as keys/values, you can use our BzTree by simply declaring it.

```cpp
#include <iostream>

#include "bztree/bztree.hpp"

using Key = uint64_t;
using Value = uint64_t;

using BzTree_t = ::dbgroup::index::bztree::BzTree<Key, Value>;
using ::dbgroup::index::bztree::ReturnCode;

int
main([[maybe_unused]] int argc, [[maybe_unused]] char** argv)
{
  // create a BzTree instance
  BzTree_t bztree{};

  // write key/value pairs
  bztree.Write(0UL, 0UL);

  // insert a duplicate key
  if (bztree.Insert(0UL, 1UL) != ReturnCode::kSuccess) {
    // inserting duplicate keys must fail, so we insert a new key
    bztree.Insert(1UL, 1UL);
  }

  // update a not inserted key
  if (bztree.Update(2UL, 2UL) != ReturnCode::kSuccess) {
    // updating non-existent keys must fail, so we update an inserted key
    bztree.Update(0UL, 2UL);
  }

  // delete a not inserted key
  if (bztree.Delete(2UL) != ReturnCode::kSuccess) {
    // deleting non-existent keys must fail, so we delete an inserted key
    bztree.Delete(1UL);
  }

  // read a deleted key
  auto [rc, value] = bztree.Read(1UL);
  if (rc != ReturnCode::kSuccess) {
    // reading deleted keys must fail, so we read an existent key
    std::tie(rc, value) = bztree.Read(0UL);

    std::cout << "Return code: " << rc << std::endl;
    std::cout << "Read value : " << value << std::endl;
  }

  return 0;
}
```

This code will output the following results.

```txt
Return code: 0
Read value : 2
```

### Range Scanning

A `Scan` function returns an iterator for scan results. We prepare a `HasNext` function and `*`/`++` operators to access scan results. If you give `nullptr` as a begin/end key, the index treats it as a negative/positive infinity value.

```cpp
#include <iostream>

#include "bztree/bztree.hpp"

using Key = uint64_t;
using Value = uint64_t;

using BzTree_t = ::dbgroup::index::bztree::BzTree<Key, Value>;
using ::dbgroup::index::bztree::ReturnCode;

int
main([[maybe_unused]] int argc, [[maybe_unused]] char** argv)
{
  // create a BzTree instance
  BzTree_t bztree{};

  // write key/value pairs
  for (uint64_t i = 0; i < 10; ++i) {
    bztree.Write(i, i);
  }

  // full scan
  uint64_t sum = 0;
  for (auto iter = bztree.Scan(); iter.HasNext(); ++iter) {
    auto [key, value] = *iter;
    // auto value = iter.GetPayload();  // you can get a value by itself
    sum += value;
  }
  std::cout << "Sum: " << sum << std::endl;

  // scan greater than: (3, infinity)
  sum = 0;
  uint64_t begin_key = 3;
  for (auto iter = bztree.Scan(&begin_key, false); iter.HasNext(); ++iter) {
    auto [key, value] = *iter;
    sum += value;
  }
  std::cout << "Sum: " << sum << std::endl;

  // scan less than or equal to: (-infinity, 7]
  sum = 0;
  uint64_t end_key = 7;
  for (auto iter = bztree.Scan(nullptr, false, &end_key, true); iter.HasNext(); ++iter) {
    auto [key, value] = *iter;
    sum += value;
  }
  std::cout << "Sum: " << sum << std::endl;

  // scan between: [3, 7)
  sum = 0;
  for (auto iter = bztree.Scan(&begin_key, true, &end_key, false); iter.HasNext(); ++iter) {
    auto [key, value] = *iter;
    sum += value;
  }
  std::cout << "Sum: " << sum << std::endl;

  return 0;
}
```

This code will output the following results.

```txt
Sum: 45
Sum: 39
Sum: 28
Sum: 18
```

### Bulk-Load for Single Thread

If you use bulk-load api, please prepare an EntryArray, which is a vector of tuples of key, payload, key length, and payload length.

```cpp
#include <iostream>

#include "bztree/bztree.hpp"

using Key = uint64_t;
using Value = uint64_t;

using BzTree_t = ::dbgroup::index::bztree::BzTree<Key, Value>;
using ::dbgroup::index::bztree::ReturnCode;

using Entry = ::dbgroup::index::bztree::BulkloadEntry<Key, Value>;
using EntryArray = std::vector<Entry>;

int
main([[maybe_unused]] int argc, [[maybe_unused]] char** argv)
{
  // create a BzTree instance
  BzTree_t bztree{};

  // create entries
  EntryArray entries;
  for (Key i = 0; i <= 4096; ++i) {
    entries.emplace_back(Entry{i, i});
  }

  // bulk-load entries
  bztree.BulkLoadForSingleThread(entries);

  // read bulk-loaded key and one key that has not been inserted
  for (Key i = 0; i <= 4096 + 1; ++i) {
    auto [rc, value] = bztree.Read(i);

    if (rc != ReturnCode::kSuccess) {
      std::cout << "Search key: " << i << std::endl;
      std::cout << "Return code: " << rc << std::endl;
    }
  }

  return 0;
}
```

This code will output the following results.

```txt
Search key: 4097
Return code: 1
```
### Multi-Threading

This library is a thread-safe implementation. You can call all the APIs (i.e., `Read`, `Scan`, `Write`, `Insert`, `Update`, and `Delete`) from multi-threads concurrently. Note that concurrent writes follow the last write win protocol, and so you need to some concurrency control methods (e.g., snapshot isolation) externally to guarantee the order of read/write operations.

```cpp
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "bztree/bztree.hpp"

using Key = uint64_t;
using Value = uint64_t;

using BzTree_t = ::dbgroup::index::bztree::BzTree<Key, Value>;
using ::dbgroup::index::bztree::ReturnCode;

uint64_t
Sum(const std::unique_ptr<BzTree_t>& bztree)
{
  uint64_t sum = 0;
  for (auto iter = bztree->Scan(); iter.HasNext(); ++iter) {
    sum += iter.GetPayload();
  }
  return sum;
}

int
main([[maybe_unused]] int argc, [[maybe_unused]] char** argv)
{
  // create a BzTree instance
  auto bztree = std::make_unique<BzTree_t>();

  // a lambda function for a multi-threading example
  auto f = [&](const uint64_t begin_id, const uint64_t end_id) {
    for (uint64_t i = begin_id; i < end_id; ++i) {
      bztree->Write(i, i);
    }
  };

  // write values by single threads
  std::vector<std::thread> threads;
  threads.emplace_back(f, 0, 4e6);
  for (auto&& t : threads) t.join();

  // compute the sum of all the values for validation
  std::cout << "Sum: " << Sum(bztree) << std::endl;

  // reset a BzTree instance
  bztree = std::make_unique<BzTree_t>();

  // write values by four threads
  threads.clear();
  threads.emplace_back(f, 0, 1e6);
  threads.emplace_back(f, 1e6, 2e6);
  threads.emplace_back(f, 2e6, 3e6);
  threads.emplace_back(f, 3e6, 4e6);
  for (auto&& t : threads) t.join();

  // check all the values are written
  std::cout << "Sum: " << Sum(bztree) << std::endl;

  return 0;
}
```

This code will output the following results.

```txt
Sum: 7999998000000
Sum: 7999998000000
```

### Variable-Length Keys/Values

If you use variable-length keys/values (i.e., binary data), you need to specify theier lengths for each API except for the read API. Note that it is also required to implement a specialized `IsVariableLengthData` function in `dbgroup::index::bztree` namespace to represent variable-length data. The following snippet is an example to use `char*` as variable-length keys/values.

```cpp
#include <stdio.h>

#include <iostream>

#include "bztree/bztree.hpp"

using Key = char*;
using Value = char*;

// we prepare a comparator for CString as an example one
using ::dbgroup::index::bztree::CompareAsCString;

using BzTree_t = ::dbgroup::index::bztree::BzTree<Key, Value, CompareAsCString>;
using ::dbgroup::index::bztree::ReturnCode;

namespace dbgroup::index::bztree
{
/**
 * @brief Use CString as variable-length data.
 *
 */
template <>
constexpr bool
IsVariableLengthData<char*>()
{
  return true;
}

}  // namespace dbgroup::index::bztree

int
main([[maybe_unused]] int argc, [[maybe_unused]] char** argv)
{
  constexpr size_t kWordLength = 8;

  // create a BzTree instance
  BzTree_t bztree{};

  // prepare a variable-length key/value
  char key[kWordLength], value[kWordLength];
  snprintf(key, kWordLength, "key");
  snprintf(value, kWordLength, "value");

  // the length of CString includes '\0'
  bztree.Write(key, value, 4, 6);

  // in the case of variable-length values, the type of the return value is std::unique_ptr
  auto [rc, read_value] = bztree.Read(key);
  std::cout << "Return code: " << rc << std::endl;
  std::cout << "Read value : " << read_value.get() << std::endl;

  return 0;
}
```

This code will output the following results.

```txt
Return code: 0
Read value : value
```

If you use bulk-load api with variable-length keys/values, it is also required to implement a specialized class `BulkloadEntry` in dbgroup::index::bztree namespace. The following snippet is an example to use bulk-load api with char* as variable-length keys/values.

```cpp
#include <iostream>

#include "bztree/bztree.hpp"

using Key = char *;
using Value = char *;

// we prepare a comparator for CString as an example one
using ::dbgroup::index::bztree::CompareAsCString;

using BzTree_t = ::dbgroup::index::bztree::BzTree<Key, Value, CompareAsCString>;
using ::dbgroup::index::bztree::ReturnCode;

using Entry = ::dbgroup::index::bztree::BulkloadEntry<Key, Value>;
using EntryArray = std::vector<Entry>;

namespace dbgroup::index::bztree
{
/**
 * @brief Use CString as variable-length data.
 *
 */
template <>
constexpr bool
IsVariableLengthData<char *>()
{
  return true;
}

/**
 * @brief An example specialization of BulkloadEntry.
 *
 */
template <>
class BulkloadEntry<char *, char *>
{
 private:
  char *key_{};
  char *payload_{};

 public:
  constexpr BulkloadEntry(  //
      const char *key,
      const char *payload)
      : key_{const_cast<char *>(key)}, payload_{const_cast<char *>(payload)}
  {
  }

  ~BulkloadEntry() = default;

  constexpr auto
  GetKey() const  //
      -> char *const &
  {
    return key_;
  }

  constexpr auto
  GetPayload() const  //
      -> char *const &
  {
    return payload_;
  }

  constexpr auto
  GetKeyLength() const  //
      -> size_t
  {
    return 7;
  }

  constexpr auto
  GetPayloadLength() const  //
      -> size_t
  {
    return 7;
  }
};

}  // namespace dbgroup::index::bztree

void
PrepareSampleData(  //
    char **data_array,
    const size_t data_num,
    [[maybe_unused]] const size_t data_length)
{
  for (size_t i = 0; i < data_num; ++i) {
    auto data = reinterpret_cast<char *>(::operator new(data_length));
    snprintf(data, data_length, "%06lu", i);
    data_array[i] = reinterpret_cast<char *>(data);
  }
}

int
main([[maybe_unused]] int argc, [[maybe_unused]] char **argv)
{
  static constexpr size_t kKeyNum = 4096;

  // create a BzTree instance
  BzTree_t bztree{};

  // create sample data
  Key keys[kKeyNum];
  Value values[kKeyNum];
  PrepareSampleData(keys, kKeyNum, 7);
  PrepareSampleData(values, kKeyNum, 7);

  // create entries
  EntryArray entries;
  for (size_t i = 0; i < kKeyNum - 1; ++i) {
    entries.emplace_back(Entry{keys[i], values[i]});
  }

  // bulk-load entries
  bztree.BulkLoadForSingleThread(entries);

  // read bulk-loaded key and one key that has not been inserted
  for (size_t i = 0; i < kKeyNum; ++i) {
    auto [rc, value] = bztree.Read(keys[i]);

    if (rc != ReturnCode::kSuccess) {
      std::cout << "Search key: " << i << std::endl;
      std::cout << "Return code: " << rc << std::endl;
    }
  }

  return 0;
}
```

This code will output the following results.

```txt
Search key: 4095
Return code: 1
```
### Updating Payloads by Using MwCAS

Although our BzTree can update payloads directly by using MwCAS operations (for details, please refer to Section 4.2 in [1]), it is restricted to unsigned integers and pointer types as default. To enable this feature for your own type, it must satisfy the following conditions:

1. the length of a class is `8` (i.e., `static_assert(sizeof(<payload_class>) == 8)`),
2. the last three bits are reserved for MwCAS control bits and initialized by zeros, and
3. a specialized `CanCASUpdate` function is implemented in `dbgroup::index::bztree` namespace.

The following snippet is an example implementation of an original payload class to enable MwCAS-based update.

```cpp
/**
 * @brief An example class to represent CAS-updatable data.
 *
 */
struct MyClass {
  /// an actual payload
  uint64_t data : 61;

  /// reserve three bits for MwCAS operations
  uint64_t control_bits : 3;

  // control bits must be initialzed by zeros
  constexpr MyClass() : data{}, control_bits{0} {}

  ~MyClass() = default;

  // target class must be trivially copyable
  constexpr MyClass(const MyClass &) = default;
  constexpr MyClass &operator=(const MyClass &) = default;
  constexpr MyClass(MyClass &&) = default;
  constexpr MyClass &operator=(MyClass &&) = default;

  // enable std::less to compare this class
  constexpr bool
  operator<(const MyClass &comp) const
  {
    return data < comp.data;
  }
};

namespace dbgroup::index::bztree
{
/**
 * @brief An example specialization to enable CAS-based update.
 *
 */
template <>
constexpr bool
CanCASUpdate<MyClass>()
{
  return true;
}

}  // namespace dbgroup::index::bztree
```
