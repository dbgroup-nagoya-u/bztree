# BzTree

![example workflow name](https://github.com/dbgroup-nagoya-u/bztree/workflows/Unit%20Tests/badge.svg?branch=main)

## Build

**Note**: this is a header only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

- `BZTREE_PAGE_SIZE`: The byte length of each node page (default `8192`).
- `BZTREE_MAX_UNSORTED_REC_NUM`: Invoking consolidation if the number of unsorted records exceeds this threshold (default `64`).
- `BZTREE_MAX_DELETED_SPACE_SIZE`: Invoking consolidation if the size of deleted records exceeds this threshold (default `0.25 * BZTREE_PAGE_SIZE`).
- `BZTREE_MIN_FREE_SPACE_SIZE`: Invoking a split if the size of free space in a node exceeds this threshold (default `BZTREE_MAX_UNSORTED_REC_NUM * 24`).
- `BZTREE_MIN_SORTED_REC_NUM`: Invoking merging if the number of sorted records falls below this threshold (default `2 * BZTREE_MAX_UNSORTED_REC_NUM`).
- `BZTREE_MAX_MERGED_SIZE`: Canceling merging if the size of a merged node exceeds this threshold (default `BZTREE_PAGE_SIZE - (2 * BZTREE_MIN_FREE_SPACE_SIZE)`).
- `BZTREE_USE_MIMALLOC`: use [mimalloc](https://github.com/microsoft/mimalloc) as a memory allocator/deleter if `on` (default `off`).
    - If you use this option, you need to install mimalloc beforehand and enable `cmake` find it by using the [find_package](https://cmake.org/cmake/help/latest/command/find_package.html) command.

### Build Options for Unit Testing

- `BZTREE_BUILD_TESTS`: Building unit tests for this library if `on` (default `off`).
- `BZTREE_TEST_THREAD_NUM`: The maximum number of threads to perform unit tests (default `8`).

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DBZTREE_BUILD_TESTS=on ..
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

If you use fixed length types as keys/values, you can use our BzTree by simply declaring it.

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

### Multi-Threading

This library is a thread-safe implementation. You can call all the APIs (i.e., `Read`, `Scan`, `Write`, `Insert`, `Update`, and `Delete`) from multi-threads concurrently. Note that concurrent writes follow the last write win protocol, and so you need to some concurrency control methods (e.g., snapshot isolation) to guarantee the order of read/write operations.

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

### Variable Length Keys/Values

If you use variable length keys/values (i.e., binary data), you need to specify theier lengths for each API except for the read API. Note that we use `char*` to represent binary data, and so you may need to cast your keys/values to write a BzTree instance, such as `reinterpret_cast<char*>(&<key_instance>)`.

```cpp
#include <iostream>
#include <string>

#include "bztree/bztree.hpp"

// we use char* to represent binary data
using Key = char*;
using Value = char*;

// we prepare a comparator for CString as an example
using ::dbgroup::index::bztree::CompareAsCString;

using BzTree_t = ::dbgroup::index::bztree::BzTree<Key, Value, CompareAsCString>;
using ::dbgroup::index::bztree::ReturnCode;

int
main([[maybe_unused]] int argc, [[maybe_unused]] char** argv)
{
  // create a BzTree instance
  BzTree_t bztree{};

  // the length of CString includes '\0'
  bztree.Write("key", "value", 4, 6);

  // in the case of variable values, the type of the return value is std::unique_ptr<char>
  auto [rc, value] = bztree.Read("key");
  std::cout << "Return code: " << rc << std::endl;
  std::cout << "Read value : " << value.get() << std::endl;

  return 0;
}
```

This code will output the following results.

```txt
Return code: 0
Read value : value
```
