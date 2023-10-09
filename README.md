# BzTree

[![Ubuntu-20.04](https://github.com/dbgroup-nagoya-u/bztree/actions/workflows/unit_tests.yaml/badge.svg)](https://github.com/dbgroup-nagoya-u/bztree/actions/workflows/unit_tests.yaml)

This repository is an open source implementation of a BzTree[^1] for research use. The purpose of this implementation is to reproduce a BzTree and measure its performance. However, the concurrency controls proposed in the paper are insufficient, so we have modified them to guarantee consistent read/write operations. Moreover, some tuning parameters have been changed for convenience.

Note that although the original BzTree is proposed as an index for persistent memory (e.g., Intel Optane), we implemented our BzTree for volatile memory. Thus, there is no persistency support in this implementation.

- [Build](#build)
    - [Prerequisites](#prerequisites)
    - [Build Options](#build-options)
    - [Build Options for Unit Testing](#build-options-for-unit-testing)
    - [Build and Run Unit Tests](#build-and-run-unit-tests)
- [Usage](#usage)
    - [Linking by CMake](#linking-by-cmake)
    - [Read/Write APIs](#readwrite-apis)
    - [Updating Payloads Using MwCAS](#updating-payloads-using-mwcas)

## Build

**Note**: this is a header-only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

#### Tuning Parameters

- `BZTREE_PAGE_SIZE`: Page size in bytes (default `1024`).
- `BZTREE_MAX_DELTA_RECORD_NUM`: Invoking consolidation if the number of delta records exceeds this threshold (default `64`).
- `BZTREE_MAX_DELETED_SPACE_SIZE`: Invoking consolidation if the size of deleted space exceeds this threshold (default `${BZTREE_PAGE_SIZE} / 8`).
- `BZTREE_MIN_FREE_SPACE_SIZE`: Invoking a split-operation if the size of free space falls below this threshold (default `${BZTREE_PAGE_SIZE} / 8`).
- `BZTREE_MIN_NODE_SIZE`: Invoking a merge-operation if the size of a consolidated node falls below this threshold (default `${BZTREE_PAGE_SIZE} / 16`).
- `BZTREE_MAX_MERGED_SIZE`: Canceling a merge-operation if the size of a merged node exceeds this threshold (default `${BZTREE_PAGE_SIZE} / 2`).
- `BZTREE_MAX_VARIABLE_DATA_SIZE`: The expected maximum size of a variable-length data (default `128`).

### Build Options for Unit Testing

- `BZTREE_BUILD_TESTS`: Building unit tests for this library if `ON` (default `OFF`).
- `BZTREE_TEST_BUILD_APPEND`: Build tests for append based BzTrees if `ON` (default `OFF`).
- `BZTREE_TEST_BUILD_IN_PLACE`: Build tests for in-place based BzTrees if `ON` (default `OFF`).
- `DBGROUP_TEST_THREAD_NUM`: The maximum number of threads for testing (default `8`).
- `DBGROUP_TEST_RANDOM_SEED`: A fixed seed value to reproduce unit tests (default `0`).
- `DBGROUP_TEST_EXEC_NUM`: The number of executions per a thread (default `1E5`).
- `DBGROUP_TEST_OVERRIDE_MIMALLOC`: Override entire memory allocation with mimalloc (default `OFF`).
    - NOTE: we use `find_package(mimalloc 1.7 REQUIRED)` to link mimalloc.

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
      dbgroup::bztree
    )
    ```

### Read/Write APIs

We provide the same read/write APIs for the implemented indexes. See [here](https://github.com/dbgroup-nagoya-u/index-benchmark/wiki/Common-APIs-for-Index-Implementations) for common APIs and usage examples.

### Updating Payloads Using MwCAS

Although our BzTree can update payloads directly by using MwCAS operations (for details, please refer to Section 4.2 in the original paper[^1]), it is restricted to unsigned integers and pointer types as default. To enable this feature for your own type, the class must satisfy the following conditions:

1. the length of a class is `8` (i.e., `static_assert(sizeof(<payload_class>) == 8)`),
2. the last bit is reserved for MwCAS control bits and initialized by zeros, and
3. a specialized `CanCASUpdate` function is implemented in `dbgroup::index::bztree` namespace.

The following snippet is an example implementation of an original payload class to enable MwCAS-based update.

```cpp
/**
 * @brief An example class to represent CAS-updatable data.
 *
 */
struct MyClass {
  /// an actual payload
  uint64_t data : 63;

  /// reserve at least one bit for MwCAS operations
  uint64_t control_bits : 1;

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
 * @brief An example specialization to enable in-place updating.
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

[^1]: [J. Arulraj, J. Levandoski, U. F. Minhas, P.-A. Larson, "BzTree: A High-Performance Latch-free Range Index for Non-Volatile Memory,” PVLDB, Vol. 11, No. 5, pp. 553-565, 2018](http://www.vldb.org/pvldb/vol11/p553-arulraj.pdf).
