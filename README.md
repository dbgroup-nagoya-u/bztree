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
