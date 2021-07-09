# BzTree

![example workflow name](https://github.com/dbgroup-nagoya-u/bztree/workflows/Unit%20Tests/badge.svg?branch=main)

## Build

**Note**: this is a header only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

- `BZTREE_PAGE_SIZE`: the byte length of each node page: default `8192`.

### Build Options for Unit Testing

- `BZTREE_BUILD_TESTS`: build unit tests for this library if `on`: default `off`.
- `BZTREE_TEST_THREAD_NUM`: the maximum number of threads to perform unit tests: default `8`.

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DBZTREE_BUILD_TESTS=on ..
make -j
ctest -C Release
```
