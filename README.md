# BzTree

![example workflow name](https://github.com/dbgroup-nagoya-u/bztree/workflows/Unit%20Tests/badge.svg?branch=main)

## Build

**Note**: this is a header only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

- `BZTREE_BUILD_TESTS`: build unit tests for this library if `on`: default `off`.

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DBZTREE_BUILD_TESTS=on ..
make -j
ctest -C Release
```
