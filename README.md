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

## For Developers

### Recommended Extensions for VS Code

- `ms-vscode.cpptools`
- `ms-vscode.cmake-tools`
- `twxs.cmake`
- (optional) `mine.cpplint`
- (optional) `jeff-hykin.better-cpp-syntax`
- (optional) `matepek.vscode-catch2-test-adapter`

### Prerequisites

```bash
sudo apt install -y build-essential cmake gdb
cd <_workspace>
git clone --recurse-submodules git@github.com:dbgroup-nagoya-u/bztree.git
```
