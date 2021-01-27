# BzTree

## 想定環境

以下のVS Code拡張がインストール済み．

- `ms-vscode.cpptools`
- `ms-vscode.cmake-tools`
- `twxs.cmake`
- (optional) `mine.cpplint`
- (optional) `jeff-hykin.better-cpp-syntax`
- (optional) `matepek.vscode-catch2-test-adapter`

他，コンパイラなどの設定は`CMakeList.txt`に従う．

## 開発準備

必要なライブラリをインストール．

```bash
sudo apt install -y build-essential cmake gdb libnuma-dev
```

サブモジュール含めてリポジトリをクローン．

```bash
git clone --recurse-submodules git@github.com:dbgroup-nagoya-u/202004-bztree.git
```

## コンパイル・デバッグ

デバッグ用にコンパイルしたい場合は，[VS Codeのコマンドパレット](https://code.visualstudio.com/docs/getstarted/userinterface#_command-palette)を開いて，`CMake: Select Variant`で`Debug`か`RelWithDebInfo`を選ぶ．そのまま`cmake`のconfigurationが行われる（手動でやる場合は`CMake: Configuration`）ので，終わったら`CMake: Build`でビルド．（この辺は全て下部のステータスバーからも行える．）

## ビルドオプション

- `BZTREE_BUILD_TESTS`：デフォルト`off`．
