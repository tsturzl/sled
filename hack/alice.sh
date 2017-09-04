#!/bin/sh

# ALICE test for rsdb
# Usage:
# 1. get ALICE from https://github.com/madthanu/alice
# 2. configure and install alice-strace:
#    cd alice/alice-strace && ./configure && make && make install
# 3. set the ALICE_HOME variable below to the directory of the alice repo
# 4. run this script from the root of the rsdb codebase

set -e

export ALICE_HOME=$HOME/src/alice/
export PATH=$PATH:$ALICE_HOME/bin

# 1. build rsdb dylib so that rsdb.py can load it
cd rsdbc

cargo build

cd ..

# 2. set up directories
rm -rf alice

mkdir -p alice/work
mkdir -p alice/traces

cp rsdbc/target/debug/librsdb.so alice/

cd alice

# 3. run trace
alice-record --workload_dir work \
  --traces_dir traces \
  ../python/alice_populate.py

# 4. run check
alice-check --traces_dir=traces --checker=../python/alice_check.py
