cmake_minimum_required(VERSION 3.23)
project(Lsm_KV)

set(CMAKE_CXX_STANDARD 14)

add_executable(Lsm_KV skiplist.cc skiplist.h sstable.cc sstable.h test.h utils.h persistence.cc kvstore.h kvstore.cc bloomfilter.cc global.cc)
