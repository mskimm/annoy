cmake_minimum_required(VERSION 3.8)
project(annoy)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY bin)

add_executable(annoy-on-rocksdb
        src/annoyrocksdblib.h
        test/annoy-on-rocksdb.cpp)

target_link_libraries(annoy-on-rocksdb rocksdb)
