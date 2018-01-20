cmake_minimum_required(VERSION 3.8)
project(annoy)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY bin)

add_executable(test-annoy
        src/annoylib.h
        src/experimental/annoylib.h
        test/experimental/test.cpp)

target_link_libraries(test-annoy rocksdb)
