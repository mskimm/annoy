cmake_minimum_required(VERSION 3.8)
project(annoy)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY bin)

add_executable(precision_test
        examples/precision_test.cpp
        src/annoylib.h)
