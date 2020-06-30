cmake_minimum_required(VERSION 3.16)
project(db2_programming_project)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)
include_directories(compression)
include_directories(core)
include_directories(data)
include_directories(doc)

add_executable(db2_programming_project
        compression/dictionary_compressed_column.hpp
        core/base_column.hpp
        core/column.hpp
        core/column_base_typed.hpp
        core/compressed_column.hpp
        core/global_definitions.hpp
        core/lookup_array.hpp
        "data/float column"
        "data/int column"
        "data/string column"
        doc/documentation.hpp
        doc/doxygen.conf
        doc/warnings.txt
        base_column.cpp
        main.cpp
        Makefile
        unittest.hpp)
