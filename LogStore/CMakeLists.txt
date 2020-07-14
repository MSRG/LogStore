cmake_minimum_required(VERSION 3.5)
project(Logstore)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)
include_directories(include)

set(DB_SRC
        db/c.cc
        db/db_impl.cc
        db/builder.cc
        db/db_iter.cc
        db/dbformat.cc
        db/dynamic_histogram.cc
        db/static_histogram.cc
        db/filename.cc
        db/log_reader.cc
        db/log_writer.cc
        db/memtable.cc
        db/table_cache.cc
        db/version_edit.cc
        db/version_set.cc
        db/write_batch.cc)

set(PORT_SRC port/port_posix.cc)
set(TABLE_SRC
        table/block.cc
        table/block_builder.cc
        table/filter_block.cc
        table/format.cc
        table/iterator.cc
        table/merger.cc
        table/table.cc
        table/table_builder.cc
        table/two_level_iterator.cc
        )

set(UTIL_SRC
        util/arena.cc
        util/bloom.cc
        util/cache.cc
        util/coding.cc
        util/comparator.cc
        util/crc32c.cc
        util/env.cc
        util/env_posix.cc
        util/filter_policy.cc
        util/hash.cc
        util/histogram.cc
        util/logging.cc
        util/options.cc
        util/statistics.cc
        util/status.cc
        )

set(HELPER_SRC
        helpers/memenv/memenv.cc)

if (__APPLE__)
    include_directories("/Applications/Xcode.app/Contents//Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk/usr/include")
    include_directories("/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/clang/11.0.3/include")
    add_definitions(OS_MACOSX)
endif ()

add_library(leveldb ${DB_SRCS} ${PORT_SRC} ${TABLE_SRC} ${UTIL_SRC} ${HELPER_SRC})

add_executable(db_bench db/db_bench.cc)
target_link_libraries(db_bench leveldb)