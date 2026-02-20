cmake_minimum_required(VERSION 3.20)
project(bakread VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
option(BAKREAD_ENABLE_PARQUET "Build with Apache Arrow / Parquet support" ON)
option(BAKREAD_ENABLE_TESTS   "Build test suite"                         OFF)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
# ODBC -- required for Mode B (restore & extract)
if(WIN32)
    # Windows ships ODBC as a system library
    set(ODBC_LIBRARIES odbc32 odbccp32)
else()
    find_package(ODBC REQUIRED)
    set(ODBC_LIBRARIES ODBC::ODBC)
endif()

# Apache Arrow / Parquet -- optional but strongly recommended
if(BAKREAD_ENABLE_PARQUET)
    find_package(Arrow  QUIET)
    find_package(Parquet QUIET)
    if(Arrow_FOUND AND Parquet_FOUND)
        message(STATUS "Arrow ${Arrow_VERSION} and Parquet found -- Parquet output enabled")
        add_compile_definitions(BAKREAD_HAS_PARQUET=1)
    else()
        message(WARNING "Arrow/Parquet not found -- Parquet output disabled. "
                        "Install via vcpkg: vcpkg install arrow:x64-windows")
        set(BAKREAD_ENABLE_PARQUET OFF)
    endif()
endif()

# Threading
find_package(Threads REQUIRED)

# zlib -- used for decompression of certain backup blocks
find_package(ZLIB QUIET)
if(ZLIB_FOUND)
    add_compile_definitions(BAKREAD_HAS_ZLIB=1)
endif()

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------
set(BAKREAD_SOURCES
    src/main.cpp
    src/cli.cpp
    src/logging.cpp
    src/backup_stream.cpp
    src/backup_header.cpp
    src/decompressor.cpp
    src/row_decoder.cpp
    src/catalog_reader.cpp
    src/direct_extractor.cpp
    src/restore_adapter.cpp
    src/tde_handler.cpp
    src/csv_writer.cpp
    src/parquet_writer.cpp
    src/json_writer.cpp
    src/pipeline.cpp
    src/page_index.cpp
    src/lru_cache.cpp
    src/indexed_page_store.cpp
)

# ---------------------------------------------------------------------------
# Target
# ---------------------------------------------------------------------------
add_executable(bakread ${BAKREAD_SOURCES})

target_include_directories(bakread PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_link_libraries(bakread PRIVATE
    ${ODBC_LIBRARIES}
    Threads::Threads
)

if(ZLIB_FOUND)
    target_link_libraries(bakread PRIVATE ZLIB::ZLIB)
endif()

if(BAKREAD_ENABLE_PARQUET)
    target_link_libraries(bakread PRIVATE
        Arrow::arrow_shared
        Parquet::parquet_shared
    )
endif()

# ---------------------------------------------------------------------------
# Compiler flags
# ---------------------------------------------------------------------------
if(MSVC)
    target_compile_options(bakread PRIVATE /W4 /permissive- /Zc:__cplusplus)
    target_compile_definitions(bakread PRIVATE
        _CRT_SECURE_NO_WARNINGS
        NOMINMAX
        WIN32_LEAN_AND_MEAN
        UNICODE
        _UNICODE
    )
else()
    target_compile_options(bakread PRIVATE -Wall -Wextra -Wpedantic)
endif()

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------
install(TARGETS bakread RUNTIME DESTINATION bin)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(BAKREAD_ENABLE_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
