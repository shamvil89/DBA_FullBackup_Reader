find_package(GTest QUIET)

if(GTest_FOUND)
    add_executable(bakread_tests
        test_types.cpp
        test_row_decoder.cpp
        test_decompressor.cpp
        test_csv_writer.cpp
    )

    target_include_directories(bakread_tests PRIVATE ${CMAKE_SOURCE_DIR}/include)
    target_link_libraries(bakread_tests PRIVATE GTest::gtest_main)

    include(GoogleTest)
    gtest_discover_tests(bakread_tests)
else()
    message(STATUS "GTest not found -- tests will not be built")
endif()
