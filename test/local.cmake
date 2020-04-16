# ---------------------------------------------------------------------------
# Test Files and Classes
# ---------------------------------------------------------------------------

find_library(SDSL_PATH sdsl PATH ~/lib)
message("found the following path for sdsl ${SDSL_PATH}")

function(add_unit_test file_name name)
    add_executable(${name} ${file_name}.cpp)
    target_link_libraries(${name} gtest ${SDSL_PATH})
    add_test(NAME ${name}
            COMMAND ${CMAKE_CURRENT_BINARY_DIR}/${name}
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endfunction()

# ---------------------------------------------------------------------------
# Add Each Test as Separate Executable
# ---------------------------------------------------------------------------
add_unit_test(test/test_fst_example test_example)
add_unit_test(test/test_fst_example_words test_example_words)
add_unit_test(test/test_fst_ints test_int32)
add_unit_test(test/test_bitvector test_bitvector)


# ---------------------------------------------------------------------------
# Copy required files for test to build directory
# ---------------------------------------------------------------------------
configure_file(test/keys.txt keys.txt COPYONLY)
