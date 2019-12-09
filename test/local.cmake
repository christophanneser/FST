# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

set(TEST_CC
        test/test_fst_ints.cpp
        #test/test_fst_example.cpp
        #test/test_fst_example_words.cpp
        #test/test_fst_small.cpp
        )

# ---------------------------------------------------------------------------
# Tester
# ---------------------------------------------------------------------------

add_executable(tester test/tester.cc ${TEST_CC})
target_link_libraries(tester gtest gmock Threads::Threads)

enable_testing()
add_test(moderndbs tester)
