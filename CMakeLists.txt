cmake_minimum_required (VERSION 2.6)
project (SuRF)

message(STATUS "Configuring..." ${CMAKE_PROJECT_NAME})

include("${CMAKE_SOURCE_DIR}/cmake/clang-tidy.cmake")

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release")
endif()



set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -g -Wall -mpopcnt -pthread -std=c++11")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -O3 -Wall -Werror -mpopcnt -pthread -std=c++11")

option(COVERALLS "Generate coveralls data" OFF)

if (COVERALLS)
  include("${CMAKE_CURRENT_SOURCE_DIR}/CodeCoverage.cmake")
  append_coverage_compiler_flags()
  set(COVERAGE_EXCLUDES 'ARF/*' 'bench/*' 'test/*' '/usr/*' '/lib/*')
  setup_target_for_coverage(
    NAME coverage
    EXECUTABLE make test
    )
else()
  add_definitions(-DNDEBUG)
endif()

enable_testing()

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include")

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

# Gather lintable files
set(SRC_CC_LINTING "")
foreach(SRC_FILE ${SRC_CC})
  list(FIND SRC_CC_LINTER_IGNORE "${SRC_FILE}" SRC_FILE_IDX)
  if (${SRC_FILE_IDX} EQUAL -1)
    list(APPEND SRC_CC_LINTING "${SRC_FILE}")
  endif()
endforeach()

add_clang_tidy_target(lint_src "${SRC_CC_LINTING}")
list(APPEND lint_targets lint_src)


add_custom_target(lint)
add_dependencies(lint ${lint_targets})

add_subdirectory(test)


#add_subdirectory(bench)

#include_directories("${CMAKE_CURRENT_SOURCE_DIR}/ARF/include")
#add_subdirectory(ARF)
