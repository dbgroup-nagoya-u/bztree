cmake_minimum_required(VERSION "3.8")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(bztree
  VERSION "0.1.0"
  DESCRIPTION "An open-source BzTree library."
  LANGUAGES "CXX"
)

# define third-party's source directories
set(PMWCAS_SROUCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/pmwcas")
set(GOOGLETEST_SROUCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/googletest")

# BzTree uses the PMwCAS library with three-words swap
set(PMWCAS_DESC_CAP "3")
add_subdirectory("${PMWCAS_SROUCE_DIR}")

# build BzTree as a header-only library
add_library(bztree INTERFACE)
target_compile_features(bztree INTERFACE "cxx_std_20")
target_compile_options(bztree INTERFACE
  -Wall
  -Wextra
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
)
target_include_directories(bztree INTERFACE
  "${CMAKE_CURRENT_SOURCE_DIR}/src"
  "${PMWCAS_SROUCE_DIR}"
  "${PMWCAS_SROUCE_DIR}/src"
  "${PMWCAS_SROUCE_DIR}/include"
)
target_link_libraries(bztree INTERFACE pmwcas_static)
add_dependencies(bztree pmwcas_static)

# build unit tests if required
option(BZTREE_BUILD_TESTS, "Build unit tests for BzTree" OFF)
if(${BZTREE_BUILD_TESTS})
  # use GoogleTest for unit tests
  add_subdirectory("${GOOGLETEST_SROUCE_DIR}")
  # include unit tests
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
