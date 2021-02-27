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

# define source directories
set(BZTREE_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(MWCAS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/mwcas")

# BzTree uses the MwCAS library with three-words swap
set(MWCAS_CAPACITY "3" CACHE STRING "Use MwCAS with three words" FORCE)
add_subdirectory("${MWCAS_SOURCE_DIR}")

# build BzTree as a header-only library
add_library(bztree INTERFACE)
target_compile_features(bztree INTERFACE
  "cxx_std_17"
)
target_compile_options(bztree INTERFACE
  -Wall
  -Wextra
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
)
target_include_directories(bztree INTERFACE
  "${BZTREE_SOURCE_DIR}/include"
)
target_link_libraries(bztree INTERFACE
  mwcas
)
add_dependencies(bztree
  mwcas
)

# build unit tests if required
option(BZTREE_BUILD_TESTS, "Build unit tests for BzTree" OFF)
if(${BZTREE_BUILD_TESTS})
  enable_testing()
  # include unit tests
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
