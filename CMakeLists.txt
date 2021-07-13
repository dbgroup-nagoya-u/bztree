cmake_minimum_required(VERSION "3.8")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(
  bztree
  VERSION "0.1.0"
  DESCRIPTION "An open-source BzTree library."
  LANGUAGES "CXX"
)

# define source directories
set(BZTREE_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(MWCAS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/mwcas")
set(MEMORY_MANAGER_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/memory-manager")

#--------------------------------------------------------------------------------------#
# Configure external libraries
#--------------------------------------------------------------------------------------#

# BzTree uses the MwCAS library with three-words swap
set(MWCAS_CAPACITY "3" CACHE STRING "Use MwCAS with three words" FORCE)
add_subdirectory("${MWCAS_SOURCE_DIR}")

# use GC
add_subdirectory("${MEMORY_MANAGER_SOURCE_DIR}")

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

add_library(bztree INTERFACE)
target_compile_features(
  bztree INTERFACE
  "cxx_std_17"
)
target_compile_options(
  bztree INTERFACE
  -Wall
  -Wextra
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
)
target_include_directories(
  bztree INTERFACE
  "${BZTREE_SOURCE_DIR}/include"
)
target_link_libraries(
  bztree INTERFACE
  mwcas
  memory_manager
)

#--------------------------------------------------------------------------------------#
# Add compile options
#--------------------------------------------------------------------------------------#

if(DEFINED BZTREE_PAGE_SIZE)
  target_compile_definitions(
    bztree INTERFACE
    BZTREE_PAGE_SIZE=${BZTREE_PAGE_SIZE}
  )
endif()

if(DEFINED BZTREE_MAX_UNSORTED_REC_NUM)
  target_compile_definitions(
    bztree INTERFACE
    BZTREE_MAX_UNSORTED_REC_NUM=${BZTREE_MAX_UNSORTED_REC_NUM}
  )
endif()

if(DEFINED BZTREE_MAX_DELETED_SPACE_SIZ)
  target_compile_definitions(
    bztree INTERFACE
    BZTREE_MAX_DELETED_SPACE_SIZE=${BZTREE_MAX_DELETED_SPACE_SIZE}
  )
endif()

if(DEFINED BZTREE_MIN_FREE_SPACE_SIZE)
  target_compile_definitions(
    bztree INTERFACE
    BZTREE_MIN_FREE_SPACE_SIZE=${BZTREE_MIN_FREE_SPACE_SIZE}
  )
endif()

if(DEFINED BZTREE_MIN_SORTED_REC_NUM)
  target_compile_definitions(
    bztree INTERFACE
    BZTREE_MIN_SORTED_REC_NUM=${BZTREE_MIN_SORTED_REC_NUM}
  )
endif()

if(DEFINED BZTREE_MAX_MERGED_SIZE)
  target_compile_definitions(
    bztree INTERFACE
    BZTREE_MAX_MERGED_SIZE=${BZTREE_MAX_MERGED_SIZE}
  )
endif()

#--------------------------------------------------------------------------------------#
# Build unit tests
#--------------------------------------------------------------------------------------#

option(BZTREE_BUILD_TESTS "Build unit tests for BzTree" OFF)
if(${BZTREE_BUILD_TESTS})
  enable_testing()
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
