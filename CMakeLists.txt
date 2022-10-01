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

# check SSE support for spinlock hints
include(CheckIncludeFileCXX)
CHECK_INCLUDE_FILE_CXX("xmmintrin.h" BZTREE_HAS_SPINLOCK_HINT)

#--------------------------------------------------------------------------------------#
# BzTree configurations
#--------------------------------------------------------------------------------------#

set(BZTREE_PAGE_SIZE "1024" CACHE STRING "Page size in bytes.")
set(BZTREE_MAX_DELTA_RECORD_NUM "64" CACHE STRING "Invoking consolidation if the number of delta records exceeds this threshold.")
set(BZTREE_MAX_DELETED_SPACE_SIZE "(kPageSize / 8)" CACHE STRING "Invoking consolidation if the size of deleted space exceeds this threshold.")
set(BZTREE_MIN_FREE_SPACE_SIZE "(kPageSize / 8)" CACHE STRING "Invoking a split-operation if the size of free space falls below this threshold.")
set(BZTREE_MIN_NODE_SIZE  "(kPageSize / 16)" CACHE STRING "Invoking a merge-operation if the size of a consolidated node falls below this threshold.")
set(BZTREE_MAX_MERGED_SIZE "(kPageSize / 2)" CACHE STRING "Canceling a merge-operation if the size of a merged node exceeds this threshold.")
set(BZTREE_MAX_VARIABLE_DATA_SIZE "128" CACHE STRING "The maximun size of variable-length data.")

#--------------------------------------------------------------------------------------#
# Configure external libraries
#--------------------------------------------------------------------------------------#

# BzTree uses the MwCAS library with three-words swap
if(NOT DEFINED MWCAS_CAPACITY)
  set(MWCAS_CAPACITY "3" CACHE STRING "BzTree uses three-words CAS." FORCE)
endif()

add_subdirectory("${MWCAS_SOURCE_DIR}")

if(MWCAS_CAPACITY LESS 3)
  message(FATAL_ERROR "BzTree requires at least three-words CAS.")
endif()

# configure GC
add_subdirectory("${MEMORY_MANAGER_SOURCE_DIR}")

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET bztree::bztree)
  add_library(bztree INTERFACE)
  add_library(bztree::bztree ALIAS bztree)
  target_compile_features(bztree INTERFACE
    "cxx_std_17"
  )
  target_compile_definitions(bztree INTERFACE
    BZTREE_PAGE_SIZE=${BZTREE_PAGE_SIZE}
    BZTREE_MAX_DELTA_RECORD_NUM=${BZTREE_MAX_DELTA_RECORD_NUM}
    BZTREE_MAX_DELETED_SPACE_SIZE=${BZTREE_MAX_DELETED_SPACE_SIZE}
    BZTREE_MIN_FREE_SPACE_SIZE=${BZTREE_MIN_FREE_SPACE_SIZE}
    BZTREE_MIN_NODE_SIZE=${BZTREE_MIN_NODE_SIZE}
    BZTREE_MAX_MERGED_SIZE=${BZTREE_MAX_MERGED_SIZE}
    BZTREE_MAX_VARIABLE_DATA_SIZE=${BZTREE_MAX_VARIABLE_DATA_SIZE}
    $<$<BOOL:${BZTREE_HAS_SPINLOCK_HINT}>:BZTREE_HAS_SPINLOCK_HINT>
  )
  target_include_directories(bztree INTERFACE
    "${BZTREE_SOURCE_DIR}/include"
  )
  target_link_libraries(bztree INTERFACE
    mwcas::mwcas
    memory_manager::memory_manager
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
