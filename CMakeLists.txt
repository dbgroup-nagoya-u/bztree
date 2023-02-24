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

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/mwcas")

if(MWCAS_CAPACITY LESS 3)
  message(FATAL_ERROR "BzTree requires at least three-words CAS.")
endif()

# configure GC
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/memory-manager")

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET dbgroup::${PROJECT_NAME})
  add_library(${PROJECT_NAME} INTERFACE)
  add_library(dbgroup::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
  target_compile_features(${PROJECT_NAME} INTERFACE
    "cxx_std_17"
  )
  target_compile_definitions(${PROJECT_NAME} INTERFACE
    BZTREE_PAGE_SIZE=${BZTREE_PAGE_SIZE}
    BZTREE_MAX_DELTA_RECORD_NUM=${BZTREE_MAX_DELTA_RECORD_NUM}
    BZTREE_MAX_DELETED_SPACE_SIZE=${BZTREE_MAX_DELETED_SPACE_SIZE}
    BZTREE_MIN_FREE_SPACE_SIZE=${BZTREE_MIN_FREE_SPACE_SIZE}
    BZTREE_MIN_NODE_SIZE=${BZTREE_MIN_NODE_SIZE}
    BZTREE_MAX_MERGED_SIZE=${BZTREE_MAX_MERGED_SIZE}
    BZTREE_MAX_VARIABLE_DATA_SIZE=${BZTREE_MAX_VARIABLE_DATA_SIZE}
    $<$<BOOL:${BZTREE_HAS_SPINLOCK_HINT}>:BZTREE_HAS_SPINLOCK_HINT>
  )
  target_include_directories(${PROJECT_NAME} INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_link_libraries(${PROJECT_NAME} INTERFACE
    dbgroup::mwcas
    dbgroup::memory_manager
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
