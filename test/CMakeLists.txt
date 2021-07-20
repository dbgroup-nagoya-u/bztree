#--------------------------------------------------------------------------------------#
# Configure GoogleTest
#--------------------------------------------------------------------------------------#

configure_file(CMakeLists-googletest.txt.in googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
                 ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
                 EXCLUDE_FROM_ALL)

#--------------------------------------------------------------------------------------#
# Build Unit Tests
#--------------------------------------------------------------------------------------#

# define function to add unit tests in the same format
function(ADD_BZTREE_TEST BZTREE_TEST_TARGET)
  add_executable(
    ${BZTREE_TEST_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/${BZTREE_TEST_TARGET}.cpp"
  )
  target_compile_features(
    ${BZTREE_TEST_TARGET} PRIVATE
    "cxx_std_17"
  )
  target_compile_options(
    ${BZTREE_TEST_TARGET} PRIVATE
    -Wall
    -Wextra
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_include_directories(
    ${BZTREE_TEST_TARGET} PRIVATE
    "${BZTREE_SOURCE_DIR}/include"
  )
  target_link_libraries(
    ${BZTREE_TEST_TARGET} PRIVATE
    pthread
    mwcas
    memory_manager
    gtest_main
  )

  if(DEFINED BZTREE_PAGE_SIZE)
    target_compile_definitions(
      ${BZTREE_TEST_TARGET} PRIVATE
      BZTREE_PAGE_SIZE=${BZTREE_PAGE_SIZE}
    )
  endif()

  if(DEFINED BZTREE_MAX_UNSORTED_REC_NUM)
    target_compile_definitions(
      ${BZTREE_TEST_TARGET} PRIVATE
      BZTREE_MAX_UNSORTED_REC_NUM=${BZTREE_MAX_UNSORTED_REC_NUM}
    )
  endif()

  if(DEFINED BZTREE_MAX_DELETED_SPACE_SIZ)
    target_compile_definitions(
      ${BZTREE_TEST_TARGET} PRIVATE
      BZTREE_MAX_DELETED_SPACE_SIZE=${BZTREE_MAX_DELETED_SPACE_SIZE}
    )
  endif()

  if(DEFINED BZTREE_MIN_FREE_SPACE_SIZE)
    target_compile_definitions(
      ${BZTREE_TEST_TARGET} PRIVATE
      BZTREE_MIN_FREE_SPACE_SIZE=${BZTREE_MIN_FREE_SPACE_SIZE}
    )
  endif()

  if(DEFINED BZTREE_MIN_SORTED_REC_NUM)
    target_compile_definitions(
      ${BZTREE_TEST_TARGET} PRIVATE
      BZTREE_MIN_SORTED_REC_NUM=${BZTREE_MIN_SORTED_REC_NUM}
    )
  endif()

  if(DEFINED BZTREE_MAX_MERGED_SIZE)
    target_compile_definitions(
      ${BZTREE_TEST_TARGET} PRIVATE
      BZTREE_MAX_MERGED_SIZE=${BZTREE_MAX_MERGED_SIZE}
    )
  endif()

  if(DEFINED BZTREE_TEST_THREAD_NUM)
    target_compile_definitions(
      ${BZTREE_TEST_TARGET} PRIVATE
      BZTREE_TEST_THREAD_NUM=${BZTREE_TEST_THREAD_NUM}
    )
  endif()

  add_test(
    NAME ${BZTREE_TEST_TARGET}
    COMMAND $<TARGET_FILE:${BZTREE_TEST_TARGET}>
  )
endfunction()

# add unit tests to build targets
ADD_BZTREE_TEST(status_word_test)
ADD_BZTREE_TEST(metadata_test)
ADD_BZTREE_TEST(record_page_test)
ADD_BZTREE_TEST(base_node_test)
ADD_BZTREE_TEST(leaf_node_test)
ADD_BZTREE_TEST(leaf_node_multi_thread_test)
ADD_BZTREE_TEST(internal_node_test)
ADD_BZTREE_TEST(bztree_test)
ADD_BZTREE_TEST(bztree_multi_thread_test)
