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
# Configurations for testing
#--------------------------------------------------------------------------------------#

set(BZTREE_TEST_THREAD_NUM "8" CACHE STRING "The maximum number of threads unit testing.")
set(BZTREE_TEST_RANDOM_SEED "10" CACHE STRING "A fixed seed value to reproduce unit tests.")
set(BZTREE_TEST_EXEC_NUM "1E5" CACHE STRING "The number of executions per a thread.")
option(BZTREE_TEST_OVERRIDE_MIMALLOC "Override entire memory allocation with mimalloc" OFF)

#--------------------------------------------------------------------------------------#
# Build Unit Tests
#--------------------------------------------------------------------------------------#

if(${BZTREE_TEST_OVERRIDE_MIMALLOC})
  find_package(mimalloc 1.7 REQUIRED)
endif()

# define function to add unit tests in the same format
function(ADD_BZTREE_TEST BZTREE_TEST_TARGET)
  add_executable(${BZTREE_TEST_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/${BZTREE_TEST_TARGET}.cpp"
  )
  target_compile_features(${BZTREE_TEST_TARGET} PRIVATE
    "cxx_std_17"
  )
  target_compile_options(${BZTREE_TEST_TARGET} PRIVATE
    -Wall
    -Wextra
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_link_libraries(${BZTREE_TEST_TARGET} PRIVATE
    bztree
    gtest_main
    $<$<BOOL:${BZTREE_TEST_OVERRIDE_MIMALLOC}>:mimalloc-static>
  )
  target_compile_definitions(${BZTREE_TEST_TARGET} PRIVATE
    BZTREE_TEST_THREAD_NUM=${BZTREE_TEST_THREAD_NUM}
    BZTREE_TEST_RANDOM_SEED=${BZTREE_TEST_RANDOM_SEED}
    INDEX_FIXTURE_THREAD_NUM=${BZTREE_TEST_THREAD_NUM}
    INDEX_FIXTURE_RANDOM_SEED=${BZTREE_TEST_RANDOM_SEED}
    INDEX_FIXTURE_EXEC_NUM_PER_THREAD=${BZTREE_TEST_EXEC_NUM}
  )

  add_test(
    NAME ${BZTREE_TEST_TARGET}
    COMMAND $<TARGET_FILE:${BZTREE_TEST_TARGET}>
  )
endfunction()

# add unit tests to build targets
# ADD_BZTREE_TEST(status_word_test)
ADD_BZTREE_TEST(metadata_test)
ADD_BZTREE_TEST(node_test)
ADD_BZTREE_TEST(bztree_test)
ADD_BZTREE_TEST(bztree_multi_thread_test)
