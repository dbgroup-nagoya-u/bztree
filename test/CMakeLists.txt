# define function to add unit tests in the same format
function(ADD_BZTREE_TEST BZTREE_TEST_TARGET)
  add_executable(${BZTREE_TEST_TARGET} "${CMAKE_CURRENT_SOURCE_DIR}/${BZTREE_TEST_TARGET}.cpp")
  target_compile_features(${BZTREE_TEST_TARGET} PRIVATE "cxx_std_20")
  target_compile_options(${BZTREE_TEST_TARGET} PRIVATE
    -Wall
    -Wextra
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_include_directories(${BZTREE_TEST_TARGET} PRIVATE
    "${BZTREE_SOURCE_DIR}/src"
    "${PMWCAS_SOURCE_DIR}"
    "${PMWCAS_SOURCE_DIR}/src"
    "${PMWCAS_SOURCE_DIR}/include"
    "${GOOGLETEST_SOURCE_DIR}/include"
  )
  target_link_libraries(${BZTREE_TEST_TARGET} PRIVATE
    pmwcas_static
    gtest_main
  )
  add_dependencies(${BZTREE_TEST_TARGET}
    pmwcas_static
    gtest_main
  )
  add_test(NAME ${BZTREE_TEST_TARGET} COMMAND $<TARGET_FILE:${BZTREE_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
ADD_BZTREE_TEST(status_word_test)
ADD_BZTREE_TEST(metadata_test)
ADD_BZTREE_TEST(base_node_test)
ADD_BZTREE_TEST(leaf_node_test)
ADD_BZTREE_TEST(internal_node_test)
ADD_BZTREE_TEST(bztree_test)
