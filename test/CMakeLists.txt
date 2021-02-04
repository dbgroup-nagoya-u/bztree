# define function to add unit tests in the same format
function(ADD_BZTREE_TEST BZTREE_TEST_TARGET)
  add_executable(${BZTREE_TEST_TARGET} "${CMAKE_CURRENT_SOURCE_DIR}/${BZTREE_TEST_TARGET}.cpp")
  target_include_directories(${BZTREE_TEST_TARGET} PRIVATE
    "${GOOGLETEST_SROUCE_DIR}/include"
  )
  target_link_libraries(${BZTREE_TEST_TARGET} PRIVATE
    bztree
    gtest_main
  )
  add_dependencies(${BZTREE_TEST_TARGET}
    bztree
    gtest_main
  )
endfunction()

# add unit tests to build targets
ADD_BZTREE_TEST(status_word_test)
ADD_BZTREE_TEST(metadata_test)
ADD_BZTREE_TEST(base_node_test)
ADD_BZTREE_TEST(leaf_node_test)
ADD_BZTREE_TEST(internal_node_test)
ADD_BZTREE_TEST(bztree_test)
