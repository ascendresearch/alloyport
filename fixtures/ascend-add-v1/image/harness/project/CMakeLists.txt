cmake_minimum_required(VERSION 3.16)
find_package(ASC REQUIRED)
project(alloyport_ascend_add_fixture LANGUAGES ASC CXX)
set(CMAKE_CXX_STANDARD 17)

add_executable(ascend_add_fixture host.asc)
target_include_directories(ascend_add_fixture PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_options(
  ascend_add_fixture PRIVATE $<$<COMPILE_LANGUAGE:ASC>:--npu-arch=dav-3510>
)
target_link_libraries(
  ascend_add_fixture PRIVATE tiling_api register platform unified_dlog dl m graph_base
)
