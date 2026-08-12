cmake_minimum_required(VERSION 3.24)
project(alloyport_cuda_reduction LANGUAGES CXX CUDA)

add_library(reduce_sum STATIC
    src/reduce_sum_kernel.cu
    src/reduce_sum_launch.cu
)
target_include_directories(reduce_sum PUBLIC include)
target_compile_features(reduce_sum PUBLIC cxx_std_17)
set_target_properties(reduce_sum PROPERTIES
    CUDA_ARCHITECTURES native
    CUDA_SEPARABLE_COMPILATION ON
)

add_executable(reduction_reference tests/reference_main.cpp)
target_compile_features(reduction_reference PRIVATE cxx_std_17)
target_link_libraries(reduction_reference PRIVATE reduce_sum)

enable_testing()
add_test(NAME reduction_release COMMAND reduction_reference --case-set release)

