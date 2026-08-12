#include <cuda_runtime.h>

#include <stddef.h>

extern "C" __global__ void alloyport_reduce_sum_blocks(
    const float *input,
    size_t elements,
    float *output) {
    extern __shared__ float partial[];

    float sum = 0.0F;
    for (size_t index = blockIdx.x * blockDim.x + threadIdx.x; index < elements;
         index += static_cast<size_t>(blockDim.x) * gridDim.x) {
        sum += input[index];
    }
    partial[threadIdx.x] = sum;
    __syncthreads();

    for (unsigned int stride = blockDim.x / 2; stride > 0; stride /= 2) {
        if (threadIdx.x < stride) {
            partial[threadIdx.x] += partial[threadIdx.x + stride];
        }
        __syncthreads();
    }

    if (threadIdx.x == 0) {
        atomicAdd(output, partial[0]);
    }
}

