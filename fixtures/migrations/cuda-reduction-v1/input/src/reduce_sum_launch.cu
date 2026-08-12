#include "reduce_sum.h"

#include <cuda_runtime.h>

#include <algorithm>
#include <cstddef>

extern "C" __global__ void alloyport_reduce_sum_blocks(
    const float *input,
    size_t elements,
    float *output);

namespace {

constexpr size_t kMaximumElements = 1U << 20U;
constexpr unsigned int kThreads = 256;
constexpr unsigned int kMaximumBlocks = 256;

bool succeeded(cudaError_t status) {
    return status == cudaSuccess;
}

}  // namespace

extern "C" int alloyport_reduce_sum_f32(
    const float *input,
    size_t elements,
    float *output) {
    if (output == nullptr || (elements != 0 && input == nullptr)) {
        return ALLOYPORT_REDUCE_INVALID_ARGUMENT;
    }
    if (elements > kMaximumElements) {
        return ALLOYPORT_REDUCE_UNSUPPORTED;
    }
    if (elements == 0) {
        *output = 0.0F;
        return ALLOYPORT_REDUCE_OK;
    }

    float *device_input = nullptr;
    float *device_output = nullptr;
    const size_t input_bytes = elements * sizeof(float);
    if (!succeeded(cudaMalloc(&device_input, input_bytes))
        || !succeeded(cudaMalloc(&device_output, sizeof(float)))) {
        cudaFree(device_input);
        cudaFree(device_output);
        return ALLOYPORT_REDUCE_RUNTIME_ERROR;
    }

    const unsigned int blocks = std::min(
        kMaximumBlocks,
        static_cast<unsigned int>((elements + kThreads - 1) / kThreads));
    cudaError_t status = cudaMemcpy(device_input, input, input_bytes, cudaMemcpyHostToDevice);
    if (succeeded(status)) {
        status = cudaMemset(device_output, 0, sizeof(float));
    }
    if (succeeded(status)) {
        alloyport_reduce_sum_blocks<<<blocks, kThreads, kThreads * sizeof(float)>>>(
            device_input,
            elements,
            device_output);
        status = cudaGetLastError();
    }
    if (succeeded(status)) {
        status = cudaDeviceSynchronize();
    }
    if (succeeded(status)) {
        status = cudaMemcpy(output, device_output, sizeof(float), cudaMemcpyDeviceToHost);
    }

    const cudaError_t input_free = cudaFree(device_input);
    const cudaError_t output_free = cudaFree(device_output);
    return succeeded(status) && succeeded(input_free) && succeeded(output_free)
        ? ALLOYPORT_REDUCE_OK
        : ALLOYPORT_REDUCE_RUNTIME_ERROR;
}

