#include <cuda_runtime.h>

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <vector>

#define CUDA_CHECK(call)                                                                        \
    do {                                                                                        \
        cudaError_t error = (call);                                                             \
        if (error != cudaSuccess) {                                                             \
            std::fprintf(stderr, "%s failed: %s\n", #call, cudaGetErrorString(error));          \
            return 2;                                                                           \
        }                                                                                       \
    } while (false)

__global__ void vector_add(const float* left, const float* right, float* output, int count) {
    const int index = blockDim.x * blockIdx.x + threadIdx.x;
    if (index < count) {
        output[index] = left[index] + right[index];
    }
}

int main() {
    constexpr int count = 1 << 20;
    constexpr int threads = 256;
    const std::size_t bytes = static_cast<std::size_t>(count) * sizeof(float);
    std::vector<float> left(count);
    std::vector<float> right(count);
    std::vector<float> output(count);
    for (int index = 0; index < count; ++index) {
        left[index] = static_cast<float>(index % 1024);
        right[index] = static_cast<float>(index % 257);
    }

    float* device_left = nullptr;
    float* device_right = nullptr;
    float* device_output = nullptr;
    CUDA_CHECK(cudaMalloc(&device_left, bytes));
    CUDA_CHECK(cudaMalloc(&device_right, bytes));
    CUDA_CHECK(cudaMalloc(&device_output, bytes));
    CUDA_CHECK(cudaMemcpy(device_left, left.data(), bytes, cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(device_right, right.data(), bytes, cudaMemcpyHostToDevice));

    const int blocks = (count + threads - 1) / threads;
    vector_add<<<blocks, threads>>>(device_left, device_right, device_output, count);
    CUDA_CHECK(cudaGetLastError());
    CUDA_CHECK(cudaDeviceSynchronize());
    CUDA_CHECK(cudaMemcpy(output.data(), device_output, bytes, cudaMemcpyDeviceToHost));

    double checksum = 0.0;
    for (int index = 0; index < count; ++index) {
        const float expected = left[index] + right[index];
        if (output[index] != expected) {
            std::fprintf(stderr,
                         "verification failed at %d: got %.9g expected %.9g\n",
                         index,
                         output[index],
                         expected);
            return 3;
        }
        checksum += static_cast<double>(output[index]);
    }

    CUDA_CHECK(cudaFree(device_left));
    CUDA_CHECK(cudaFree(device_right));
    CUDA_CHECK(cudaFree(device_output));
    std::printf("PASS fixture=cuda-vectoradd-v1 elements=%d checksum=%.0f\n", count, checksum);
    return 0;
}
