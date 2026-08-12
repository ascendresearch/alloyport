#include "reduce_sum.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iterator>
#include <vector>

namespace {

uint32_t next_random(uint32_t *state) {
    *state = *state * 1664525U + 1013904223U;
    return *state;
}

std::vector<float> make_input(size_t elements, uint32_t seed) {
    std::vector<float> input(elements);
    for (float &value : input) {
        const int32_t centered = static_cast<int32_t>(next_random(&seed) >> 8U) - (1 << 23);
        value = static_cast<float>(centered) / static_cast<float>(1 << 20);
    }
    return input;
}

double reference_sum(const std::vector<float> &input) {
    double sum = 0.0;
    double correction = 0.0;
    for (float value : input) {
        const double adjusted = static_cast<double>(value) - correction;
        const double next = sum + adjusted;
        correction = (next - sum) - adjusted;
        sum = next;
    }
    return sum;
}

uint64_t hash_input(uint64_t hash, const std::vector<float> &input) {
    constexpr uint64_t kFnvPrime = 1099511628211ULL;
    for (float value : input) {
        uint32_t bits = 0;
        static_assert(sizeof(bits) == sizeof(value));
        std::memcpy(&bits, &value, sizeof(bits));
        for (unsigned int shift = 0; shift < 32; shift += 8) {
            hash ^= (bits >> shift) & 0xffU;
            hash *= kFnvPrime;
        }
    }
    return hash;
}

bool run_case(size_t elements, uint32_t seed, uint64_t *checksum) {
    const std::vector<float> input = make_input(elements, seed);
    float actual = -1.0F;
    const float *data = input.empty() ? nullptr : input.data();
    const int status = alloyport_reduce_sum_f32(data, input.size(), &actual);
    if (status != ALLOYPORT_REDUCE_OK) {
        std::fprintf(stderr, "case elements=%zu returned status=%d\n", elements, status);
        return false;
    }

    const double expected = reference_sum(input);
    const double error = std::abs(static_cast<double>(actual) - expected);
    const double limit = std::max(1.0e-4, std::abs(expected) * 2.0e-5);
    if (!std::isfinite(actual) || error > limit) {
        std::fprintf(
            stderr,
            "case elements=%zu expected=%.9g actual=%.9g error=%.9g limit=%.9g\n",
            elements,
            expected,
            static_cast<double>(actual),
            error,
            limit);
        return false;
    }
    *checksum = hash_input(*checksum, input);
    return true;
}

}  // namespace

int main(int argc, char **argv) {
    if (argc != 3 || std::strcmp(argv[1], "--case-set") != 0
        || std::strcmp(argv[2], "release") != 0) {
        std::fprintf(stderr, "usage: reduction_reference --case-set release\n");
        return 2;
    }

    const size_t cases[] = {0, 1, 3, 255, 256, 257, 4097, 65536, 1048576};
    uint64_t checksum = 1469598103934665603ULL;
    for (size_t index = 0; index < std::size(cases); ++index) {
        if (!run_case(cases[index], 0x5eed1234U + static_cast<uint32_t>(index), &checksum)) {
            return 1;
        }
    }

    std::printf(
        "PASS fixture=cuda-reduction-v1 cases=%zu input_checksum=%016llx\n",
        std::size(cases),
        static_cast<unsigned long long>(checksum));
    return 0;
}
