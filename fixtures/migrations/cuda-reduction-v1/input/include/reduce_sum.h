#ifndef ALLOYPORT_REDUCE_SUM_H
#define ALLOYPORT_REDUCE_SUM_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

enum AlloyPortReduceStatus {
    ALLOYPORT_REDUCE_OK = 0,
    ALLOYPORT_REDUCE_INVALID_ARGUMENT = 1,
    ALLOYPORT_REDUCE_RUNTIME_ERROR = 2,
    ALLOYPORT_REDUCE_UNSUPPORTED = 3,
};

int alloyport_reduce_sum_f32(const float *input, size_t elements, float *output);

#ifdef __cplusplus
}
#endif

#endif

