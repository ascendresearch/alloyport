#include "kernel_operator.h"
#include "add_custom_tiling.h"

class AlloyPortAddKernel {
public:
    __aicore__ inline explicit AlloyPortAddKernel(AscendC::TPipe* pipe) : pipe_(pipe) {}

    __aicore__ inline void Init(GM_ADDR x, GM_ADDR y, GM_ADDR z,
                                const __gm__ AddTilingData* tiling)
    {
        const uint32_t block = AscendC::GetBlockIdx();
        elements_ = block + 1 == tiling->block_count ? tiling->elements_last_core
                                                     : tiling->elements_per_core;
        const uint32_t offset = block * tiling->elements_per_core;
        x_.SetGlobalBuffer((__gm__ float*)x + offset, elements_);
        y_.SetGlobalBuffer((__gm__ float*)y + offset, elements_);
        z_.SetGlobalBuffer((__gm__ float*)z + offset, elements_);
        pipe_->InitBuffer(x_queue_, ALLOYPORT_BUFFER_COUNT,
                          ALLOYPORT_TILE_ELEMENTS * sizeof(float));
        pipe_->InitBuffer(y_queue_, ALLOYPORT_BUFFER_COUNT,
                          ALLOYPORT_TILE_ELEMENTS * sizeof(float));
        pipe_->InitBuffer(z_queue_, ALLOYPORT_BUFFER_COUNT,
                          ALLOYPORT_TILE_ELEMENTS * sizeof(float));
    }

    __aicore__ inline void Process()
    {
        const uint32_t tiles = (elements_ + ALLOYPORT_TILE_ELEMENTS - 1) /
                               ALLOYPORT_TILE_ELEMENTS;
        for (uint32_t tile = 0; tile < tiles; ++tile) {
            const uint32_t offset = tile * ALLOYPORT_TILE_ELEMENTS;
            const uint32_t remaining = elements_ - offset;
            const uint32_t count = remaining < ALLOYPORT_TILE_ELEMENTS ? remaining
                                                                       : ALLOYPORT_TILE_ELEMENTS;
            CopyIn(offset, count);
            Compute(count);
            CopyOut(offset, count);
        }
    }

private:
    __aicore__ inline void CopyIn(uint32_t offset, uint32_t count)
    {
        auto x = x_queue_.AllocTensor<float>();
        auto y = y_queue_.AllocTensor<float>();
        AscendC::DataCopyPad(x, x_[offset],
                            {1, static_cast<uint16_t>(count * sizeof(float)), 0, 0},
                            {false, 0, 0, 0});
        AscendC::DataCopyPad(y, y_[offset],
                            {1, static_cast<uint16_t>(count * sizeof(float)), 0, 0},
                            {false, 0, 0, 0});
        x_queue_.EnQue(x);
        y_queue_.EnQue(y);
    }

    __aicore__ inline void Compute(uint32_t count)
    {
        auto x = x_queue_.DeQue<float>();
        auto y = y_queue_.DeQue<float>();
        auto z = z_queue_.AllocTensor<float>();
        AscendC::Add(z, x, y, count);
        z_queue_.EnQue(z);
        x_queue_.FreeTensor(x);
        y_queue_.FreeTensor(y);
    }

    __aicore__ inline void CopyOut(uint32_t offset, uint32_t count)
    {
        auto z = z_queue_.DeQue<float>();
        AscendC::DataCopyPad(z_[offset], z,
                            {1, static_cast<uint16_t>(count * sizeof(float)), 0, 0});
        z_queue_.FreeTensor(z);
    }

    AscendC::TPipe* pipe_;
    AscendC::GlobalTensor<float> x_;
    AscendC::GlobalTensor<float> y_;
    AscendC::GlobalTensor<float> z_;
    AscendC::TQue<AscendC::TPosition::VECIN, 1> x_queue_;
    AscendC::TQue<AscendC::TPosition::VECIN, 1> y_queue_;
    AscendC::TQue<AscendC::TPosition::VECOUT, 1> z_queue_;
    uint32_t elements_ = 0;
};

extern "C" __global__ __vector__ void add_custom_kernel(GM_ADDR x, GM_ADDR y, GM_ADDR z,
                                                          GM_ADDR tiling_address)
{
    AscendC::TPipe pipe;
    AlloyPortAddKernel kernel(&pipe);
    kernel.Init(x, y, z, (__gm__ AddTilingData*)tiling_address);
    kernel.Process();
}
