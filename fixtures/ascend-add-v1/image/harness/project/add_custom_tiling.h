#pragma once

#include <cstdint>

constexpr uint32_t ALLOYPORT_ELEMENTS = 16384;
constexpr uint32_t ALLOYPORT_TILE_ELEMENTS = 1024;
constexpr uint32_t ALLOYPORT_BUFFER_COUNT = 2;

struct AddTilingData {
    uint32_t block_count;
    uint32_t elements_per_core;
    uint32_t elements_last_core;
};
