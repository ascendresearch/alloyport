ARG CANN_IMAGE
FROM ${CANN_IMAGE}

LABEL org.alloyport.runner="ascend-reduction-correctness-v1"

# What this image can compile, stated by the image instead of probed by whoever needs to know.
#
# The absence of these three facts cost a migration. The task prompt listed `kernel_operator.h`'s
# directory and a `ccec` binary, every path real and individually probed, and invited the obvious
# conclusion that a compiler invocation pointed at them would work. It cannot: `kernel_operator.h`
# includes `kernel_tpipe.h` from a different top-level tree. CANN's CMake language package owns that
# include set, and nothing in the image said so.
LABEL org.alloyport.toolchain.cann_version="9.1.0-beta.1"
LABEL org.alloyport.toolchain.asc_cmake_package="find_package(ASC REQUIRED) before project(<name> LANGUAGES ASC CXX)"
LABEL org.alloyport.toolchain.asc_source_extension=".asc"
LABEL org.alloyport.toolchain.asc_arch_flag="$<$<COMPILE_LANGUAGE:ASC>:--npu-arch=dav-3510>"
LABEL org.alloyport.toolchain.device_compiler="bisheng, selected by the ASC language package; a hand-written command line with a single -I cannot resolve kernel_operator.h's own includes"
LABEL org.alloyport.toolchain.host_acl_headers="${ASCEND_HOME_PATH}/x86_64-linux/include"
LABEL org.alloyport.toolchain.verified_by="fixtures/ascend-add-v1 compiled and linked in this image with no accelerator attached, 2026-08-17"

ENV ASCEND_TOOLKIT_HOME=/usr/local/Ascend/ascend-toolkit/latest \
    ASCEND_OPP_PATH=/usr/local/Ascend/ascend-toolkit/latest/opp \
    ASCEND_AICPU_PATH=/usr/local/Ascend/ascend-toolkit/latest \
    TOOLCHAIN_HOME=/usr/local/Ascend/ascend-toolkit/latest/toolkit \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
# The base image already puts `ccec`, `bisheng`, and `bishengir-compile` on PATH via
# `${ASCEND_TOOLKIT_HOME}/bin`, and the compiler bin directory at `tools/ccec_compiler/bin`. An
# earlier revision added `compiler/ccec_compiler/bin`, which does not exist in this CANN layout.
ENV PATH=${ASCEND_TOOLKIT_HOME}/bin:${PATH}
# The driver directories are empty in a build container and populated when the correctness role
# attaches a device. One image serves both roles, so they stay.
ENV LD_LIBRARY_PATH=${ASCEND_TOOLKIT_HOME}/lib64:${ASCEND_TOOLKIT_HOME}/lib64/plugin/opskernel:${ASCEND_TOOLKIT_HOME}/lib64/plugin/nnengine:/usr/local/Ascend/driver/lib64:/usr/local/Ascend/driver/lib64/common:/usr/local/Ascend/driver/lib64/driver:${LD_LIBRARY_PATH}

# Deliberately no COPY. A trusted runner image must not carry content a run could reach outside the
# artifact chain, and `trusted_images_declare_the_complete_runner_toolchain` enforces that. The
# contract above is labels for exactly this reason: `docker inspect` answers without running the
# image, and nothing a candidate builds can read them.

ENTRYPOINT ["python3"]
