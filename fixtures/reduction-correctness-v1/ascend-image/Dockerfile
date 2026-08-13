ARG CANN_IMAGE
FROM ${CANN_IMAGE}

LABEL org.alloyport.runner="ascend-reduction-correctness-v1"

ENV ASCEND_TOOLKIT_HOME=/usr/local/Ascend/ascend-toolkit/latest \
    ASCEND_OPP_PATH=/usr/local/Ascend/ascend-toolkit/latest/opp \
    ASCEND_AICPU_PATH=/usr/local/Ascend/ascend-toolkit/latest \
    TOOLCHAIN_HOME=/usr/local/Ascend/ascend-toolkit/latest/toolkit \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
ENV PATH=${ASCEND_TOOLKIT_HOME}/bin:${ASCEND_TOOLKIT_HOME}/compiler/ccec_compiler/bin:${PATH}
ENV LD_LIBRARY_PATH=${ASCEND_TOOLKIT_HOME}/lib64:${ASCEND_TOOLKIT_HOME}/lib64/plugin/opskernel:${ASCEND_TOOLKIT_HOME}/lib64/plugin/nnengine:/usr/local/Ascend/driver/lib64:/usr/local/Ascend/driver/lib64/common:/usr/local/Ascend/driver/lib64/driver:${LD_LIBRARY_PATH}

ENTRYPOINT ["python3"]
