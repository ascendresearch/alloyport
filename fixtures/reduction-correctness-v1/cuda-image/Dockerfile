ARG CUDA_IMAGE
FROM ${CUDA_IMAGE}

LABEL org.alloyport.runner="cuda-reduction-correctness-v1"

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        cmake=3.28.3-1build7 \
        g++=4:13.2.0-7ubuntu1 \
        make=4.3-4.1build2 \
        python3=3.12.3-0ubuntu2.1 \
    && rm -rf /var/lib/apt/lists/*

ENV PATH=/usr/local/cuda/bin:${PATH} \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python3"]
