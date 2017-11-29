# docker build -t edxops/analytics-pipeline .

FROM edxops/python:2.7
ENV BOTO_CONFIG /dev/null

RUN apt-get update && \
    apt-get install --no-install-recommends -y \
        libatlas-base-dev \
        libblas-dev \
        liblapack-dev \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /edx/app/analytics-pipeline/requirements
COPY Makefile /edx/app/analytics-pipeline
COPY requirements /edx/app/analytics-pipeline/requirements

WORKDIR /edx/app/analytics-pipeline

RUN make test-requirements

VOLUME /edx/app/analytics-pipeline
