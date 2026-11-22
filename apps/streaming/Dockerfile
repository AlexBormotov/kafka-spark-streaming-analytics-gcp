# Build context: repository root (the uv workspace lock lives there).
FROM apache/spark:4.0.5-python3
COPY --from=ghcr.io/astral-sh/uv:0.9.24 /uv /bin/uv

USER root
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; \
    cd /opt/spark/jars; \
    for jar in \
        org/apache/spark/spark-avro_2.13/4.0.5/spark-avro_2.13-4.0.5.jar \
        org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.5/spark-sql-kafka-0-10_2.13-4.0.5.jar \
        org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.5/spark-token-provider-kafka-0-10_2.13-4.0.5.jar \
        org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
        org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar; \
    do curl -fsSLO "$MAVEN/$jar"; done

WORKDIR /opt/qoe
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_INSTALL_DIR=/opt/qoe/python \
    PYSPARK_PYTHON=/opt/qoe/.venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/qoe/.venv/bin/python

COPY pyproject.toml uv.lock ./
COPY apps/generator/pyproject.toml apps/generator/pyproject.toml
COPY apps/streaming apps/streaming
RUN uv sync --frozen --no-dev --package qoe-streaming-job --no-editable \
    && cp apps/streaming/src/qoe_streaming/job.py /opt/qoe/job.py \
    && chown -R 185:185 /opt/qoe

USER 185
