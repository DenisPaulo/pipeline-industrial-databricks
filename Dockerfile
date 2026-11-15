# Pipeline local: Python 3.12 (imagem oficial) e JDK Temurin 21 (imagem eclipse-temurin).
# O CI usa o Temurin 21. O comando de entrada é scripts/rodar_local.py.
FROM eclipse-temurin:21-jdk-jammy AS temurin

FROM python:3.12-slim-bookworm

COPY --from=temurin /opt/java/openjdk /opt/java/openjdk

ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}" \
    LANG=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PI_EXIGIR_PARIDADE=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 zlib1g \
    && rm -rf /var/lib/apt/lists/* \
    && java -version

WORKDIR /app

COPY requirements-local.txt .
RUN pip install --no-cache-dir -r requirements-local.txt

COPY dados ./dados
COPY dashboards ./dashboards
COPY notebooks ./notebooks
COPY scripts ./scripts

CMD ["python", "scripts/rodar_local.py"]
