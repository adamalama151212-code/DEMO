# =============================================================================
# Obraz do lokalnego uruchamiania radplume (Spark 4 + Delta Lake + Java 17).
#
# Dlaczego Docker: Spark na Windowsie wymaga winutils.exe/hadoop.dll i ręcznej
# konfiguracji HADOOP_HOME — źródło godzin frustracji. W kontenerze Linux
# wszystko działa tak samo na Windows, macOS i Linux, i tak samo jak w CI.
# =============================================================================
FROM python:3.11-slim-bookworm

# Java 17: minimalna wersja wymagana przez Spark 4 (Databricks Runtime 17 też używa 17).
# procps: Spark używa `ps` przy zamykaniu procesów.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# TZ=UTC: te same godziny co na klastrze (patrz radplume/session.py).
ENV PYTHONUNBUFFERED=1 \
    TZ=UTC \
    RADPLUME_ENV=local

WORKDIR /app

# Najpierw tylko pliki potrzebne do instalacji — Docker cache'uje tę warstwę
# i przy zmianie kodu nie instaluje zależności od nowa.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[dev]"

# Pobranie jarów Delta Lake do obrazu (cache Ivy). Bez tego pierwsze uruchomienie
# każdego kontenera ściągałoby je z Maven Central.
RUN python -c "from radplume.config import load_config; from radplume.session import get_spark; get_spark(load_config('local')).stop()"

COPY tests ./tests

CMD ["radplume", "--help"]
