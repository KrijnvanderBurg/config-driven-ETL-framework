ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim-bullseye AS BASE

ENV \
    # Java
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/ 

RUN apt-get update && \
    apt-get install -y \
    curl \
    git \
    openjdk-11-jdk && \
    apt-get clean

WORKDIR /repo

COPY . .