ARG PYTHON_VERSION=3.10
FROM python:${PYTHON_VERSION}-bullseye

# Install Java and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    wget \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Arguments for Spark setup
ARG SPARK_VERSION=3.5.5
ARG HADOOP_VERSION=3

# Set Spark environment variables
ENV SPARK_VERSION=${SPARK_VERSION}
ENV HADOOP_VERSION=${HADOOP_VERSION}
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Download and install Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install Python dependencies including Jupyter
RUN pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    jupyter \
    jupyterlab \
    ipykernel 

# Set up a Jupyter kernel with auto Spark initialization
RUN python -m ipykernel install --name pyspark --display-name "PySpark" && \
    mkdir -p /root/.ipython/profile_default/startup/

# Create startup script for auto Spark initialization
COPY spark_init.py /root/.ipython/profile_default/startup/00-spark-init.py

# Create a Jupyter kernel for PySpark
RUN ipython kernel install --user --name=pyspark --display-name='PySpark'

WORKDIR /workspace

CMD ["sleep", "infinity"]