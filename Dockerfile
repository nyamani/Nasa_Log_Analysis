FROM python:3.6-alpine3.10

ARG SPARK_VERSION=3.0.3
ARG HADOOP_VERSION_SHORT=3.2
ARG HADOOP_VERSION=3.2.0


RUN apk add --no-cache bash openjdk8-jre && \
  apk add --no-cache libc6-compat && \
  ln -s /lib/libc.musl-x86_64.so.1 /lib/ld-linux-x86-64.so.2 && \
  pip install findspark

# Download and extract Spark
RUN wget -qO- https://www-eu.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SHORT}.tgz | tar zx -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SHORT} /opt/spark

ENV PATH="/opt/spark/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:${PYTHONPATH}"
# Define default command

RUN mkdir $SPARK_HOME/conf
RUN echo "SPARK_LOCAL_IP=127.0.0.1" > $SPARK_HOME/conf/spark-env.sh

EXPOSE 4040

# Install pip requirements
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

RUN mkdir /log_dir

#Copy python script for batch
COPY . /app/


# Downlaod Nasa logfile and copy it into input_dir or landing dir.
WORKDIR /app/src/input_dir/ 
RUN wget ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz

# Commenting to provide the flexibility to run Pytest test cases interactive.
#WORKDIR /app
#RUN python -m pytest /app/tests/test_nasa_log.py | tee test_results.log

WORKDIR /root
# Running test cases using Pytest

# Commenting to provide the flexibility to submit spark job on interactive shell.
# Define Spark Submit command and run
#RUN spark-submit --num-executors 1 --executor-cores 2 --executor-memory 1g --driver-cores 1 --driver-memory 1g  --properties-file /app/configs/nasa_log_conf.properties /app/src/nasa_log_analysis.py

# Define default command
CMD ["/bin/bash"]