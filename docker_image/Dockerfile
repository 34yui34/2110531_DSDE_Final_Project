FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install Python + Java
RUN apt-get update && \
    apt-get install -y python3 python3-pip openjdk-17-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Make python3 default
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# Dynamically detect JAVA_HOME
RUN JAVA_HOME_DETECTED=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "Detected JAVA_HOME=$JAVA_HOME_DETECTED" && \
    echo "JAVA_HOME=$JAVA_HOME_DETECTED" >> /etc/environment && \
    echo "PATH=$JAVA_HOME_DETECTED/bin:$PATH" >> /etc/environment && \
    export JAVA_HOME=$JAVA_HOME_DETECTED

# Set it for this Docker image
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /

COPY ./dataset ./dataset
COPY ./requirements.txt .
RUN pip install -r requirements.txt
