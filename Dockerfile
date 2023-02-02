#https://cloud.google.com/sql/docs/sqlserver/connect-docker
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONBUFFERED 1
ENV PATH /usr/local/bin:$PATH

RUN apt-get update
RUN apt install -y \
    software-properties-common \
    dirmngr \
    apt-transport-https \
    ca-certificates \
    gnupg \
    gnupg2 \
    gnupg1 \
    wget \
    curl \
    libeccodes0 \
    gdal-bin \
    libgdal-dev \
    python3-numpy \
    libffi7 \
    vim \
    python3-pip \
    gcc \
    gfortran \
    zlib1g-dev \
    mysql-client \
    && rm -rf /var/lib/apt/lists/*

ENV CC gcc
ENV FC gfortran
# Download wgrib2
RUN cd ~ \
    && wget ftp://ftp.cpc.ncep.noaa.gov/wd51we/wgrib2/wgrib2.tgz \
    && tar xvzf wgrib2.tgz
# Install wgrib2
RUN cd ~/grib2/ \
    && make \
    && cp wgrib2/wgrib2 /usr/local/bin/wgrib2

COPY . /analysis
COPY ./requirements.txt /tmp/requirements.txt
RUN pip3 install --upgrade pip && \
  pip3 install -r /tmp/requirements.txt
RUN rm -rf /tmp/requirements.txt
WORKDIR /analysis
CMD ["/bin/bash"]


