# Ubuntu 20.04 LTS
# Python 3.8.10
# GDAL 3.0.4
FROM rocker/geospatial

RUN export DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install -y \
  software-properties-common \
  dirmngr \
  apt-transport-https \
  apt-utils \
  gnupg \
  gnupg2 \
  gnupg1 \
  wget \
  python3 \
  python3-pip


RUN R -e 'install.packages(c("sf", "geojsonsf", "raster", "rgdal", "tidyverse", "lubridate", "lwgeom", "here"))'

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV TZ=America/New_York

RUN mkdir -pv /shellcast
COPY . /shellcast
COPY ./requirements.txt /shellcast
#COPY install_packages.R /app
RUN pip3 install -r /shellcast/requirements.txt

