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

RUN mkdir -pv /shellcast
COPY . /shellcast
COPY ./requirements.txt /shellcast
#COPY install_packages.R /app
RUN pip3 install -r /shellcast/requirements.txt


# https://hub.docker.com/r/osgeo/gdal/dockerfile
# https://dev.to/ku6ryo/access-cloud-sql-from-docker-on-cloud-run-41b6
#FROM osgeo/gdal:ubuntu-small-latest
#
#RUN export DEBIAN_FRONTEND=noninteractive
#RUN apt-get update
#
#RUN apt-get install -y \
#  software-properties-common \
#  dirmngr \
#  apt-transport-https \
#  apt-utils \
#  gnupg \
#  gnupg2 \
#  gnupg1 \
#  wget
#
#
#RUN wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc
#RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/'
#RUN add-apt-repository ppa:ubuntugis/ppa
#RUN apt-get update
#
#
#RUN apt-get install -y \
#    python3 \
#    python3-pip \
#    r-base \
#    r-base-core \
#    r-recommended \
#    r-base-dev \
#    libgdal-dev \
#    libproj-dev \
#    sqlite3
#
#
##RUN R -e 'install.packages(c("sf", "geojsonsf", "raster", "rgdal", "tidyverse", "lubridate", "lwgeom", "here"))'
#
#ENV PYTHONDONTWRITEBYTECODE=1
#ENV PYTHONUNBUFFERED=1
#
#RUN mkdir -pv /app
#WORKDIR /app
#COPY . /app
##COPY requirements.txt /app
##COPY install_packages.R /app
#RUN pip3 install -r requirements.txt
#
#RUN mkdir -pv /usr/local/lib/R/site-library
#RUN chmod 777 /usr/local/lib/R/site-library
#RUN echo "export R_LIBS_USER=/usr/local/lib/R/site-library" >> /etc/bash.bashrc
#RUN Rscript install_packages.R
#
#CMD ["Rscript", "/analysis/ncdmf_tidy_lease_data_script.R]"]

