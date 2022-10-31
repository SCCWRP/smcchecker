#FROM ubuntu:latest
FROM debian:sid
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update 
RUN apt-get install -y apt-utils
RUN apt-get install -y python3-pip python3-dev build-essential libssl-dev libffi-dev
RUN apt-get install -y libpcre3 libpcre3-dev
RUN apt-get install -y vim
RUN apt-get install -y libpq-dev && pip3 install psycopg2
RUN apt-get install -y libgdal-dev
RUN apt-get install -y libproj-dev
RUN apt-get install -y cython3
RUN apt-get install -y python3-pyproj
RUN pip3 install wheel
RUN pip3 install uwsgi flask ipython
RUN pip3 install xlrd flask_session
RUN pip3 install flask_cors
RUN pip3 install Werkzeug
RUN pip3 install sqlalchemy
RUN pip3 install xlsxwriter
RUN pip3 install openpyxl
RUN pip3 install numpy 
RUN pip3 install pandas 
RUN pip3 install postgis
ENV PROJ_DIR=/usr/
RUN pip3 install fiona
RUN pip3 install shapely
ENV PROJ_LIBDIR=/usr/lib
ENV PROJ_INCDIR=/usr/lib
RUN pip3 install geopandas 
RUN pip3 install folium 
RUN pip3 install matplotlib 
RUN apt-get update
RUN apt-get install -y libc6-dev
RUN apt-get install -y libkrb5-dev
RUN pip3 install arcgis 
RUN apt-get install -y mdbtools
RUN pip3 install pandas_access

#RUN pip3 install opencv-python

# Install what we need for the analysis indices
RUN apt-get install -y r-base
RUN apt-get install -y git
WORKDIR /tmp
RUN git clone https://github.com/SCCWRP/CSCI
RUN git clone https://github.com/SCCWRP/ASCI
RUN git clone https://github.com/SCCWRP/PHAB
RUN git clone https://github.com/SCCWRP/PHABMetrics
RUN git clone https://github.com/SCCWRP/BMIMetrics

# Install dependencies
# CSCI dependencies
RUN Rscript -e 'install.packages(c("randomForest", "reshape2", "ggplot2", "mapproj", "maps", "plyr", "stringr", "vegan"))'

# ASCI dependencies (minus the ones already installed for CSCI)
RUN Rscript -e 'install.packages(c("dplyr", "magrittr", "purrr", "quantregForest",  "tibble", "tidyr", "glue"))'

# PHABMetrics dependencies (minus the ones installed for the previous packages)
RUN Rscript -e 'install.packages("reshape")'

# IPI (aka PHAB) dependencies (minus the ones installed for the previous packages)
# All dependencies are already installed

# BMIMetrics is a dependency for CSCI so it must be installed before CSCI
RUN R CMD INSTALL --no-multiarch --with-keep.source BMIMetrics
RUN R CMD INSTALL --no-multiarch --with-keep.source CSCI
RUN R CMD INSTALL --no-multiarch --with-keep.source ASCI
RUN R CMD INSTALL --no-multiarch --with-keep.source PHABMetrics
RUN R CMD INSTALL --no-multiarch --with-keep.source PHAB


RUN mkdir -p /var/www/checker/
WORKDIR /var/www/checker/

# this Dockerfile is for the SMC checker, since there are certain libraries that are unique to this checking system
# sudo docker image build -t sccwrp/flask:smcchecker .

# example deployment script
# docker container run -it -d --name $1 \
#     -v /tmp:/tmp -v /var/www/templates/$1:/var/www/$1 \
#     -v /etc/timezone:/etc/timezone:ro \
#     -v /etc/localtime:/etc/localtime:ro \
#     -w /var/www/checker \
#     -e DB_CONNECTION_STRING='$2' \
#     sccwrp/flask:checkertemplate \
#     uwsgi -s /tmp/$1.sock --uid www-data --gid www-data --manage-script-name --mount /checker=run:app --chmod-socket=666
