FROM python:2

MAINTAINER Francis Gassert <fgassert@gmail.com>

RUN mkdir -p /usr/src
WORKDIR /usr/src

RUN apt-get update
RUN apt-get install -y libhdf5-dev gdal-bin libgdal-dev
RUN pip install --no-cache-dir numpy
RUN pip install --no-cache-dir netcdf4 boto3 rasterio pandas

COPY ./src /usr/src
VOLUME /usr/src

CMD ["python", "./process.py"]
