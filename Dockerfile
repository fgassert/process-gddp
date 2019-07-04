FROM python:3

MAINTAINER Francis Gassert <fgassert@gmail.com>

RUN mkdir -p /usr/src
WORKDIR /usr/src

RUN apt-get update
RUN apt-get install -y libhdf5-dev gdal-bin libgdal-dev
RUN pip install --no-cache-dir numpy
RUN pip install --no-cache-dir boto3 rasterio requests

COPY ./src /usr/src

CMD ["python ./main.py"]
