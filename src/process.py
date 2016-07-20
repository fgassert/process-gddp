#!/usr/bin/env python

import netCDF4 as nc
import numpy as np
import rasterio as rio
import urllib
import boto3
import os
import multiprocessing as mp
import sys

urlTemplate = "http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/{scenario}/day/atmos/{variable}/r1i1p1/v1.0/{variable}_day_BCSD_{scenario}_r1i1p1_{model}_{year}.nc"
filenameTemplate = "{variable}_day_BCSD_{scenario}_r1i1p1_{model}_{year}.nc"
bucket = "md.cc"
acl = "public-read"
threads = 4

scenarios = ["historical","rcp85","rcp45"]
variables = ["pr","tasmax","tasmin"]
models =    ['ACCESS1-0',
              'BNU-ESM',
              'CCSM4',
              'CESM1-BGC',
              'CNRM-CM5',
              'CSIRO-Mk3-6-0',
              'CanESM2',
              'GFDL-CM3',
              'GFDL-ESM2G',
              'GFDL-ESM2M',
              'IPSL-CM5A-LR',
              'IPSL-CM5A-MR',
              'MIROC-ESM-CHEM',
              'MIROC-ESM',
              'MIROC5',
              'MPI-ESM-LR',
              'MPI-ESM-MR',
              'MRI-CGCM3',
              'NorESM1-M',
              'bcc-csm1-1',
              'inmcm4']

yearsHistorical = np.arange(1950,2006)
yearsFuture = np.arange(2006,2100)

def getUrl(scenario, variable, model, year):
    return urlTemplate.format(scenario=scenario, variable=variable, model=model, year=year)

def getFilename(scenario, variable, model, year):
    return filenameTemplate.format(scenario=scenario, variable=variable, model=model, year=year)

def monthlyMeans(inArr):
    t,h,w = inArr.shape
    arr = np.zeros((12,h,w), dtype=inArr.dtype)
    mo = t/12.0
    #months are 30 or 31 days
    for i in range(12):
        arr[i] = inArr[int(mo*i):int(mo*(i+1))].mean(axis=0)
    return arr

def monthlyMax(inArr):
    t,h,w = inArr.shape
    arr = np.zeros((12,h,w), dtype=inArr.dtype)
    mo = t/12.0
    #months are 30 or 31 days
    for i in range(12):
        arr[i] = inArr[int(mo*i):int(mo*(i+1))].maximum(axis=0)
    return arr

def monthlyMin(inArr):
    t,h,w = inArr.shape
    arr = np.zeros((12,h,w), dtype=inArr.dtype)
    mo = t/12.0
    #months are 30 or 31 days
    for i in range(12):
        arr[i] = inArr[int(mo*i):int(mo*(i+1))].minimum(axis=0)
    return arr

def processnc2tiff(inNC, outTiff, function):
    with nc.Dataset(inNC) as src:
        var = 'pr'
        inArr = src.variables[var]
        dtype = inArr.dtype
        arr = function(inArr)

        t,h,w = arr.shape
        #roll x-axis for 180W origin
        arr = np.roll(arr, w/2, axis=2)
        #set scale to .25 and origin to 90S,180W
        transform = rio.Affine(360.0/w,0,-180,0,180.0/h,-90)

        with rio.open(outTiff, 'w', 'GTiff',
                      width = w,
                      height = h,
                      count = t,
                      crs = "WGS84",
                      transform = transform,
                      dtype = dtype
        ) as dst:
            dst.write(arr.astype(dtype))

def worker(args):
    inUrl, tmpIn, function = args

    outName = tmpIn[:-3]+'.tif'

    print "Fetching {}".format(inUrl)
    urllib.urlretrieve(inUrl, tmpIn)

    print "Processing {}".format(outName)
    processnc2tiff(tmpIn, outName, function)

    s3 = boto3.client('s3')
    key = 'tmp/nex-gddp/{}'.format(outName)
    print "Putting {}".format(key)

    s3.put_object(ACL=acl, Bucket=bucket, Key=key, Body=outName)

    os.remove(tmpIn)
    os.remove(outName)

def main(threads = 1):
    tasks = []
    functions = {
        "pr":monthlyMeans,
        "tasmax":monthlyMax,
        "tasmin":monthlyMin
    }
    for v in variables:
        for m in models:
            for s in scenarios[:1]:
                for y in yearsHistorical:
                    tasks.append(
                        (
                            getUrl(s,v,m,y),
                            getFilename(s,v,m,y),
                            functions[v]
                        )
                    )
            for s in scenarios[1:]:
                for y in yearsFuture:
                    tasks.append(
                        (
                            getUrl(s,v,m,y),
                            getFilename(s,v,m,y),
                            functions[v]
                        )
                    )
    pool = mp.Pool(threads)
    pool.map(worker, tasks)

if __name__ == "__main__":
    threads = 1
    if len(sys.argv)>1:
        threads = int(sys.argv[1])
    main(threads)
