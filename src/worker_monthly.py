import boto3
import os
import numpy as np
import rasterio as rio
import urllib
import netCDF4 as nc

def monthlyMeans(inArr):
    t,h,w = inArr.shape
    arr = np.zeros((12,h,w), dtype=inArr.dtype)
    mo = t/12.0
    #months are 30 or 31 days
    for i in range(12):
        arr[i] = np.nanmean(inArr[int(mo*i):int(mo*(i+1))], axis=0)
    return arr

def monthlyMax(inArr):
    t,h,w = inArr.shape
    arr = np.zeros((12,h,w), dtype=inArr.dtype)
    mo = t/12.0
    #months are 30 or 31 days
    for i in range(12):
        arr[i] = np.nanmax(inArr[int(mo*i):int(mo*(i+1))], axis=0)
    return arr

def monthlyMin(inArr):
    t,h,w = inArr.shape
    arr = np.zeros((12,h,w), dtype=inArr.dtype)
    mo = t/12.0
    #months are 30 or 31 days
    for i in range(12):
        arr[i] = np.nanmin(inArr[int(mo*i):int(mo*(i+1))], axis=0)
    return arr

def processNc2Tiff(inNC, outTiff, function, var):
    with nc.Dataset(inNC) as src:
        arr = np.array(src.variables[var])
        if ("missing_value" in src.variables[var].ncattrs()):
            arr[arr==src.variables[var].missing_value] = np.nan

    dtype = arr.dtype
    arr = function(arr)

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

def worker(options):
    try:
        inUrl = options['inUrl']
        outBucket = options['outBucket']
        outKey = options['outKey']
        var = options['var']
        i = options['id']
        tmp1 = 'tmp_{}'.format(i)
        tmp2 = 'tmp2_{}'.format(i)

        print ("{} Fetching {}".format(i,inUrl))
        urllib.urlretrieve(inUrl, tmp1)

        print ("{} Processing {}".format(i,inUrl))
        f = {
            "pr":monthlyMeans,
            "tasmax":monthlyMax,
            "tasmin":monthlyMin
        }[var]

        processNc2Tiff(tmp1, tmp2, f, var)

        print ("{} Putting {}".format(i,outKey))

        s3 = boto3.client('s3')
        s3.upload_file(tmp2, outBucket, outKey, {"ACL": "public-read"})

        print ("{} Cleaning {}".format(i,outKey))
        os.remove(tmp1)
        os.remove(tmp2)

    except Exception as e:
        print (e, options)
        try:
            os.remove(tmp1)
            os.remove(tmp2)
        except:
            pass


if __name__ == "__main__":
    processNc2Tiff('pr_day_BCSD_rcp45_r1i1p1_CCSM4_2010.nc', 'out.tif', monthlyMeans, 'pr')
