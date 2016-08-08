import boto3
import os
import numpy as np
import rasterio as rio
import urllib

def bandMean(arr):
    return np.nanmean(arr,axis=0, keepdims=True)
def bandMin(arr):
    return np.nanmin(arr,axis=0, keepdims=True)
def bandMax(arr):
    return np.nanmax(arr,axis=0, keepdims=True)

def processTiff(inFile, outFile, f):
    with rio.open(inFile, 'r', 'GTiff') as src:
        profile = src.profile
        arr = f(src.read())
        profile['count'] = arr.shape[0]
        profile['height'] = arr.shape[1]
        profile['width'] = arr.shape[2]
        profile['transform'] = src.affine
        with rio.open(outFile, 'w', **profile) as dst:
            dst.write(arr)

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
            "pr":bandMean,
            "tasmax":bandMax,
            "tasmin":bandMin
        }[var]

        processTiff(tmp1, tmp2, f)

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
