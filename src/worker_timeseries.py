import boto3
import os
import numpy as np
import rasterio as rio
import urllib
import pandas as pd


def worker(options):
    try:
        inUrlTemplate = options['inUrlTemplate']
        years = options['years']
        outBucket = options['outBucket']
        outKey = options['outKey']
        var = options['var']
        idx = options['id']

        tmp2 = 'tmp2_{}'.format(idx)
        tmp1 = []

        for y in years:
            inUrl = inUrlTemplate.format(year=y)
            print ("{} Fetching {}".format(idx,inUrl))
            tmp1.append('tmp_{}_{}'.format(idx, y))
            urllib.urlretrieve(inUrl, tmp1[-1])

        print ("{} Processing {}".format(idx,inUrl))
        with rio.open(tmp1[0],'r') as peek:
            A = peek.affine
            arr = np.zeros((peek.shape[0]*peek.shape[1], 2+len(years)), dtype=peek.dtypes[0])
            coords = A*np.array([(j,i) for i in range(peek.shape[0]) for j in range(peek.shape[1])]).T
            arr[:,0] = coords[1]
            arr[:,1] = coords[0]
            arr[:,2] = peek.read().flatten()
            columns = ['lat','lng'] + list(years)
            for i in range(1,len(years)):
                try:
                    with rio.open(tmp1[i],'r') as src:
                        arr[:,2+i] = src.read().flatten()
                except:
                    print "{} Empty raster {}".format(idx, tmp1[i])
                    arr[:,2+i] = np.nan
            df = pd.DataFrame(arr, columns=columns, dtype=arr.dtype)
            df.to_csv(tmp2, index=False)

        print ("{} Putting {}".format(idx,outKey))

        s3 = boto3.client('s3')
        s3.upload_file(tmp2, outBucket, outKey, {"ACL": "public-read"})

        print ("{} Cleaning {}".format(idx,outKey))
        for f in tmp1:
            os.remove(f)
        os.remove(tmp2)

    except Exception as e:
        print (e, options)
        try:
            for f in tmp1:
                os.remove(f)
            os.remove(tmp2)
        except:
            pass
