import boto3
import os
import numpy as np
import rasterio as rio
import urllib

def worker(options):
    try:
        inUrlTemplate = options['inUrlTemplate']
        models = options['models']
        outBucket = options['outBucket']
        outKeyTemplate = options['outKeyTemplate']
        stats = options['stats']
        idx = options['id']

        tmp2 = []
        tmp1 = []

        for m in models:
            inUrl = inUrlTemplate.format(model=m)
            print ("{} Fetching {}".format(idx, inUrl))
            tmp1.append('tmp_{}_{}'.format(idx, m))
            urllib.urlretrieve(inUrl, tmp1[-1])

        print ("{} Processing {}".format(idx, inUrl))

        with rio.open(tmp1[0],'r','GTiff') as peek:
            h,w = peek.shape
            count = len(models)

            arr = np.zeros((count, h, w), dtype=peek.dtypes[0])
            arr[0] = peek.read()

            profile = peek.profile
            profile['transform'] = peek.affine

            for i in range(1,len(models)):
                try:
                    with rio.open(tmp1[i],'r','GTiff') as src:
                        arr[i] = src.read()
                except:
                    print "{} Empty raster {}".format(idx, tmp1[i])
                    arr[i] = np.nan

            out = {
                "q25":np.nanpercentile(arr, 25, axis=0, keepdims=True),
                "q50":np.nanpercentile(arr, 50, axis=0, keepdims=True),
                "q75":np.nanpercentile(arr, 75, axis=0, keepdims=True),
                "mean":np.nanmean(arr, axis=0, keepdims=True)
            }
            s3 = boto3.client('s3')

            for s in stats:
                tmp2.append('tmp2_{}_{}'.format(idx,s))
                outKey = outKeyTemplate.format(model=s)

                with rio.open(tmp2[-1],'w',**profile) as dst:
                    dst.write(out[s].astype(arr.dtype))

                print ("{} Putting {}".format(idx, outKey))
                s3.upload_file(tmp2[-1], outBucket, outKey, {"ACL": "public-read"})

        print ("{} Cleaning {}".format(i,outKey))
        for f in tmp1:
            os.remove(f)
        for f in tmp2:
            os.remove(f)

    except Exception as e:
        print (e, options)
        try:
            for f in tmp1:
                os.remove(f)
            for f in tmp2:
                os.remove(f)
        except:
            pass
