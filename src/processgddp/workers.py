import os
import numpy as np
import rasterio as rio
import urllib
import logging
from . import util

def bandMean(arr):
    return np.nanmean(arr, axis=0, keepdims=True)
def bandSum(arr):
    return np.nansum(arr, axis=0, keepdims=True)

def percentile(q):
    return lambda arr: np.nanpercentile(arr, q, axis=0, keepdims=True)
def countAbove(v):
    return lambda arr: np.nansum(np.where(arr>v, 1, 0) , axis=0, keepdims=True)

def hdd(v):
    return lambda arr: np.nansum(np.where(arr>v, arr-v, 0), axis=0, keepdims=True)
def cdd(v):
    return lambda arr: np.nansum(np.where(arr<v, v-arr, 0), axis=0, keepdims=True)

def subtractArr(arr):
    return arr[:-1]-arr[-1]
def divideArr(arr):
    return arr[:-1]/arr[-1]
def countAboveArr(arr):
    return np.nansum(np.where(arr[:-1]>arr[-1], 1, 0), axis=0, keepdims=True)


def f2k(deg):
    return c2k(f2c(deg))
def f2c(deg):
    return (deg-32)*5/9
def c2k(deg):
    return deg+273.15

FUNCTIONS = {
    None: lambda x:x,
    'mean': bandMean,
    'sum': bandSum,
    'sub': subtractArr,
    'div': divideArr,
    'gt':countAboveArr,
    'q25':percentile(25),
    'q50':percentile(50),
    'q75':percentile(75),
    'q98':percentile(98),
    'q99':percentile(99),
    'gt95f':countAbove(f2k(95)),
    'gt90f':countAbove(f2k(90)),
    'gt32f':countAbove(f2k(32)),
    'gt50':countAbove(50),
    'hdd65f':hdd(f2k(65)),
    'cdd65f':cdd(f2k(65)),
    'hdd16c':hdd(c2k(16)),
    'cdd16c':cdd(c2k(16))
}

NOCACHE = False
STRICT = True

def worker(yields, requires, function=None, options={}):
    nocache = options.get('nocache', NOCACHE)
    strict = options.get('strict', STRICT)
    client = util.Client(**options)


    if function not in FUNCTIONS:
        raise Exception("Function {} not defined".format(function))
    if type(requires) not in (list, tuple):
        requires = [requires]

    try:
        arr = None
        for r in requires:
            fname = client.getObj(r, nocache=nocache)
            if arr is None:
                arr, profile = read(fname)
            else:
                arr2, _ = read(fname)
                arr = np.concatenate((arr, arr2), axis=0)

        logging.info('Processing {}'.format(yields))
        arr = FUNCTIONS[function](arr)

        fname = client.cached(yields)
        write(arr, fname, profile)
        client.putObj(fname, yields)

        if nocache:
            client.cleanObjs(yields)
            client.cleanObjs(requires)

    except Exception as e:
        err = '{}: {}'.format(yields, e)
        if strict:
            raise Exception(err)
        else:
            logging.error(err)
            return err

    return None

def _writeTiff(arr, outfile, profile):
    profile["driver"] = "GTiff"
    profile["count"] = arr.shape[0]
    try:
        with rio.open(outfile, 'w', **profile) as dst:
            dst.write(arr)
    except (SystemExit, KeyboardInterrupt):
        logging.info('Exiting gracefully {}'.format(outfile))
        os.remove(outfile)
    return outfile

def write(arr, outfile, profile):
    return _writeTiff(arr.astype(profile['dtype']), outfile, profile)

def _readNC(infile):
    with rio.open(infile) as src:
        arr = src.read()
        profile = src.profile
        t,h,w = arr.shape

        # No Data
        arr[arr==profile['nodata']] = np.nan
        # Roll x-axis for 180W origin
        arr = np.roll(arr, int(w/2), axis=2)
        # Set scale to .25 and origin to 90S,180W
        profile.update({
            "transform":rio.Affine(360.0/w,0,-180,0,-180.0/h,90),
            "crs":"EPSG:4326"
        })
        return arr, profile

def _readTiff(infile):
    with rio.open(infile) as src:
        arr = src.read()
        profile = src.profile
        if 'nodata' in profile:
            arr[arr==profile['nodata']] = np.nan
        return arr, profile

def read(infile):
    if len(infile)>3 and infile[-3:]=='.nc':
        return _readNC(infile)
    else:
        return _readTiff(infile)
