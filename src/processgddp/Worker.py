import os
import numpy as np
import rasterio as rio
import urllib
import logging

from . import FileHandler
from .formulae import FUNCTIONS

NOCACHE = False
STRICT = True

def worker(yields, requires, function=None, options={}):
    nocache = options.get('nocache', NOCACHE)
    strict = options.get('strict', STRICT)
    client = FileHandler.Client(**options)
    if function not in FUNCTIONS:
        raise Exception("Function {} not defined".format(function))

    try:
        arr, profile = getData(requires, client, nocache)

        logging.info('Processing {}'.format(yields))
        arr = FUNCTIONS[function](arr)

        fname = client.cached(yields)
        write(arr, fname, profile)
        client.putObj(fname, yields)

        if nocache:
            client.cleanObjs(fname)
            client.cleanObjs(requires)

    except Exception as e:
        err = '{}: {}'.format(yields, e)
        if strict:
            raise Exception(err)
        else:
            logging.error(err)
            return err

    return None

def getData(requires, client, nocache=NOCACHE):
    arr = None
    if type(requires) not in (list, tuple):
        requires = [requires]
    for r in requires:
        fname = client.getObj(r, nocache=nocache)
        if arr is None:
            arr, profile = read(fname)
        else:
            arr2, _ = read(fname)
            arr = np.concatenate((arr, arr2), axis=0)
    return arr, profile

def _writeTiff(arr, outfile, profile):
    profile["driver"] = "GTiff"
    profile["count"] = arr.shape[0]
    try:
        with rio.open(outfile, 'w', **profile) as dst:
            dst.write(arr.astype(profile['dtype']))
    except (SystemExit, KeyboardInterrupt):
        logging.info('Exiting gracefully {}'.format(outfile))
        os.remove(outfile)
    return outfile

def write(arr, outfile, profile):
    return _writeTiff(arr, outfile, profile)

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
