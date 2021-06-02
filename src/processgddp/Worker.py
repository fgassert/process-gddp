import os
import numpy as np
import rasterio as rio
import logging

from . import FileHandler
from . import DependencyHandler
from .formulae import FUNCTIONS

NOCACHE = False
STRICT = True

def worker(yields, requires, function=None, options={}, dryrun=False):
    if dryrun:
        print(yields, requires)
        return yields
    if function is None:
        raise Exception('Function not defined')
    nocache = options.get('nocache', NOCACHE)
    dataset = DependencyHandler.parseKey(yields)['d']
    client = FileHandler.Client(**options)

    arr, profile = getData(requires, client, dataset, nocache)

    logging.debug('Processing {}'.format(yields))
    arr = FUNCTIONS[function](arr)

    fname = client.cached(yields)
    write(arr, fname, profile)
    client.putObj(fname, yields)

    if nocache:
        client.cleanObjs(fname)

    return yields

def getData(requires, client, dataset, nocache=NOCACHE):
    arr = None
    if type(requires) not in (list, tuple):
        requires = [requires]
    for r in requires:
        fname = client.getObj(r, nocache=nocache)
        if arr is None:
            arr, profile = read(fname, dataset)
        else:
            arr2, _ = read(fname, dataset)
            arr = np.concatenate((arr, arr2), axis=0)
        if nocache:
            client.cleanObjs(fname)
    return arr, profile

def _writeTiff(arr, outfile, profile):
    profile["driver"] = "GTiff"
    profile["count"] = arr.shape[0]
    try:
        with rio.open(outfile, 'w', **profile) as dst:
            dst.write(arr.astype(profile['dtype']))
    except (SystemExit, KeyboardInterrupt):
        logging.debug('Exiting gracefully {}'.format(outfile))
        os.remove(outfile)
    return outfile

def write(arr, outfile, profile):
    return _writeTiff(arr, outfile, profile)

def _readNC(infile, dataset):
    with rio.open(infile) as src:
        arr = src.read()
        profile = src.profile
        # No Data
        if 'nodata' in profile:
            arr[arr==profile['nodata']] = np.nan

        # Reshape NEXGDDP raster
        if dataset == DependencyHandler.NEXGDDP:
            _,h,w = arr.shape
            # Roll x-axis for 180W origin
            arr = np.roll(arr, int(w/2), axis=2)
            # Set scale to .25 and origin to 90S,180W
            profile.update({
                "transform":rio.Affine(360.0/w,0,-180,0,-180.0/h,90),
                "crs":"EPSG:4326"
            })
        elif dataset == DependencyHandler.LOCA:
            profile.update({'crs':"EPSG:4326"})
        return arr, profile

def _readTiff(infile):
    with rio.open(infile) as src:
        arr = src.read()
        profile = src.profile
        if 'nodata' in profile:
            arr[arr==profile['nodata']] = np.nan
        return arr, profile

def read(infile, dataset):
    if os.path.splitext(infile)[-1] == '.nc':
        return _readNC(infile, dataset)
    else:
        return _readTiff(infile)
