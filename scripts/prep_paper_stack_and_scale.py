#!/usr/bin/env python3

import rasterio as rio
from rasterio import plot, mask
import numpy as np
import os

temp_indicators = [
    'annual_tasmin',
    'annual_tasmax',
    'tavg-tasmin_tasmax',
    'hdd65f-tasmin_tasmax',
    'cdd65f-tasmin_tasmax',
    'frostfree_tasmin',
    'gt-q99_tasmax'
]

pr_indicators = [
    'dryspells_pr',
    'annual_pr',
    'gt-q99_pr',
]

ensembles = [
    'q50',
    'q25',
    'q75',
    'iqr'
]

scenarios = [
    'rcp45',
    'rcp85'
]

datasets = [
    'nexgddp',
    'loca'
]

startyear = 2000
endyear = 2080


def raster_template(e, ch, i, s, y1, y2, d):
    if not d:
        return f'{e}-{ch}-{i}_{s}_ens_{y1}-{y2}.tif'
    return f'{e}-{ch}-{i}_{s}_ens_{y1}-{y2}_{d}.tif'

def url_template(rast):
    return f'https://s3.amazonaws.com/md.cc/tmp/nex-gddp/{rast}'


def c2k(c):
    return c+273.15
def k2c(k):
    return k-273.15
def k2f(k):
    return c2f(k2c(k))
def c2f(c):
    return c*9/5+32
def c2f_rel(c):
    return c*9/5
def mm2kgs(mm):
    return mm/86400/365
def kgs2mm(kgs):
    return kgs*86400*365

def stack_and_scale(ch, i, s, y, d):
    arr = None
    profile = None
    y1 = y-15
    y2 = y+15
    for e in ['q25', 'q50', 'q75']:
        rast = raster_template(e, ch, i, s, y1, y2, d)
        url = url_template(rast)
        print(f' reading {url}')
        with rio.open(url, 'r') as src:
            _arr = src.read()
            if arr is None:
                arr = _arr
                profile = src.profile
            else:
                arr = arr = np.concatenate((arr, _arr), axis=0)
        
    profile["driver"] = "GTiff"
    profile["count"] = arr.shape[0]

    pfx = 'stacked'
    if i in ['annual_tasmin','annual_tasmax','tavg-tasmin_tasmax'] and ch == 'abs':
        pfx += '-degC'
        arr = k2c(arr)
    elif i in ['annual_pr'] and ch == 'abs':
        pfx += '-mmyr'
        arr = kgs2mm(arr)

    outrast = raster_template(pfx, ch, i, s, y1, y2, d)
    print(f' writing {outrast}')
    with rio.open(os.path.join(OUTDIR, outrast), 'w', **profile) as dst:
        dst.write(arr.astype(profile['dtype']))


OUTDIR = 'prep_share'
os.makedirs(OUTDIR, exist_ok=True)

for d in datasets:
    for s in scenarios:
        for y in range(startyear, endyear+1, 10):
            for i in temp_indicators:
                for ch in ['abs', 'diff']:
                    stack_and_scale(ch, i, s, y, d)
            for i in pr_indicators:
                for ch in ['abs', 'ch']:
                    stack_and_scale(ch, i, s, y, d)
