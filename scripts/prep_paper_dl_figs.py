#!/usr/bin/env python3

import rasterio as rio
from rasterio import plot, mask
import numpy as np
import os
from prep_paper import *

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

def download_geotiff(e, ch, i, s, y, d):
    arr = None
    profile = None
    y1 = y-15
    y2 = y+15
    rast = raster_template(e, ch, i, s, y1, y2, d)
    url = url_template(rast)
    print(f' reading {url}')
    with rio.open(url, 'r') as src:
        arr = src.read()
        profile = src.profile

    # calc RELATIVE IQR
    pfx = e
    if e == 'iqr' and i in pr_indicators:
        url2 = url_template(raster_template('q50', 'abs', i, s, y1, y2, d))
        with rio.open(url2, 'r') as src:
            arr = arr / src.read()
            pfx = 'riqr'
    
    
    profile["driver"] = "GTiff"
    profile["count"] = arr.shape[0]

    if e != 'iqr' and ch not in ['ch', 'diff']:
        if i in ['annual_tasmin','annual_tasmax','tavg-tasmin_tasmax']:
            pfx += '-degC'
            arr = k2c(arr)
        elif i in ['annual_pr']:
            pfx += '-mmyr'
            arr = kgs2mm(arr)

    outrast = raster_template(pfx, ch, i, s, y1, y2, d)
    print(f' writing {outrast}')
    with rio.open(os.path.join(OUTDIR, outrast), 'w', **profile) as dst:
        dst.write(arr.astype(profile['dtype']))


OUTDIR = 'figures'
os.makedirs(OUTDIR, exist_ok=True)

for d in datasets:
    for s in ('rcp85',):
        for y in (2050,):
            for i in temp_indicators:
                for e, ch in [('q50', 'diff'), ('iqr', 'abs'), ('q50', 'abs')]:
                    download_geotiff(e, ch, i, s, y, d)
            for i in pr_indicators:
                for e, ch in [('q50', 'ch'), ('iqr', 'abs'), ('q50', 'abs')]:
                    download_geotiff(e, ch, i, s, y, d)
