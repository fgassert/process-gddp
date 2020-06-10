
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
    return mm/86400
def kgs2mm(kgs):
    return kgs*86400

def stack_and_scale(ch, i, s, y):
    arr = None
    profile = None
    y1 = y-15
    y2 = y+15
    for e in ['q25', 'q50', 'q75']:
        rast = raster_template(e, ch, i, s, y1, y2)
        url = url_template(rast)
        with rio.open(url, 'r') as src:
            print(f' reading {url}')
            _arr = src.read()
            if arr is None:
                arr = _arr
                profile = src.profile
            else:
                arr = arr = np.concatenate((arr, _arr), axis=0)
        
    profile["driver"] = "GTiff"
    profile["count"] = arr.shape[0]

    pfx = 'stacked'
    if i in ['annual_tasmin','annual_tasmax','tavg-tasmin_tasmax']:
        pfx += '-degC'
        arr = k2c(arr)
    elif i in ['annual_pr']:
        pfx += '-mmyr'
        arr = kgs2mm(arr)

    outrast = raster_template(pfx, ch, i, s, y1, y2)
    print(f' writing {outrast}')
    with rio.open(os.path.join(OUTDIR, outrast), 'w', **profile) as dst:
        dst.write(arr.astype(profile['dtype']))


OUTDIR = 'prep_share'
os.makedirs(OUTDIR, exist_ok=True)

for i in temp_indicators:
    for s in scenarios:
        for y in range(startyear, endyear+1, 10):
            for ch in ['abs', 'diff']:
                stack_and_scale(ch, i, s, y)

for i in pr_indicators:
    for s in scenarios:
        for y in range(startyear, endyear+1, 10):
            for ch in ['abs', 'ch']:
                stack_and_scale(ch, i, s, y)
