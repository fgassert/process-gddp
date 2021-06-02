#!/usr/bin/env python
import sys
import boto3
import os
import zipfile
import rasterio as rio
import numpy as np


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


BUCKET = os.getenv('GDDP_BUCKET')
PREFIX = os.getenv('GDDP_PREFIX') or 'tmp/nex-gddp'
ARCHIVE_PREFIX = 'prepdata/nex-gddp'
DATA_DIR = 'prep_share'


def k2c(k):
    return k-273.15
def kgs2mmyr(kgs):
    return kgs*86400*365

def raster_template(e, ch, i, s, y1, y2, d):
    if not d:
        return f'{e}-{ch}-{i}_{s}_ens_{y1}-{y2}.tif'
    return f'{e}-{ch}-{i}_{s}_ens_{y1}-{y2}_{d}.tif'


def url_template(rast):
    return os.path.join(f'https://s3.amazonaws.com/md.cc/{ARCHIVE_PREFIX}', rast)


def _gen_all_args():
    for d in datasets:
        for s in scenarios:
            for y in range(startyear, endyear+1, 10):
                y1 = y-15
                y2 = y+15
                for e in ensembles:
                    for i in temp_indicators:
                        for ch in ['abs', 'diff']:
                            yield (e, ch, i, s, y1, y2, d)
                    for i in pr_indicators:
                        for ch in ['abs', 'ch']:
                            yield (e, ch, i, s, y1, y2, d)           
        for i in temp_indicators+pr_indicators:
            yield ('q50', 'abs', i, 'historical', 1960, 1990, d)
            

def main():
    for args in _gen_all_args():
        print(raster_template(*args))


def baselines():
    for d in datasets:
        for i in temp_indicators+pr_indicators:
            print(raster_template('q50', 'abs', i, 'historical', 1960, 1990, d))

def csv():
    print('indicator, scenario, year, change, ensemble, dataset, tiff, url')
    
    for e, ch, i, s, y1, y2, d in _gen_all_args():
        rast = raster_template(e, ch, i, s, y1, y2, d)
        url = url_template(rast)
        y = f'{y1}-{y2}'
        print(', '.join([i, s, str(y), ch, e, d, rast, url]))


def move_to_archive():
    session = boto3.session.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    client = session.resource('s3')
    bucket = client.Bucket(BUCKET)

    for e, ch, i, s, y1, y2, d in _gen_all_args():
        rast = raster_template(e, ch, i, s, y1, y2, d)
        rast0 = os.path.join(PREFIX, rast)
        rast1 = os.path.join(ARCHIVE_PREFIX, rast)
        try:
            bucket.Object(rast1).copy_from(CopySource=os.path.join(BUCKET, rast0))
            print(f'copy from {rast0} to {rast1}')
        except Exception as ex:
            print(ex, rast0)
            pass


def download():
    session = boto3.session.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    client = session.resource('s3')
    bucket = client.Bucket(BUCKET)

    os.makedirs(DATA_DIR, exist_ok=True)

    for args in _gen_all_args():
        rast = raster_template(*args)
        obj = os.path.join(ARCHIVE_PREFIX, rast)
        fname = os.path.join(DATA_DIR, rast)
        if os.path.isfile(fname):
            print(f'{fname} exists; skipping.')
        else:
            try:
                bucket.download_file(obj, fname)
                print(f'downloading {obj}')
            except Exception as ex:
                print(ex, obj)
                pass


def download_zip():
    zips = {}

    download()
    for e, ch, i, s, y1, y2, d in _gen_all_args():
        rast = raster_template(e, ch, i, s, y1, y2, d)
        zipname = os.path.join(DATA_DIR, f'{d}_{i}.zip')
        write_opts = {
            'filename': os.path.join(DATA_DIR, rast),
            'arcname': os.path.join(s, e, rast)
        }
        if zipname in zips:
            zips[zipname].append(write_opts)
        else:
            zips[zipname] = [write_opts]
        if s == 'historical':
            zipname = 'baselines.zip'
            write_opts['arcname'] = rast
            if zipname in zips:
                zips[zipname].append(write_opts)
            else:
                zips[zipname] = [write_opts]

    for zipname, opts_list in zips.items():
        with zipfile.ZipFile(zipname, 'w', compression=zipfile.ZIP_DEFLATED) as z:
            for i, opts in enumerate(opts_list):
                z.write(**opts)
                print(f'{zipname}: {i+1}/{len(opts_list)}', end='\r')
        print('')


def fix_loca_projection():
    download()
    count = 0
    for e, ch, i, s, y1, y2, d in _gen_all_args():
        if d == 'loca':
            rast = raster_template(e, ch, i, s, y1, y2, d)
            fname = os.path.join(DATA_DIR, rast)
            with rio.open(fname, 'r+') as dst:
                if dst.crs is None:
                    dst.crs = 'EPSG:4326'
                    count += 1
                    print(f'fixed proj for {count} rasters', end='\r')
    print('All LOCA rasts have CRS')


def fix_loca_mask():
    download()

    rast = raster_template('q50', 'abs', 'annual_tasmax', 'historical', 1960, 1990, 'loca')
    fname = os.path.join(DATA_DIR, rast)
    with rio.open(fname, 'r') as src:
        mask = src.read()
        mask = (mask >= 1e30) | (mask == np.nan)

    print(f"masksize: {mask.sum()/mask.size}")
    
    for e, ch, i, s, y1, y2, d in _gen_all_args():
        if d == 'loca':
            rast = raster_template(e, ch, i, s, y1, y2, d)
            fname = os.path.join(DATA_DIR, rast)
            print(f'masking {rast}')
            with rio.open(fname, 'r+') as dst:
                arr = dst.read()
                arr[mask] = np.nan
                dst.write(arr)


def stack_and_scale():
    download()

    stacks = {}
    zips = {}
    for d in datasets:
        for s in scenarios:
            for y in range(startyear, endyear+1, 10):
                y1 = y-15
                y2 = y+15
                for i in temp_indicators:
                    for ch in ['abs', 'diff']:
                        outargs = ('stacked', ch, i, s, y1, y2, d)
                        stacks[outargs] = [raster_template(e, ch, i, s, y1, y2, d) for e in ('q25', 'q50', 'q75')]
                for i in pr_indicators:
                    for ch in ['abs', 'ch']:
                        outargs = ('stacked', ch, i, s, y1, y2, d)
                        stacks[outargs] = [raster_template(e, ch, i, s, y1, y2, d) for e in ('q25', 'q50', 'q75')]
            
    for outargs, rasts in stacks.items():
        (pfx, ch, i, s, y1, y2, d) = outargs
        
        pfx = 'stacked'
        conv_degC = i in ['annual_tasmin','annual_tasmax','tavg-tasmin_tasmax'] and ch == 'abs'
        conv_mmyr = i in ['annual_pr'] and ch == 'abs'
        if conv_degC:
            pfx += '-degC'
        elif conv_mmyr:
            pfx += '-mmyr'

        outrast = raster_template(pfx, ch, i, s, y1, y2, d)
        outfile = os.path.join(DATA_DIR, outrast)
        
        if not os.path.isfile(outfile):
            arr = None
            profile = None
            for rast in rasts:
                fname = os.path.join(DATA_DIR, rast)
                with rio.open(fname, 'r') as src:
                    _arr = src.read()
                    if arr is None:
                        arr = _arr
                        profile = src.profile
                    else:
                        arr = arr = np.concatenate((arr, _arr), axis=0)

            if conv_degC:
                arr = k2c(arr)
            elif conv_mmyr:
                arr = kgs2mmyr(arr)
            
            print(f' writing {outrast}')
            profile["driver"] = "GTiff"
            profile["count"] = arr.shape[0]
            profile['crs'] = 'EPSG:4326'
            with rio.open(outfile, 'w', **profile) as dst:
                dst.write(arr.astype(profile['dtype']))
        
        else:
            print(f'{outfile} exists, skipping')

        zipname = os.path.join(DATA_DIR, f'prep_share_{d}.zip')
        write_opts = {
            'filename': os.path.join(DATA_DIR, outrast),
            'arcname': outrast
        }
        if zipname in zips:
            zips[zipname].append(write_opts)
        else:
            zips[zipname] = [write_opts]

    for zipname, opts_list in zips.items():
        with zipfile.ZipFile(zipname, 'w', compression=zipfile.ZIP_DEFLATED) as z:
            for i, opts in enumerate(opts_list):
                z.write(**opts)
                print(f'{zipname}: {i+1}/{len(opts_list)}', end='\r')
        print('')


if __name__ == '__main__':
    if len(sys.argv)>1 and sys.argv[1]=='csv':
        csv()
    elif len(sys.argv)>1 and sys.argv[1]=='archive':
        move_to_archive()
    elif len(sys.argv)>1 and sys.argv[1]=='baselines':
        baselines()
    elif len(sys.argv)>1 and sys.argv[1]=='zipdl':
        download_zip()
    elif len(sys.argv)>1 and sys.argv[1]=='stackscale':
        stack_and_scale()
    elif len(sys.argv)>1 and sys.argv[1]=='locaproj':
        fix_loca_projection()
    elif len(sys.argv)>1 and sys.argv[1]=='locamask':
        fix_loca_mask()
    else:
        main()
