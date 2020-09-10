#!/usr/bin/env python
import sys
import boto3
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


BUCKET = os.getenv('GDDP_BUCKET')
PREFIX = os.getenv('GDDP_PREFIX') or 'tmp/nex-gddp/'
ARCHIVE_PREFIX = 'prepdata/nex-gddp/'


def raster_template(e, ch, i, s, y1, y2, d):
    if not d:
        return f'{e}-{ch}-{i}_{s}_ens_{y1}-{y2}.tif'
    return f'{e}-{ch}-{i}_{s}_ens_{y1}-{y2}_{d}.tif'

def url_template(rast):
    return f'https://s3.amazonaws.com/md.cc/tmp/nex-gddp/{rast}'


def _iter_func(func):
    for d in datasets:
        for s in scenarios:
            for y in range(startyear, endyear+1, 10):
                y1 = y-15
                y2 = y+15
                for e in ensembles:
                    for i in temp_indicators:
                        for ch in ['abs', 'diff']:
                            func(e, ch, i, s, y1, y2, d)
                    for i in pr_indicators:
                        for ch in ['abs', 'ch']:
                            func(e, ch, i, s, y1, y2, d)
        for i in temp_indicators+pr_indicators:
            func('q50', 'abs', i, 'historical', 1960, 1990, d)


def main():
    def _print_rasters(*args):
        print(raster_template(*args))    
    _iter_func(_print_rasters)


def baselines():
    for d in datasets:
        for i in temp_indicators+pr_indicators:
            print(raster_template('q50', 'abs', i, 'historical', 1960, 1990, d))

def csv():
    print('indicator, scenario, year, change, ensemble, dataset, tiff, url')
    def _print_csv_line(e, ch, i, s, y1, y2, d):
        rast = raster_template(e, ch, i, s, y1, y2, d)
        url = url_template(rast)
        print(', '.join([i, s, str(y), ch, e, d, rast, url]))
    _iter_func(_print_csv_line)


def move_to_archive():
    session = boto3.session.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    client = session.resource('s3')
    bucket = client.Bucket(BUCKET)

    def _archive(*args, dry_run=False):
        rast = raster_template(e, ch, i, s, y1, y2, d)
        rast0 = os.path.join(PREFIX, rast)
        rast1 = os.path.join(ARCHIVE_PREFIX, rast)
        try:
            bucket.Object(rast1).copy_from(CopySource=os.path.join(BUCKET, rast0))
            print(f'copy from {rast0} to {rast1}')
        except Exception as ex:
            print(ex, 'rast0')
            pass
    
    _iter_func(_archive)

if __name__ == '__main__':
    if len(sys.argv)>1 and sys.argv[1]=='csv':
        csv()
    elif len(sys.argv)>1 and sys.argv[1]=='archive':
        move_to_archive()
    elif len(sys.argv)>1 and sys.argv[1]=='baselines':
        baselines()
    else:
        main()
