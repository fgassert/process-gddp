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


def raster_template(e, ch, i, s, y1, y2, d):
    if not d:
        return f'{e}-{ch}-{i}_{s}_ens_{y1}-{y2}.tif'
    return f'{e}-{ch}-{i}_{s}_ens_{y1}-{y2}_{d}.tif'

def url_template(rast):
    return f'https://s3.amazonaws.com/md.cc/tmp/nex-gddp/{rast}'

def main():
    for d in datasets:
        for i in temp_indicators:
            for s in scenarios:
                for y in range(startyear, endyear+1, 10):
                    for ch in ['abs', 'diff']:
                        for e in ensembles:
                            y1 = y-15
                            y2 = y+15
                            print(raster_template(e, ch, i, s, y1, y2, d))
        for i in pr_indicators:
            for s in scenarios:
                for y in range(startyear, endyear+1, 10):
                    for ch in ['abs', 'ch']:
                        for e in ensembles:
                            y1 = y-15
                            y2 = y+15
                            print(raster_template(e, ch, i, s, y1, y2, d))

def csv():
    print('indicator, scenario, year, change, ensemble, dataset, tiff, url')

    for d in datasets:
        for i in temp_indicators:
            for s in scenarios:
                for y in range(startyear, endyear+1, 10):
                    for ch in ['abs', 'diff']:
                        for e in ensembles:
                            y1 = y-15
                            y2 = y+15
                            rast = raster_template(e, ch, i, s, y1, y2, d)
                            url = url_template(rast)
                            print(', '.join([i, s, str(y), ch, e, d, rast, url]))

        for i in pr_indicators:
            for s in scenarios:
                for y in range(startyear, endyear+1, 10):
                    for ch in ['abs', 'ch']:
                        for e in ensembles:
                            y1 = y-15
                            y2 = y+15
                            rast = raster_template(e, ch, i, s, y1, y2, d)
                            url = url_template(rast)
                            print(', '.join([i, s, str(y), ch, e, d, rast, url]))





def check_rename():
    BUCKET = os.getenv('GDDP_BUCKET')
    PREFIX = os.getenv('GDDP_PREFIX')
    session = boto3.session.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    client = session.resource('s3')
    bucket = client.Bucket(BUCKET)
    
    d = 'nexgddp'
    for s in scenarios:
        for y in range(startyear, endyear+1, 10):
            y1 = y-15
            y2 = y+15
            for e in ensembles:
                for i in temp_indicators:
                    for ch in ['abs', 'diff']:
                        rast0 = os.path.join(PREFIX, raster_template(e, ch, i, s, y1, y2, None))
                        rast1 = os.path.join(PREFIX, raster_template(e, ch, i, s, y1, y2, d))
                        try:
                            bucket.Object(rast1).copy_from(CopySource=os.path.join(BUCKET, rast0))
                            print(f'copy from {rast0} to {rast1}')
                            bucket.Object(rast0).delete()
                            print(f'delete {rast0}')
                        except Exception as ex:
                            print(ex)
                            pass

                for i in pr_indicators:
                    for ch in ['abs', 'ch']:
                        rast0 = os.path.join(PREFIX, raster_template(e, ch, i, s, y1, y2, None))
                        rast1 = os.path.join(PREFIX, raster_template(e, ch, i, s, y1, y2, d))
                        try:
                            bucket.Object(rast1).copy_from(CopySource=os.path.join(BUCKET, rast0))
                            print(f'copy from {rast0} to {rast1}')
                            bucket.Object(rast0).delete()
                            print(f'delete {rast0}')
                        except Exception as ex:
                            print(ex)
                            pass

if __name__ == '__main__':
    if len(sys.argv)>1 and sys.argv[1]=='csv':
        csv()
    elif len(sys.argv)>1 and sys.argv[1]=='rename':
        check_rename()
    else:
        main()
