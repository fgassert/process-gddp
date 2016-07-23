#!/usr/bin/env python

import boto3
import os
import multiprocessing as mp
import sys
from worker_annual import worker


inurlTemplate = "http://md.cc.s3.amazonaws.com/tmp/nex-gddp/{variable}_day_BCSD_{scenario}_r1i1p1_{model}_{year}.tif"
outkeyTemplate = "tmp/nex-gddp/annual/{variable}_annual_BCSD_{scenario}_r1i1p1_{model}_{year}.tif"
prefix = 'tmp/nex-gddp/annual/'
bucket = "md.cc"

scenarios = ["historical","rcp85","rcp45"]
variables = ["pr","tasmax","tasmin"]
models =    ['ACCESS1-0',
              'BNU-ESM',
              'CCSM4',
              'CESM1-BGC',
              'CNRM-CM5',
              'CSIRO-Mk3-6-0',
              'CanESM2',
              'GFDL-CM3',
              'GFDL-ESM2G',
              'GFDL-ESM2M',
              'IPSL-CM5A-LR',
              'IPSL-CM5A-MR',
              'MIROC-ESM-CHEM',
              'MIROC-ESM',
              'MIROC5',
              'MPI-ESM-LR',
              'MPI-ESM-MR',
              'MRI-CGCM3',
              'NorESM1-M',
              'bcc-csm1-1',
              'inmcm4']
yearsHistorical = range(1950,2006)
yearsFuture = range(2006,2100)

def templater(template, v, s, m, y):
    return template.format(variable=v, scenario=s, model=m, year=y)

def main(threads = 1):
    tasks = []

    s3 = boto3.resource('s3')
    b = s3.Bucket(bucket)
    existing_keys = [obj.key for obj in b.objects.filter(Prefix=prefix)]

    i = 0
    for v in variables:
        for m in models:
            for s in scenarios[:1]:
                for y in yearsHistorical:
                    key = templater(outkeyTemplate,v,s,m,y)
                    if not key in existing_keys:
                        i += 1
                        tasks.append(
                            {
                                "inUrl":templater(inurlTemplate,v,s,m,y),
                                "outKey":key,
                                "outBucket":bucket,
                                "var":v,
                                "id":i
                            }
                        )
            for s in scenarios[1:]:
                for y in yearsFuture:
                    key = templater(outkeyTemplate,v,s,m,y)
                    if not key in existing_keys:
                        i += 1
                        tasks.append(
                            {
                                "inUrl":templater(inurlTemplate,v,s,m,y),
                                "outKey":key,
                                "outBucket":bucket,
                                "var":v,
                                "id":i
                            }
                        )

    pool = mp.Pool(threads)
    pool.map_async(worker, tasks).get(604800)
    pool.close()

if __name__ == "__main__":
    threads = 1
    if len(sys.argv)>1:
        threads = int(sys.argv[1])
    main(threads)
