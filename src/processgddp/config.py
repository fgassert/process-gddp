#!/usr/bin/env python

dailyTemplate = "http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/{scenario}/day/atmos/{variable}/r1i1p1/v1.0/{variable}_day_BCSD_{scenario}_r1i1p1_{model}_{year}.nc"

monthlyTemplate = "tmp/nex-gddp/monthly/{variable}_monthly_BCSD_{scenario}_r1i1p1_{model}_{year}.tif"
annualTemplate = "tmp/nex-gddp/annual/{variable}_annual_BCSD_{scenario}_r1i1p1_{model}_{year}.tif"

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
yearsFuture = range(2006,2101)
stats = ['mean','q25','q50','q75']
