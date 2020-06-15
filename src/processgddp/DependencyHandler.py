#!/usr/bin/env python

import os
from .Worker import worker
from .TaskTree import TaskTree
from functools import partial

NEXGDDP = 'nexgddp'
LOCA = 'loca'
SRCTEMPLATES = {
    NEXGDDP: "http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/{scenario}/day/atmos/{variable}/r1i1p1/v1.0/{variable}_day_BCSD_{scenario}_r1i1p1_{model}_{year}.nc",
    LOCA: "ftp://gdo-dcp.ucllnl.org/pub/dcp/archive/cmip5/loca/LOCA_2016-04-02/{model}/16th/{scenario}/{member}/{variable}/{variable}_day_{model}_{scenario}_{member}_{year}0101-{year}1231.LOCA_2016-04-02.16th.nc"
}

FILETEMPLATE = "{function}_{variable}_{scenario}_{model}_{year}_{dataset}.tif"

SCENARIOS = ["historical","rcp85","rcp45"]
VARIABLES = ["pr","tasmax","tasmin"]
MODELS = {
    NEXGDDP:[
        'ACCESS1-0',
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
        'inmcm4'
    ],
    LOCA: [
        'ACCESS1-0',
        'ACCESS1-3',
        'CCSM4',
        'CESM1-BGC',
        'CESM1-CAM5',
        'CMCC-CM',
        'CMCC-CMS',
        'CNRM-CM5',
        'CSIRO-Mk3-6-0',
        'CanESM2',
        'EC-EARTH',
        'FGOALS-g2',
        'GFDL-CM3',
        'GFDL-ESM2G',
        'GFDL-ESM2M',
        'GISS-E2-H',
        'GISS-E2-R',
        'HadGEM2-AO',
        'HadGEM2-CC',
        'HadGEM2-ES',
        'IPSL-CM5A-LR',
        'IPSL-CM5A-MR',
        'MIROC-ESM',
        'MIROC-ESM-CHEM',
        'MIROC5',
        'MPI-ESM-LR',
        'MPI-ESM-MR',
        'MRI-CGCM3',
        'NorESM1-M',
        'bcc-csm1-1',
        'bcc-csm1-1-m',
        'inmcm4'
    ]
}
MODEL_MEMBERS = {
    'CCSM4': 'r6i1p1',
    'GISS-E2-H': 'r6i1p1',
    'GISS-E2-R': 'r6i1p1',
}
ENSEMBLE = 'ens'
SOURCEDATA = 'src'
STARTYEAR = 1950
PROJYEAR = 2006
ENDYEAR = 2100

DATASETS = [NEXGDDP, LOCA]

_Formulae = {}

def getTemplate(f="{function}", v="{variable}", s="{scenario}", m="{model}", y="{year}", d="{dataset}"):
    return keyName(f, v, s, m, y, d)

def keyName(f, v, s, m, y, d=NEXGDDP):
    if f == SOURCEDATA:
        return srcName(v, s, m, y, d)
    return FILETEMPLATE.format(
        function=f, variable=v, scenario=s, model=m, year=str(y), dataset=d)

def srcName(v, s, m, y, d=NEXGDDP):
    args = {
        'variable': v, 
        'scenario': s, 
        'model': m, 
        'year': str(y)
    }
    if d == LOCA:
        if m in MODEL_MEMBERS:
            args['member'] = MODEL_MEMBERS[m]
        else:
            args['member'] = 'r1i1p1'
    return SRCTEMPLATES[d].format(**args)

def validateKey(key):
    vals = parseKey(key)
    _validateKey(key, **vals)
    return keyName(**vals)

def _validateKey(key, f, v, s, m, y, d):
    yrs = y.split('-')
    if not f in _Formulae:
        raise Exception('Invalid key {}; Formula {} must be one of {}'.format(
            key, f, ','.join(_Formulae.keys())))
    elif not v in VARIABLES:
        raise Exception('Invalid key {}; Variable {} must be one of {}'.format(
            key, v, ','.join(VARIABLES)))
    elif not s in SCENARIOS:
        raise Exception('Invalid key {}; Scenario {} must be one of {}'.format(
            key, s, ','.join(SCENARIOS)))
    elif not (m in MODELS[d] or m == ENSEMBLE):
        raise Exception('Invalid key {}; Model {} must be one of {},{}'.format(
            key, m, ','.join(MODELS[d]), ENSEMBLE))
    elif not ((int(yrs[0]) >= STARTYEAR and int(yrs[0]) <= ENDYEAR) and
          (len(yrs) == 1 or (int(yrs[1]) >= STARTYEAR and int(yrs[1]) <= ENDYEAR))):
        raise Exception('Invalid key {}; Year(s) {} must be between {},{}'.format(
            key, yrs, STARTYEAR, ENDYEAR))
    elif not d in DATASETS:
        raise Exception('Invalid key {}; Dataset {} must be one of {}'.format(
            key, d, ','.join(DATASETS)))

def parseKey(key):
    vals = os.path.splitext(key)[0].split('_')
    if len(vals) != 6:
        raise Exception('Invalid key {}; Must be of format {}'.format(
            key, FILETEMPLATE))
    return dict(zip(('f', 'v', 's', 'm', 'y', 'd'), vals))

def getFormula(key):
    return _Formulae[parseKey(key)['f']]

def getParams(key):
    params = parseKey(key)
    del params['f']
    return params

def listFormulae():
    return _Formulae.keys()

def dependencyTree(keys, client, skipExisting=False, poolargs={}):
    '''yeilds depth-first unique dependencies for a given set of task keys'''
    tree = TaskTree(**poolargs)
    if type(keys) is str:
        keys = [keys]
    for key in keys:
        key = validateKey(key)
        if not tree.exists(key):
            _addDependencies(tree, key, client, skipExisting)
    tree.skip_undefined()
    return tree

def _addDependencies(tree, key, client, skipExisting=False):
    if skipExisting and client.objExists2(key):
        tree.skip_task(key)
        return
    formula = getFormula(key)
    requires = formula.requires(**getParams(key))
    tree.add(formula.getFunction(), key, requires)
    for k in requires:
        if not tree.exists(k):
            if k[:4] != 'http':
                _addDependencies(tree, k, client, skipExisting)
            else:
                tree.skip_task(k)

def registerFormula(ftype, name=None, requires=SOURCEDATA, function=None, **kwargs):
    if name is None:
        name = '{}-{}'.format(function, requires)
    if name in _Formulae:
        raise Exception("Formula {} already defined".format(name))
    _Formulae[name] = ftype(name, requires, function, **kwargs)

def buildKey(key, *args, **kwargs):
    formula = getFormula(key)
    params = getParams(key)
    requires = formula.requires(**params)
    return formula.getFunction()(key, requires, *args, **kwargs)

class Formula:
    def __init__(self, name, requires, function, description=''):
        self.name = name
        self.function = function
        self._requires = requires
        self.description = description
    def __repr__(self):
        return getTemplate(self.name)
    def requires(self, v, s, m, y, d):
        if int(y) < PROJYEAR:
            s = SCENARIOS[0]
        return [keyName(self._requires, v, s, m, y, d)]
    def yields(self, v, s, m, y, d):
        try:
            if int(y) < PROJYEAR:
                s = SCENARIOS[0]
        except:
            y1, y2 = y.split('-')
            if int(y1) < PROJYEAR and int(y2) < PROJYEAR:
                s = SCENARIOS[0]
        return keyName(self.name, v, s, m, y, d)
    def getFunction(self):
        return partial(worker, function=self.function)

class Formula2(Formula):
    def __init__(self, name, requires, function, requires2, description=''):
        self.name = name
        self.function = function
        self._requires = requires
        self._requires2 = requires2
    def requires(self, v, s, m, y, d):
        try:
            if int(y) < PROJYEAR:
                s = SCENARIOS[0]
        except:
            y1, y2 = y.split('-')
            if int(y1) < PROJYEAR and int(y2) < PROJYEAR:
                s = SCENARIOS[0]
        return [
            keyName(self._requires, v, s, m, y, d),
            self._requires2.format(variable=v, scenario=s, model=m, year=y, dataset=d)
        ]

class TimeFormula(Formula):
    def __repr__(self):
        return getTemplate(self.name, y="{startYear}-{endYear}")
    def requires(self, v, s, m, y, d):
        try:
            y1, y2 = y.split('-')
        except ValueError:
            raise Exception(f'Timeseries year must be of format "<start>-<end>", got "{y}"')
        return [
            keyName(self._requires, v, SCENARIOS[0], m, i, d)
            if i < PROJYEAR else
            keyName(self._requires, v, s, m, i, d)
            for i in range(int(y1), int(y2)+1)
        ]

class EnsembleFormula(Formula):
    def __repr__(self):
        return getTemplate(self.name, m=ENSEMBLE, y="{startYear}-{endYear}")
    def yields(self, v, s, m, y, d):
        return keyName(self.name, v, s, ENSEMBLE, y, d)
    def requires(self, v, s, m, y, d):
        return [keyName(self._requires, v, s, m, y, d) for m in MODELS[d]]
