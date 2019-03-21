#!/usr/bin/env python

import os
from .Worker import worker
from .formulae import FUNCTIONS

SRCTEMPLATE = "http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/{scenario}/day/atmos/{variable}/r1i1p1/v1.0/{variable}_day_BCSD_{scenario}_r1i1p1_{model}_{year}.nc"
FILETEMPLATE = "{function}_{variable}_{scenario}_{model}_{year}.tif"

SCENARIOS = ["historical","rcp85","rcp45"]
VARIABLES = ["pr","tasmax","tasmin"]
MODELS =    ['ACCESS1-0',
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
ENSEMBLE = 'ens'
SOURCEDATA = 'src'
STARTYEAR = 1950
PROJYEAR = 2006
ENDYEAR = 2100

_Formulae = {}

def getTemplate(f="{function}", v="{variable}", s="{scenario}", m="{model}", y="{year}"):
    return keyName(f, v, s, m, y)

def keyName(f, v, s, m, y):
    if f == SOURCEDATA:
        return srcName(v, s, m, y)
    return FILETEMPLATE.format(
        function=f, variable=v, scenario=s, model=m, year=str(y))

def srcName(v, s, m, y):
    return SRCTEMPLATE.format(
        variable=v, scenario=s, model=m, year=str(y))

def validateKey(key):
    vals = os.path.splitext(key)[0].split('_')
    yrs = vals[4].split('-')
    if not len(vals) == 5:
        raise Exception('Invalid key {}; Must be of format {}'.format(
            key, FILETEMPLATE))
    elif not vals[0] in _Formulae:
        raise Exception('Invalid key {}; Formula {} must be one of {}'.format(
            key, vals[0], ','.join(_Formulae.keys())))
    elif not vals[1] in VARIABLES:
        raise Exception('Invalid key {}; Variable {} must be one of {}'.format(
            key, vals[1], ','.join(VARIABLES)))
    elif not vals[2] in SCENARIOS:
        raise Exception('Invalid key {}; Scenario {} must be one of {}'.format(
            key, vals[2], ','.join(SCENARIOS)))
    elif not (vals[3] in MODELS or vals[3] == ENSEMBLE):
        raise Exception('Invalid key {}; Model {} must be one of {},{}'.format(
            key, vals[3], ','.join(MODELS), ENSEMBLE))
    elif not ((int(yrs[0]) >= STARTYEAR and int(yrs[0]) <= ENDYEAR) and
          (len(yrs) == 1 or (int(yrs[1]) >= STARTYEAR and int(yrs[1]) <= ENDYEAR))):
        raise Exception('Invalid key {}; Year(s) {} must be between {},{}'.format(
            key, yrs, STARTYEAR, ENDYEAR))
    return True

def parseKey(key):
    vals = os.path.splitext(key)[0].split('_')
    return vals

def getFormula(key):
    f = parseKey(key)[0]
    return _Formulae[f]

def getParams(key):
    return parseKey(key)[1:]

def listFormulae():
    return _Formulae.keys()

def _getDepends(key, client, depth=0, skipExisting=False, skipExternal=True, skip=[]):
    if key in skip:
        return []
    if key[:4] == 'http':
        if skipExternal:
            return []
        return [(key, depth)]
    if skipExisting and client.objExists2(key):
        return []
    dependencies = [(key, depth)]
    depends = getFormula(key).requires(*getParams(key))
    for k in depends:
        dependencies.extend(
            _getDepends(k, client, depth+1, skipExisting, skipExternal))
    return dependencies

def dependencyTree(keys, client, skipExisting=False, skipExternal=True):
    '''yeilds depth-first unique dependencies for a given set of task keys'''
    dependencies = []
    outKeys = []
    depth = 0
    for k in keys:
        dependencies.extend(_getDepends(k, client, 0, skipExisting, skipExternal, dependencies))
    while depth==0 or len(outKeys[0]):
        depthFilter = filter(lambda d: d[1]==depth, dependencies)
        unique = list(set([d[0] for d in depthFilter]))
        outKeys.insert(0, unique)
        depth += 1
    return outKeys

def registerFormula(ftype, name=None, requires=SOURCEDATA, function='mean', **kwargs):
    if name is None:
        name = '{}-{}'.format(function, requires)
    if name in _Formulae:
        raise Exception("Formula {} already defined".format(name))
    if function not in FUNCTIONS:
        raise Exception("Function {} does not exist".format(function))
    _Formulae[name] = ftype(name, requires, function, **kwargs)

def buildKey(key, options={}):
    return getFormula(key).execute(*getParams(key), options)

class Formula:
    def __init__(self, name, requires, function, description=''):
        self.name = name
        self.function = function
        self._requires = requires
        self.description = description
    def __repr__(self):
        return getTemplate(self.name)
    def requires(self, v, s, m, y):
        if int(y) < PROJYEAR:
            s = SCENARIOS[0]
        return [keyName(self._requires, v, s, m, y)]
    def yields(self, v, s, m, y):
        try:
            if int(y) < PROJYEAR:
                s = SCENARIOS[0]
        except:
            pass
        return keyName(self.name, v, s, m, y)
    def execute(self, v, s, m, y, options={}):
        return worker(
            self.yields(v, s, m, y),
            self.requires(v, s, m, y),
            self.function,
            options
        )

class Formula2(Formula):
    def __init__(self, name, requires, function, requires2, description=''):
        self.name = name
        self.function = function
        self._requires = requires
        self._requires2 = requires2
    def requires(self, v, s, m, y):
        try:
            if int(y) < PROJYEAR:
                s = SCENARIOS[0]
        except:
            pass
        return [
            keyName(self._requires, v, s, m, y),
            self._requires2.format(variable=v, scenario=s, model=m, year=y)
        ]

class TimeFormula(Formula):
    def __repr__(self):
        return getTemplate(self.name, y="{startYear}-{endYear}")
    def requires(self, v, s, m, y):
        y1, y2 = y.split('-')
        return [
            keyName(self._requires, v, SCENARIOS[0], m, i)
            if i < PROJYEAR else
            keyName(self._requires, v, s, m, i)
            for i in range(int(y1), int(y2)+1)
        ]

class EnsembleFormula(Formula):
    def __repr__(self):
        return getTemplate(self.name, m=ENSEMBLE, y="{startYear}-{endYear}")
    def yields(self, v, s, m, y):
        return keyName(self.name, v, s, ENSEMBLE, y)
    def requires(self, v, s, _, y):
        return [keyName(self._requires, v, s, m, y) for m in MODELS]
