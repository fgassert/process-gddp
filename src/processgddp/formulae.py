#!/usr/bin/env python

import os
from . import workers

srcTemplate = "http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/{scenario}/day/atmos/{variable}/r1i1p1/v1.0/{variable}_day_BCSD_{scenario}_r1i1p1_{model}_{year}.nc"
fileTemplate = "{function}_{variable}_{scenario}_{model}_{year}.tif"


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

startYear = 1950
projYear = 2006
endYear = 2100

_Formulae = {}

BASELINE='1970-2000'

def init():
    # annual averages
    registerFormula(Formula, name='annual', requires='src', function='mean')

    # extreme values
    registerFormula(Formula, name='q99', requires='src', function='q99')
    registerFormula(Formula2, name='gt-q99', requires='src', function='gt',
                      requiresTemplate=getTemplate(f='abs-q99', y=BASELINE))
    registerFormula(Formula, name='q98', requires='src', function='q98')
    registerFormula(Formula2, name='gt-q98', requires='src', function='gt',
                      requiresTemplate=getTemplate(f='abs-q98', y=BASELINE))

    # moving averages and ensembles for each indicator
    for indicator in ('annual', 'q98', 'q99', 'gt-q99', 'gt-q98'):
        ma = 'abs-{}'.format(indicator)
        diff = 'diff-{}'.format(indicator)
        ch = 'ch-{}'.format(indicator)
        registerFormula(TimeFormula, ma, indicator, 'mean')
        registerFormula(Formula2, diff, ma, 'sub',
                    requiresTemplate=getTemplate(f=ma, y=BASELINE))
        registerFormula(Formula2, ch, ma, 'div',
                    requiresTemplate=getTemplate(f=ma, y=BASELINE))
        for stat in ['mean', 'q25', 'q75', 'q50']:
            registerFormula(EnsembleFormula, requires=ma, function=stat)
            registerFormula(EnsembleFormula, requires=diff, function=stat)
            registerFormula(EnsembleFormula, requires=ch, function=stat)

def getTemplate(f="{function}", v="{variable}", s="{scenario}", m="{model}", y="{year}"):
    return keyName(f, v, s, m, y)

def keyName(f, v, s, m, y):
    if f == 'src':
        return srcName(v, s, m, y)
    return fileTemplate.format(
        function=f, variable=v, scenario=s, model=m, year=str(y))

def srcName(v, s, m, y):
    return srcTemplate.format(
        variable=v, scenario=s, model=m, year=str(y))

def validateKey(key):
    vals = os.path.splitext(key)[0].split('_')
    yrs = vals[4].split('-')
    if not (vals[0] in _Formulae and
            vals[1] in VARIABLES and
            vals[2] in SCENARIOS and
            (vals[3] in MODELS or vals[3] == 'ens') and
            (yrs[0] >= startYear and yrs[0] <= endYear) and
            (len(yrs) == 1 or (yrs[1] >= startYear and yrs[1] <= endYear))):
        raise Exception('Invalid key {}'.format(key))


def parseKey(key):
    vals = os.path.splitext(key)[0].split('_')
    if len(vals) != 5:
        raise Exception('Invalid key {}'.format(key))
    return vals

def getFormula(key):
    f = parseKey(key)[0]
    if f not in _Formulae:
        raise Exception('Formula for {} not defined'.format(f))
    return _Formulae[f]

def listFormulae():
    return _Formulae.keys()

def getParams(key):
    return parseKey(key)[1:]

def _getDepends(key, client, depth=0, skipExisting=False, skipExternal=True):
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
        dependencies.extend(_getDepends(k, client, 0, skipExisting, skipExternal))
    while depth==0 or len(outKeys[0]):
        depthFilter = filter(lambda d: d[1]==depth, dependencies)
        unique = list(set([d[0] for d in depthFilter]))
        outKeys.insert(0, unique)
        depth += 1
    return outKeys

def buildKey(key, options={}):
    return getFormula(key).execute(*getParams(key), options)

def registerFormula(ftype, name=None, requires='src', function='mean', **kwargs):
    if name is None:
        name = '{}-{}'.format(function, requires)
    if name in _Formulae:
        raise Exception("Formula {} already defined".format(name))
    _Formulae[name] = ftype(name, requires, function, **kwargs)

class Formula:
    def __init__(self, name, requires, function, description=''):
        self.name = name
        self.function = function
        self._requires = requires
        self.description = description
    def __repr__(self):
        return getTemplate(self.name)
    def requires(self, v, s, m, y):
        if int(y) < projYear:
            s = SCENARIOS[0]
        return [keyName(self._requires, v, s, m, y)]
    def yields(self, v, s, m, y):
        try:
            if int(y) < projYear:
                s = SCENARIOS[0]
        except:
            pass
        return keyName(self.name, v, s, m, y)
    def execute(self, v, s, m, y, options={}):
        return workers.worker(
            self.yields(v, s, m, y),
            self.requires(v, s, m, y),
            self.function,
            options
        )

class Formula2(Formula):
    def __init__(self, name, requires, function, requiresTemplate, description=''):
        self.name = name
        self.function = function
        self._requires = requires
        self._requires2 = requiresTemplate
    def requires(self, v, s, m, y):
        if int(y) < projYear:
            s = SCENARIOS[0]
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
            if i < projYear else
            keyName(self._requires, v, s, m, i)
            for i in range(int(y1), int(y2)+1)
        ]

class EnsembleFormula(Formula):
    def __repr__(self):
        return getTemplate(self.name, m="ens", y="{startYear}-{endYear}")
    def yields(self, v, s, m, y):
        return keyName(self.name, v, s, 'ens', y)
    def requires(self, v, s, _, y):
        return [keyName(self._requires, v, s, m, y) for m in MODELS]

init()
