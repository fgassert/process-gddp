#!/usr/bin/env python
import numpy as np

BASELINE='1970-2000'

def bandMean(arr):
    return np.nanmean(arr, axis=0, keepdims=True)
def bandSum(arr):
    return np.nansum(arr, axis=0, keepdims=True)

def percentile(q):
    return lambda arr: np.nanpercentile(arr, q, axis=0, keepdims=True)
def countAbove(v):
    return lambda arr: np.nansum(np.where(arr>v, 1, 0) , axis=0, keepdims=True)
def scale(s):
    return lambda arr: arr*s

def hdd(v):
    return lambda arr: np.nansum(np.where(arr>v, arr-v, 0), axis=0, keepdims=True)
def cdd(v):
    return lambda arr: np.nansum(np.where(arr<v, v-arr, 0), axis=0, keepdims=True)

def subtractArr(arr):
    return arr[:-1]-arr[-1]
def divideArr(arr):
    return arr[:-1]/arr[-1]
def countAboveArr(arr):
    return np.nansum(np.where(arr[:-1]>arr[-1], 1, 0), axis=0, keepdims=True)

def _runs(difs):
    run_starts, = np.where(difs > 0)
    run_ends, = np.where(difs < 0)
    return (run_ends - run_starts).max()

def maxRun(arr):
    shape = (1, *arr.shape[1:])
    zeros = np.zeros(shape)
    bounded = np.concatenate((zeros, arr, arr, zeros), axis=0)
    difs = np.diff(bounded, axis=0)
    out = np.empty(shape)
    for i,j in np.ndindex(difs.shape[1:]):
        out[0,i,j] = _runs(difs[:,i,j])
    return out

def drydays(arr):
    return np.clip(maxRun(arr<mm2kgs(1)), 0, 365)
def frostfree(arr):
    return np.clip(maxRun(arr>c2k(0)), 0, 365)

def wrap(f):
    return lambda arr: f(arr)

def f2k(f):
    return c2k(f2c(f))
def f2c(f):
    return (f-32)*5/9
def c2k(c):
    return c+273.15
def k2c(k):
    return k-273.15
def k2f(k):
    return c2f(k2c(k))
def c2f(c):
    return c*9/5+32

def mm2kgs(mm):
    return mm/86400
def kgs2mm(kgs):
    return kgs*86400

FUNCTIONS = {
    'mean': bandMean,
    'sum': bandSum,
    'sub': subtractArr,
    'div': divideArr,
    'gt':countAboveArr,
    'q25':percentile(25),
    'q50':percentile(50),
    'q75':percentile(75),
    'q98':percentile(98),
    'q99':percentile(99),
    'gt95f':countAbove(f2k(95)),
    'gt90f':countAbove(f2k(90)),
    'gt32f':countAbove(f2k(32)),
    'gt50mm':countAbove(mm2kgs(50)),
    'hdd65f':hdd(f2k(65)),
    'cdd65f':cdd(f2k(65)),
    'hdd16c':hdd(c2k(16)),
    'cdd16c':cdd(c2k(16)),
    'mmday':kgs2mm,
    'degc':k2c,
    'degf':k2f,
    'drydays':drydays,
    'frostfree':frostfree,
}

def registerFormulae():
    from . import DependencyHandler as dh

    # annual averages
    dh.registerFormula(dh.Formula, name='annual', requires='src', function='mean')

    # extreme values
    dh.registerFormula(dh.Formula, name='gt50mm', requires='src', function='gt50mm')
    dh.registerFormula(dh.Formula, name='gt95f', requires='src', function='gt95f')
    dh.registerFormula(dh.Formula, name='gt32f', requires='src', function='gt32f')

    dh.registerFormula(dh.Formula, name='q99', requires='src', function='q99')
    dh.registerFormula(dh.Formula2, name='gt-q99', requires='src', function='gt',
                      requires2=dh.getTemplate(f='abs-q99', s='historical', y=BASELINE))
    dh.registerFormula(dh.Formula, name='q98', requires='src', function='q98')
    dh.registerFormula(dh.Formula2, name='gt-q98', requires='src', function='gt',
                      requires2=dh.getTemplate(f='abs-q98', s='historical', y=BASELINE))

    # streaks
    dh.registerFormula(dh.Formula, name='drydays', requires='src', function='drydays')
    dh.registerFormula(dh.Formula, name='frostfree', requires='src', function='frostfree')

    # moving averages and ensembles for each indicator
    for indicator in ('annual', 'q98', 'q99', 'gt-q99',
                      'gt-q98', 'gt50mm', 'gt95f', 'gt32f',
                      'frostfree', 'drydays'
    ):
        ma = 'abs-{}'.format(indicator)
        diff = 'diff-{}'.format(indicator)
        ch = 'ch-{}'.format(indicator)
        dh.registerFormula(dh.TimeFormula, ma, indicator, 'mean')
        dh.registerFormula(dh.Formula2, diff, ma, 'sub',
                    requires2=dh.getTemplate(f=ma, s='historical', y=BASELINE))
        dh.registerFormula(dh.Formula2, ch, ma, 'div',
                    requires2=dh.getTemplate(f=ma, s='historical', y=BASELINE))
        for stat in ['mean', 'q25', 'q75', 'q50']:
            dh.registerFormula(dh.EnsembleFormula, "{}-{}".format(stat, ma), requires=ma, function=stat)
            dh.registerFormula(dh.EnsembleFormula, "{}-{}".format(stat, diff), requires=diff, function=stat)
            dh.registerFormula(dh.EnsembleFormula, "{}-{}".format(stat, ch), requires=ch, function=stat)
