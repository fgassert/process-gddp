#!/usr/bin/env python
import numpy as np

BASELINE='1960-1990'

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

def dailymean(arr):
    return (arr[:arr.shape[0]//2] + arr[arr.shape[0]//2:]) / 2
def cdd(v):
    return lambda arr: np.nansum(np.where(arr>v, arr-v, 0), axis=0, keepdims=True)
def cdd_tmin_tmax(v):
    return lambda arr: cdd(v)(dailymean(arr))
def hdd(v):
    return lambda arr: np.nansum(np.where(arr<v, v-arr, 0), axis=0, keepdims=True)
def hdd_tmin_tmax(v):
    return lambda arr: hdd(v)(dailymean(arr))

def subtractArr(arr):
    return arr[:-1]-arr[-1]
def divideArr(arr):
    return arr[:-1]/arr[-1]
def countAboveArr(arr):
    return np.nansum(np.where(arr[:-1]>arr[-1], 1, 0), axis=0, keepdims=True)


def _maxruns(difs):
    run_starts, = np.where(difs > 0)
    run_ends, = np.where(difs < 0)
    if len(run_starts):
        return (run_ends - run_starts).max()
    else:
        return 0

def maxRun2(arr):
    shape = (1, *arr.shape[1:])
    zeros = np.zeros(shape)
    bounded = np.concatenate((zeros, arr, arr, zeros), axis=0)
    difs = np.diff(bounded, axis=0)
    out = np.empty(shape)
    for i,j in np.ndindex(difs.shape[1:]):
        out[0,i,j] = _maxruns(difs[:,i,j])
    return out

def maxRun(arr):
    out = np.empty((1, *arr.shape[1:]))
    for i,j in np.ndindex(arr.shape[1:]):
        bounded = np.concatenate(([0], arr[:,i,j], arr[:,i,j], [0]))
        difs = np.diff(bounded)
        out[0,i,j] = _maxruns(difs)
    return out

def _spells(arr, min_length):
    out = np.empty((1, *arr.shape[1:]))
    for i,j in np.ndindex(arr.shape[1:]):
        bounded = np.concatenate(([0], arr[:,i,j], arr[:min_length,i,j], [0]))
        difs = np.diff(bounded)
        run_starts, = np.where(difs > 0)
        run_ends, = np.where(difs < 0)
        if len(run_starts):
            run_lengths = run_ends - run_starts
            out[0,i,j] = run_lengths[run_lengths >= min_length].sum() / min_length
        else:
            out[0,i,j] = 0
    return out

def drydays(arr):
    return np.clip(maxRun(arr<mm2kgs(1)), 0, 365)
def frostfree(arr):
    return np.clip(maxRun(arr>c2k(0)), 0, 365)
def dryspells(min_length):
    return lambda arr: _spells(arr<mm2kgs(1), min_length)


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
def c2f_rel(c):
    return c*9/5

def mm2kgs(mm):
    return mm/86400
def kgs2mm(kgs):
    return kgs*86400
def kgs2mmyr(kgs):
    return kgs*86400*365

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
    'gt85f':countAbove(f2k(85)),
    'gt32f':countAbove(f2k(32)),
    'gt50mm':countAbove(mm2kgs(50)),
    'dailymean':dailymean,
    'hdd65f':hdd(f2k(65)),
    'cdd65f':cdd(f2k(65)),
    'hdd16c':hdd(c2k(16)),
    'cdd16c':cdd(c2k(16)),
    'hdd65f_tmin_tmax':hdd_tmin_tmax(f2k(65)),
    'cdd65f_tmin_tmax':cdd_tmin_tmax(f2k(65)),
    'hdd16c_tmin_tmax':hdd_tmin_tmax(c2k(16)),
    'cdd16c_tmin_tmax':cdd_tmin_tmax(c2k(16)),
    'mmday':kgs2mm,
    'degc':k2c,
    'degf':k2f,
    'reldegf':c2f_rel,
    'drydays':drydays,
    'frostfree':frostfree,
    'dryspells':dryspells(5)
}

def registerFormulae():
    from . import DependencyHandler as dh

    # annual averages
    dh.registerFormula(dh.Formula, name='annual', requires='src', function='mean')

    # extreme values
    dh.registerFormula(dh.Formula, name='gt50mm', requires='src', function='gt50mm')
    dh.registerFormula(dh.Formula, name='gt95f', requires='src', function='gt95f')
    dh.registerFormula(dh.Formula, name='gt90f', requires='src', function='gt90f')
    dh.registerFormula(dh.Formula, name='gt85f', requires='src', function='gt85f')
    dh.registerFormula(dh.Formula, name='gt32f', requires='src', function='gt32f')

    dh.registerFormula(dh.Formula, name='q99', requires='src', function='q99')
    dh.registerFormula(dh.Formula2, name='gt-q99', requires='src', function='gt',
                      requires2={'f':'abs-q99', 's':'historical', 'y':BASELINE})
    dh.registerFormula(dh.Formula, name='q98', requires='src', function='q98')
    dh.registerFormula(dh.Formula2, name='gt-q98', requires='src', function='gt',
                      requires2={'f':'abs-q98', 's':'historical', 'y':BASELINE})

    # streaks
    dh.registerFormula(dh.Formula, name='drydays', requires='src', function='drydays')
    dh.registerFormula(dh.Formula, name='frostfree', requires='src', function='frostfree')
    dh.registerFormula(dh.Formula, name='dryspells', requires='src', function='dryspells')

    # tavg (averages in tmin)
    dh.registerFormula(dh.Formula2, name='tavg-tasmin', requires='annual', function='mean',
                       requires2={'f':'annual', 'v':'tasmin'})
    dh.registerFormula(dh.Formula2, name='hdd65f-tasmin', requires='src', function='hdd65f_tmin_tmax',
                       requires2={'f':'src', 'v':'tasmin'})
    dh.registerFormula(dh.Formula2, name='cdd65f-tasmin', requires='src', function='cdd65f_tmin_tmax',
                       requires2={'f':'src', 'v':'tasmin'})

    # moving averages and ensembles for each indicator
    for indicator in ('annual', 'q98', 'q99', 'gt-q99',
                      'gt-q98', 'gt50mm', 'gt95f', 'gt32f',
                      'frostfree', 'drydays', 'gt85f', 'gt90f',
                      'hdd65f-tasmin', 'cdd65f-tasmin', 
                      'tavg-tasmin', 'dryspells'
    ):
        ma = f'abs-{indicator}'
        diff = f'diff-{indicator}'
        ch = f'ch-{indicator}'
        dh.registerFormula(dh.TimeFormula, ma, indicator, 'mean')
        dh.registerFormula(dh.Formula2, diff, ma, 'sub',
                    requires2={'f':ma, 's':'historical', 'y':BASELINE})
        dh.registerFormula(dh.Formula2, ch, ma, 'div',
                    requires2={'f':ma, 's':'historical', 'y':BASELINE})
        for series in [ma, diff, ch]:
            for stat in ['mean', 'q25', 'q75', 'q50']:
                dh.registerFormula(dh.EnsembleFormula, f"{stat}-{series}", requires=series, function=stat)
            dh.registerFormula(dh.Formula2, f"iqr-{series}", requires=f"q75-{series}", function='sub',
                               requires2={'f': f'q25-{series}'})
