#!/usr/bin/env python

import os
import multiprocessing as mp
import logging
from functools import partial
from . import util
from . import formulae as f

PREFIX = os.environ.get('GDDP_PREFIX', 'gddp')
BUCKET = os.environ.get('GDDP_BUCKET', 'gddp')
OPTIONS = {
    'nocache':False,
    'bucket':BUCKET,
    'prefix':PREFIX,
    'access_key':None,
    'secret':None,
    'cachedir':'_cache',
    'verbose':True
}

def build(objs, skipExisting=True, options=OPTIONS):
    '''executes the formulae for each key in parallel'''
    client = util.Client(**options)
    for keys in f.dependencyTree(objs, client, skipExisting):
        msgs = map(
            partial(f.buildKey, options=options),
            keys
        )
        for m in msgs:
            if m:
                print (m)

def build_async(objs, skipExisting=True, options=OPTIONS, threads=None, timeout=604800):
    '''executes the formulae for each key in parallel'''
    client = util.Client(**options)
    pool = mp.Pool(threads)
    for keys in f.dependencyTree(objs, client, skipExisting):
        results = pool.map_async(
            partial(f.buildKey, options=options),
            keys
        ).get(timeout)
        for m in msgs:
            if m:
                print (m)
    pool.close()


def printDependencies(keys):
    client = util.Client(**OPTIONS)
    for k in f.dependencyTree(keys, client, skipExternal=False):
        print (k)


def main(*args):
    logging.basicConfig(level=logging.INFO)
    for key in args:
        f.validateKey(key)
    printDependencies(*args)
    build(*args)

def test():
    logging.basicConfig(level=logging.INFO)
    #key = f.keyName('mean-ma-gt-q99', 'pr', 'rcp85', 'ens', '2035-2065')
    #printDependencies([key])
    keys=[]
    keys.append(f.keyName('gt-q99', 'pr', 'rcp85', 'ACCESS1-0', '2000'))
    keys.append(f.keyName('mean-abs-annual', 'pr', 'rcp85', 'ens', '2000-2001'))
    build(keys)

if __name__ == "__main__":
    test()
