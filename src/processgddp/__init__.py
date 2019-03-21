#!/usr/bin/env python

import os
import multiprocessing as mp
import logging
from functools import partial
from . import FileHandler
from . import DependencyHandler
from . import formulae

formulae.registerFormulae()

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

logging.basicConfig(level=logging.INFO)

def build(objs, skipExisting=True, options=OPTIONS):
    ''''''
    client = FileHandler.Client(**options)
    for keys in DependencyHandler.dependencyTree(objs, client, skipExisting):
        msgs = map(
            partial(f.buildKey, options=options),
            keys
        )
        for m in msgs:
            if m:
                print (m)

def build_async(objs, skipExisting=True, options=OPTIONS, threads=None, timeout=604800):
    '''executes the formulae for each key in parallel'''
    client = FileHandler.Client(**options)
    pool = mp.Pool(threads)
    for keys in DependencyHandler.dependencyTree(objs, client, skipExisting):
        print("Tasks in level: {}".format(len(keys)))
        msgs = pool.map_async(
            partial(DependencyHandler.buildKey, options=options),
            keys
        ).get(timeout)
        for m in msgs:
            if m:
                print (m)
    pool.close()


def printDependencies(keys):
    client = FileHandler.Client(**OPTIONS)
    print ("Dependencies:")
    for dependencies in DependencyHandler.dependencyTree(keys, client, skipExternal=False):
        for d in dependencies:
            print (d)


def main(keys):
    for key in keys:
        DependencyHandler.validateKey(key)
    #printDependencies(keys)
    build_async(keys)

def test():
    keys=[]
    keys.append(DependencyHandler.keyName('gt-q99', 'pr', 'rcp85', 'ACCESS1-0', '2000'))
    keys.append(DependencyHandler.keyName('mean-abs-annual', 'pr', 'rcp85', 'ens', '2000-2001'))
    build(keys)

if __name__ == "__main__":
    test()
