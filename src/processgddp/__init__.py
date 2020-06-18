#!/usr/bin/env python

import os
import multiprocessing as mp
import logging
from functools import partial
from . import FileHandler
from . import DependencyHandler
from . import formulae

formulae.registerFormulae()

PREFIX = os.getenv('GDDP_PREFIX', 'gddp')
BUCKET = os.getenv('GDDP_BUCKET', 'gddp')
ACCESSKEY = os.getenv('AWS_ACCESS_KEY_ID') or os.getenv('AWS_KEY')
SECRETKEY = os.getenv('AWS_SECRET_ACCESS_KEY') or os.getenv('AWS_SECRET')
OPTIONS = {
    'nocache':True,
    'bucket':BUCKET,
    'prefix':PREFIX,
    'access_key':ACCESSKEY,
    'secret':SECRETKEY,
    'cachedir':'_cache',
    'verbose':False
}

def build(objs, skipExisting=True, options=OPTIONS, poolargs={}):
    ''''''
    client = FileHandler.Client(**options)
    tree = DependencyHandler.dependencyTree(objs, client, skipExisting, poolargs)
    return tree.build(options=options)

def build_async(objs, skipExisting=True, options=OPTIONS, poolargs={}):
    '''executes the formulae for each key in parallel'''
    client = FileHandler.Client(**options)
    client.cleanInvalidBucketObjs()
    tree = DependencyHandler.dependencyTree(objs, client, skipExisting, poolargs)
    return tree.build_async(options=options)

def main(keys, options=OPTIONS):
    if options['verbose']:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    if keys:
        build_async(keys)
    else:
        test()

def test():
    logging.basicConfig(level=logging.DEBUG)
    keys=[]
    keys.append(DependencyHandler.keyName('gt-q99', 'pr', 'rcp85', 'ACCESS1-0', '2000'))
    keys.append(DependencyHandler.keyName('abs-drydays', 'pr', 'rcp85', 'ACCESS1-0', '2001-2002'))
    keys.append(DependencyHandler.keyName('mean-abs-annual', 'pr', 'rcp85', 'ens', '2000-2001'))
    build_async(keys, False)

if __name__ == "__main__":
    test()
