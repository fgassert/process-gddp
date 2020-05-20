#!/usr/bin/env python

import boto3
import os
import logging
import signal
import urllib
import time
import random
from pathlib import Path


def terminate(signum, frame):
    raise SystemExit

signal.signal(signal.SIGTERM, terminate)
signal.signal(signal.SIGINT, terminate)

class Client:
    def __init__(self, cachedir='_cache', bucket='', prefix='', access_key=None, secret=None, **args):
        self.cachedir = cachedir
        os.makedirs(self.cachedir, exist_ok=True)
        self.bucket = bucket
        self.prefix = prefix
        self.session = boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret
        )
        self.client = self.session.resource('s3')
        self.existingObjects = []

    def checkCache(self, obj):
        if len(obj) > 4 and obj[:4]=="http":
            obj = os.path.basename(obj)
        return os.path.isfile(self.cached(obj))

    def objExists(self, obj, nocache=False):
        if not nocache and self.checkCache(obj):
            logging.debug('Found cached {}'.format(obj))
            return True
        elif len(obj) > 4 and obj[:4]=="http":
            return True
        key = os.path.join(self.prefix, obj)
        try:
            self.client.Bucket(self.bucket).Object(key).load()
            logging.debug('Found remote {}'.format(obj))
            return True
        except ClientError as e:
            logging.debug('Not found {}'.format(obj))
            return False

    def objExists2(self, obj, nocache=False):
        if not nocache and self.checkCache(obj):
            logging.debug('Found cached {}'.format(obj))
            return True
        elif len(obj) > 4 and obj[:4]=="http":
            return True
        key = os.path.join(self.prefix, obj)
        if not self.existingObjects:
            objs = self.client.Bucket(self.bucket).objects.filter(Prefix=self.prefix)
            self.existingObjects = [o.key for o in objs]
        if key in self.existingObjects:
            logging.debug('Found remote {}'.format(obj))
            return True
        logging.debug('Not found {}'.format(obj))
        return False

    def getObj(self, obj, nocache=False):
        isHttp = (len(obj) > 4 and obj[:4]=="http")
        fname = os.path.basename(obj) if isHttp else obj
        if nocache:
            fname = str(hash(random.random())) + fname
        fname = self.cached(fname)
        tmpname = self.cached(str(hash(fname)))

        TIMEOUT = 3600
        if os.path.isfile(tmpname):
            logging.debug("File download in process.")
            wait = 0
            while os.path.isfile(tmpname):
                time.sleep(1)
                wait += 1
                if wait > TIMEOUT:
                    logging.error(f"Was waiting for {fname}, but timed out.")
                    try:
                        os.remove(tmpname)
                    except:
                        pass

        if not os.path.isfile(fname):
            logging.debug("Fetching {}".format(obj))
            Path(tmpname).touch()
            try:
                if isHttp:
                    urllib.request.urlretrieve(obj, tmpname)
                else:
                    objpath = os.path.join(self.prefix, obj)
                    self.client.Bucket(self.bucket).download_file(objpath, tmpname)
                os.rename(tmpname, fname)
            except SystemExit:
                logging.debug('Exiting gracefully {}'.format(fname))
                try:
                    os.remove(tmpname)
                except:
                    pass
        return fname

    def putObj(self, fname, obj):
        logging.debug("Putting {}".format(obj))
        objpath = os.path.join(self.prefix, obj)
        try:
            self.client.Bucket(self.bucket).upload_file(fname, objpath)
        except SystemExit:
            try:
                self.client.Bucket(self.bucket).Object(objpath).delete()
            except:
                pass

    def cleanObjs(self, objs):
        if type(objs) not in (list, tuple):
            objs = [objs]
        for o in objs:
            try:
                os.remove(o)
                logging.debug("Cleaning {}".format(o))
            except:
                pass

    def clean(self):
        if os.path.isfile(self.cachedir):
            for f in os.listdir():
                os.remove(f)

    def cleanInvalidBucketObjs(self):
        objs = self.client.Bucket(self.bucket).objects.filter(Prefix=self.prefix)
        for o in objs:
            if o.size == 0:
                logging.info(f'{o.key} was empty, deleting')
                o.delete()


    def cached(self, obj):
        return os.path.join(self.cachedir, obj)

if __name__ == "__main__":
    pass
