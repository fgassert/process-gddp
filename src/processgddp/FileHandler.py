#!/usr/bin/env python

import boto3
import os
import logging
import signal
import requests
import time


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
        if isHttp:
            fname = self.cached(os.path.basename(obj))
        else:
            fname = self.cached(obj)
        tmpname = self.cached(str(hash(fname)))

        TIMEOUT = 360
        if os.path.isfile(tmpname):
            logging.info("File download in process.")
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
            logging.info("Fetching {}".format (obj))
            try:
                if isHttp:
                    session = requests.session()
                    with session.get(obj, stream=True) as r:
                        r.raise_for_status()
                        with open(tmpname, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=4096):
                                if chunk:
                                    f.write(chunk)
                else:
                    objpath = os.path.join(self.prefix, obj)
                    self.client.Bucket(self.bucket).download_file(objpath, tmpname)
                os.rename(tmpname, fname)
            except SystemExit:
                logging.info('Exiting gracefully {}'.format(fname))
                os.remove(tmpname)
        return fname

    def putObj(self, fname, obj):
        logging.info("Putting {}".format(obj))
        objpath = os.path.join(self.prefix, obj)
        self.client.Bucket(self.bucket).upload_file(fname, objpath)

    def cleanObjs(self, objs):
        if type(objs) not in (list, tuple):
            objs = [objs]
        for o in objs:
            try:
                logging.info("Cleaning {}".format(o))
                fname = self.cached(obj)
                os.remove(fname)
            except:
                pass

    def clean(self):
        if os.path.isfile(self.cachedir):
            for f in os.listdir():
                os.remove(f)

    def cached(self, obj):
        return os.path.join(self.cachedir, obj)

if __name__ == "__main__":
    pass
