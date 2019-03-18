#!/usr/bin/env python

import boto3
import os
import urllib
import logging
import signal


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
        self.client = boto3.resource('s3', aws_access_key_id=access_key,
                                     aws_secret_access_key=secret)
        self.existingObjects = []

    def checkCache(self, obj):
        if len(obj) > 4 and obj[:4]=="http":
            obj = os.path.basename(obj)
        return os.path.isfile(self.cached(obj))

    def objExists(self, obj, nocache=False):
        if not nocache and self.checkCache(obj):
            logging.info('Found cached {}'.format(obj))
            return True
        elif len(obj) > 4 and obj[:4]=="http":
            return True
        key = os.path.join(self.prefix, obj)
        try:
            self.client.Bucket(self.bucket).Object(key).load()
            logging.info('Found remote {}'.format(obj))
            return True
        except ClientError as e:
            logging.info('Not found {}'.format(obj))
            return False

    def objExists2(self, obj, nocache=False):
        if not nocache and self.checkCache(obj):
            logging.info('Found cached {}'.format(obj))
            return True
        elif len(obj) > 4 and obj[:4]=="http":
            return True
        key = os.path.join(self.prefix, obj)
        if not self.existingObjects:
            objs = self.client.Bucket(self.bucket).objects.filter(Prefix=self.prefix)
            self.existingObjects = [o.key for o in objs]
        if key in self.existingObjects:
            logging.info('Found remote {}'.format(obj))
            return True
        logging.info('Not found {}'.format(obj))
        return False

    def getObj(self, obj, nocache=False):
        if len(obj) > 4 and obj[:4]=="http":
            fname = self.cached(os.path.basename(obj))
            if nocache or not os.path.isfile(fname):
                logging.info("Fetching {}".format (obj))
                try:
                    urllib.request.urlretrieve(obj, fname)
                except SystemExit:
                    logging.info('Exiting gracefully {}'.format(fname))
                    os.remove(fname)
            else:
                logging.info("Using cached {}".format(obj))
        else:
            fname = self.cached(obj)
            if nocache or not os.path.isfile(fname):
                logging.info("Fetching {}".format (obj))
                objpath = os.path.join(self.prefix, obj)
                try:
                    self.client.Bucket(self.bucket).download_file(objpath, fname)
                except SystemExit:
                    logging.info('Exiting gracefully {}'.format(fname))
                    os.remove(fname)
            else:
                logging.info("Using cached {}".format(obj))
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
