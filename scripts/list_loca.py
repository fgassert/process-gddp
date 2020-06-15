#!/usr/bin/env python3
import ftplib
import os

BASEDIR = 'pub/dcp/archive/cmip5/loca/LOCA_2016-04-02/'
FTPSERVER = 'gdo-dcp.ucllnl.org'

with ftplib.FTP(FTPSERVER) as ftp:
    ftp.login()
    
    # model dirs
    for m in ftp.nlst(BASEDIR):
        mdir = os.path.join(m, '16th')

        # scenario dirs
        for s in ftp.nlst(mdir):
            sdir = ftp.nlst(s)[0]
            if os.path.basename(sdir) != 'r1i1p1':
                print(sdir.split('/')[-4:])

