import os
import logging
import subprocess
import boto3
import awscli
from pathlib import Path
import pandas as pd
from datetime import datetime,timedelta
import time
import pytz

from SharedData.Logger import Logger

S3_BUCKET='s3://deepportfolio'

def S3SyncDownloadDataFrame(path,shm_name):
    Logger.log.debug('AWS S3 Sync DataFrame %s...' % (shm_name))

    awsfolder = os.environ['S3_BUCKET']+'/'
    awsfolder = awsfolder+str(Path(shm_name).parents[0])
    awsfolder = awsfolder.replace('\\','/')+'/'   
    dbfolder = str(path)
    dbfolder = dbfolder.replace('\\','/')+'/'

    process = subprocess.Popen(['aws','s3','sync',awsfolder,dbfolder,\
        '--profile','s3readonly',\
        '--exclude','*',\
        '--include',shm_name.split('/')[-1]+'.json',\
        '--include',shm_name.split('/')[-1]+'.npy'],\
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,\
        universal_newlines=True, shell=True)        
    
    while True:
        output = process.stdout.readline()
        if ((output == '') | (output == b''))\
                & (process.poll() is not None):
            break        
        if (output) and not (output.startswith('Completed')):
            Logger.log.debug('AWSCLI:'+output.strip())  

    rc = process.poll()
    success= rc==0
    if success:
        Logger.log.debug('AWS S3 Sync DataFrame %s DONE!' % (shm_name))
    else:
        Logger.log.error('AWS S3 Sync DataFrame %s ERROR!' % (shm_name))
    return success

def S3SyncDownloadTimeSeries(path,shm_name):    
    Logger.log.debug('AWS S3 sync download timeseries %s...' % (shm_name))           
    awsfolder = os.environ['S3_BUCKET']+'/'+shm_name+'/' 
    process = subprocess.Popen(['aws','s3','sync',awsfolder,path,\
        '--profile','s3readonly',\
        #'--delete',\
        '--exclude=shm_info.json'],\
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,\
        universal_newlines=True, shell=True)
    
    while True:
        output = process.stdout.readline()
        if ((output == '') | (output == b''))\
                & (process.poll() is not None):
            break    
        if (output) and not (output.startswith('Completed')):
            Logger.log.debug('AWSCLI:'+output.strip())        

    rc = process.poll()
    success= rc==0
    if success:
        Logger.log.debug('AWS S3 Sync timeseries %s DONE!' % (shm_name))
    else:
        Logger.log.error('AWS S3 Sync timeseries %s ERROR!' % (shm_name))
    return success

def S3SyncDownloadMetadata(pathpkl,name):
    Logger.log.debug('AWS S3 Sync metadata %s...' % (name))
    folder=str(pathpkl.parents[0]).replace(\
        os.environ['DATABASE_FOLDER'],'')
    folder = folder.replace('\\','/')+'/'
    dbfolder = str(pathpkl.parents[0])
    dbfolder = dbfolder.replace('\\','/')+'/'
    awsfolder = os.environ['S3_BUCKET'] + folder
    process = subprocess.Popen(['aws','s3','sync',awsfolder,dbfolder,\
        '--profile','s3readonly',\
        '--exclude','*',\
        '--include',name.split('/')[-1]+'.pkl',\
        '--include',name.split('/')[-1]+'_SYMBOLS.pkl',\
        '--include',name.split('/')[-1]+'_SERIES.pkl',\
        '--include',name.split('/')[-1]+'.xlsx'],\
        #'--delete'],\
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,\
        universal_newlines=True, shell=True)        
    
    while True:
        output = process.stdout.readline()
        if ((output == '') | (output == b''))\
                & (process.poll() is not None):
            break        
        if (output) and not (output.startswith('Completed')):
            Logger.log.debug('AWSCLI:'+output.strip())  
            
    rc = process.poll()
    success= rc==0
    if success:
        Logger.log.debug('AWS S3 Sync metadata %s DONE!' % (name))
    else:
        Logger.log.error('AWS S3 Sync metadata %s ERROR!' % (name))
    return success



def S3Upload(localfilepath):
    Logger.log.debug('Uploading to S3 '+str(localfilepath)+' ...')
    try:        
        remotefilepath = str(localfilepath).replace(\
            os.environ['DATABASE_FOLDER'],os.environ['S3_BUCKET'])   
        remotefilepath = remotefilepath.replace('\\','/')
        localfilepath = str(localfilepath).replace('\\','/')        
        session = boto3.Session(profile_name='s3readwrite')   
        s3 = session.resource('s3')
        bucket = s3.Bucket(os.environ['S3_BUCKET'].replace('s3://',''))
        bucket.upload_file(localfilepath,remotefilepath.replace(os.environ['S3_BUCKET'],'')[1:])
        Logger.log.debug('Uploading to S3 '+localfilepath+' DONE!')
    except Exception as e:
        Logger.log.error('Uploading to S3 '+localfilepath+' ERROR! %s' % str(e))        