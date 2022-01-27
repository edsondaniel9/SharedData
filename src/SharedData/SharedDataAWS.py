import os
import subprocess
import boto3

from SharedData.Logger import Logger



def S3SyncDownloadTimeSeries(path,shm_name):    
    Logger.log.debug('AWS sync download timeseries %s...' % (shm_name))       
    awsclipath = os.environ['AWSCLI_PATH']
    awsfolder = os.environ['S3_BUCKET']+'/'+shm_name+'/' 
    process = subprocess.Popen([awsclipath,'s3','sync',awsfolder,str(path),\
        '--profile','s3readonly',\
        #'--delete',\
        '--exclude=shm_info.json'],\
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    latchcompleted=False #dont display sequenced messages starting with "Completed"
    while True:
        output = process.stdout.readline()
        if ((output == '') | (output == b''))\
                & (process.poll() is not None):
            break            
        if output:                                
            _output=output.strip().replace('\r','\r\n')
            if not latchcompleted:
                latchcompleted = False
                Logger.log.debug('AWSCLI:'+_output)                    
            latchcompleted = ('Completed' in _output)                
    Logger.log.debug('AWS sync download timeseries %s DONE!' % (shm_name))    
    rc = process.poll()
    return rc==0        


def S3SyncDownloadMetadata(pathpkl,name):
    Logger.log.debug('AWS sync download metadata %s...' % (name))
    folder=str(pathpkl.parents[0]).replace(\
        os.environ['DATABASE_FOLDER'],'')
    folder = folder.replace('\\','/')+'/'
    dbfolder = str(pathpkl.parents[0])
    dbfolder = dbfolder.replace('\\','/')+'/'
    awsfolder = os.environ['S3_BUCKET'] + folder
    awsclipath = os.environ['AWSCLI_PATH']
    process = subprocess.Popen([awsclipath,'s3','sync',awsfolder,dbfolder,\
        '--profile','s3readonly',\
        '--exclude','*',\
        '--include',name.split('/')[-1]+'.pkl',\
        '--include',name.split('/')[-1]+'_SYMBOLS.pkl',
        '--include',name.split('/')[-1]+'_SERIES.pkl',
        '--include',name.split('/')[-1]+'.xlsx'],\
        #'--delete'],\
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)        
    latchcompleted=False #dont display sequenced messages starting with "Completed"
    while True:
        output = process.stdout.readline()
        if ((output == '') | (output == b''))\
                & (process.poll() is not None):
            break
        if output:
            _output=output.strip().replace('\r','\r\n')
            if not latchcompleted:
                latchcompleted = False
                Logger.log.debug('AWSCLI:'+_output)                    
            latchcompleted = ('Completed' in _output)           

    Logger.log.debug('AWS sync download metadata %s DONE!' % (name))
    rc = process.poll()
    return rc==0

def S3Upload(localfilepath):
    Logger.log.debug('Uploading to S3 '+localfilepath+' ...')
    try:
        remotefilepath = localfilepath.replace(\
            os.environ['DATABASE_FOLDER'],os.environ['S3_BUCKET'])         
        session = boto3.Session(profile_name='s3readwrite')   
        s3 = session.resource('s3')
        s3.Bucket(os.environ['S3_BUCKET']).upload_file(localfilepath,remotefilepath)
        Logger.log.debug('Uploading to S3 '+localfilepath+' DONE!')
    except Exception as e:
        Logger.log.error('Uploading to S3 '+localfilepath+' ERROR! %s' % str(e))
        
