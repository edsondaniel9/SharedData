import awscli
import subprocess
import os,sys

os.environ['PATH'] = sys.exec_prefix+r'\Scripts'

# process = subprocess.Popen(['set'],
#     stdout=subprocess.PIPE, stderr=subprocess.PIPE,\
#     universal_newlines=True, shell=True)        

process = subprocess.Popen(['aws','s3','ls','--profile=s3readonly'],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE,\
    universal_newlines=True, shell=True)        

while True:
    output = process.stdout.readline()
    if ((output == '') | (output == b''))\
            & (process.poll() is not None):
        break        
    if (output) and not (output.startswith('Completed')):
        print('AWSCLI:'+output.strip())  

rc = process.poll()
success= rc==0

if not success:
    print('AWS S3 Sync metadata \"%s\"' % (''.join(process.stderr.readlines())))