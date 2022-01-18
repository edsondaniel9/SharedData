import os,sys
import logging

import matplotlib.pyplot as plt
plt.style.use('default')
plt.rcParams["figure.figsize"] = (10,5)
import pandas as pd
pd.set_option('display.float_format', lambda x: '%.4f' % x)

source_directory = r'C:\src\SharedData\src'
sys.path.insert(0,source_directory)

venv_directory = r'C:\src\SharedData\venv'
python_path = venv_directory+r'\Scripts\python.exe'

db_directory = r'C:\DB\files'
downloads_directory = r'D:\DOWNLOADS'

s3_sync = False
s3_bucket = 's3://tradebywire/files'
awsclipath =  r'C:\Program Files\Amazon\AWSCLIV2\aws.exe'

verbose_level = logging.INFO
logging_level = logging.INFO
username = os.environ['USERNAME']
userdomain = os.environ['USERDOMAIN']
