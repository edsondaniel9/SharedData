# SharedData
Shared Memory Database with S3 repository

# Instalation Instructions

## Prerequisite installations:
-vscode
-git
-awscli
-python 3.9.4 (add to path)
-clone repository
-create virtual environnment (pip -m venv venv)
-activate virtual environnment (venv/Scripts/activate.bat)
-pip install - r requirements.txt
-install bpapi > python -m pip install --index-url=https://bcms.bloomberg.com/pip/simple 


## create .env file Ie:
SOURCE_FOLDER=C:\src\SharedData\src
PYTHONPATH=${SOURCE_FOLDER}
DATABASE_FOLDER=C:\DB\files
DOWNLOADS_FOLDER=D:\DOWNLOADS
LOG_LEVEL=DEBUG
AWSCLI_PATH=C:\Program Files\Amazon\AWSCLIV2\aws.exe
S3_BUCKET=s3://tradebywire/files