# SharedData
Shared Memory Database with S3 repository

# Instalation Instructions

## Prerequisite installations:
<ul>
<li>-vscode</li>
<li>-git</li>
<li>-awscli</li>
<li>-python 3.9.4 (add to path)</li>
<li>-clone repository</li>
<li>-create virtual environnment (pip -m venv venv)</li>
<li>-activate virtual environnment (venv/Scripts/activate.bat)</li>
<li>-pip install - r requirements.txt</li>
<li>-install bpapi > python -m pip install --index-url=https://bcms.bloomberg.com/pip/simple </li>


## create .env file Ie:
SOURCE_FOLDER=C:\src\SharedData\src
PYTHONPATH=${SOURCE_FOLDER}
DATABASE_FOLDER=C:\DB\files
DOWNLOADS_FOLDER=D:\DOWNLOADS
LOG_LEVEL=DEBUG
AWSCLI_PATH=C:\Program Files\Amazon\AWSCLIV2\aws.exe
S3_BUCKET=s3://tradebywire/files

## configure aws cli
-aws configure --profile s3readonly
[USERKEY]
[USERSECRET]
sa-east-1
json

-aws configure --profile s3readwrite
[USERKEY]
[USERSECRET]
sa-east-1
json