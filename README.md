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
sa-east-1# SharedData
Shared Memory Database with S3 repository

# Instalation Instructions

## Prerequisite installations:
<ul>
<li>vscode</li>
<li>git</li>
<li>awscli</li>
<li>python 3.9.4 (add to path)</li>
<li>clone repository</li>
<li>create virtual environnment (pip -m venv venv)</li>
<li>activate virtual environnment (venv/Scripts/activate.bat)</li>
<li>pip install - r requirements.txt</li>
<li>install bpapi > python -m pip install --index-url=https://bcms.bloomberg.com/pip/simple </li>
<\ul>

## create .env file Ie:
<ul>
<li>SOURCE_FOLDER=C:\src\SharedData\src</li>
<li>PYTHONPATH=${SOURCE_FOLDER}</li>
<li>DATABASE_FOLDER=C:\DB\files</li>
<li>DOWNLOADS_FOLDER=D:\DOWNLOADS</li>
<li>LOG_LEVEL=DEBUG</li>
<li>AWSCLI_PATH=C:\Program Files\Amazon\AWSCLIV2\aws.exe</li>
<li>S3_BUCKET=[S3_BUCKET]</li>
</ul>

## configure aws cli
<ul>
<li> ### Read only permission enter command bellow:</li>
<li>aws configure --profile s3readonly</li>
<li>enter variables bellow:</li>
<li>[USERKEY]</li>
<li>[USERSECRET]</li>
<li>sa-east-1</li>
<li>json</li>
</ul>

<ul>
<li> ### Read-Write permission enter command bellow:</li>
<li>aws configure --profile s3readwrite</li>
<li>enter variables bellow:</li>
<li>[USERKEY]</li>
<li>[USERSECRET]</li>
<li>sa-east-1</li>
<li>json</li>
</ul>
