# Instalation Instructions

## Prerequisite installations:
<ul>
<li><a href="https://code.visualstudio.com/download">vscode</a></li>
<li><a href="https://git-scm.com/downloads">git</a></li>
<li><a href="https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html">awscli</a></li>
<li><a href="https://www.python.org/downloads/release/python-394/">python 3.9.4</a></li>
<li><a href="https://github.com/jcarlitooliveira/SharedData">clone repository</a></li>

### create virtual environment
<li>enter command: pip -m venv venv</li>
<li>activate virtual environnment (venv/Scripts/activate.bat)</li>

### install requirements
<li>enter command: pip install - r requirements.txt</li>
<li>install bpapi > python -m pip install --index-url=https://bcms.bloomberg.com/pip/simple </li>
</ul>

## create .env file in root folder Ie:
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


### Read only permission enter command bellow
<ul>
<li>aws configure --profile s3readonly</li>
<li>enter variables bellow:</li>
<li>[USERKEY]</li>
<li>[USERSECRET]</li>
<li>sa-east-1</li>
<li>json</li>
</ul>

### Read-Write permission enter command bellow:
<ul>
<li>aws configure --profile s3readwrite</li>
<li>enter variables bellow:</li>
<li>[USERKEY]</li>
<li>[USERSECRET]</li>
<li>sa-east-1</li>
<li>json</li>
</ul>
