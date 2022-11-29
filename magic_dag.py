# -*- coding: utf-8 -*-
# create or edit with:
# vi /home/airflow/airflow/dags/magic_dag.py
# %d - delete all text in vim -> strg v this code to test it via airflow
# Total amount of work: 33 Hours

# Für die Abgabe: Auf GitHub pushen, Docker erstellen, das es lauffähig ist.

# define Google Cloud IP to allow ssh-connection
GCloudIp = "35.189.68.7"

# is needed to use GET requests
import requests
# we use this to read and handle the received json files from the API
import json
# used to get the current date and time
from datetime import datetime
# used to install pip dependencies over the console. Subprocess opens a new console and executes the command
import subprocess
# used send a command to the console and run it
import sys

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow'
}

# name of the dag in airflow and it's attributes
dag = DAG(
    "MagicDAG",
    default_args=args,
    description="The DAG to get all Magic Cards",
    schedule_interval="56 18 * * *",
    start_date=datetime(2019, 10, 16),
    catchup=False,
    max_active_runs=1,
)

# We need this import to use ROW FORMAT SERDE to serialize the json file.
hiveSQL_add_Jar_dependency='''
ADD JAR /home/hadoop/hive/lib/hive-hcatalog-core-3.1.2.jar;
'''

#install pip dependencies. This is included in the dag for ease of use on new machines. But it should be excluded after the first run, because it takes approx. 20 seconds to check all allready installed pip files.
def installDependencies():
    subprocess.call([sys.executable, "-m", "pip", "install", 'mysql-connector-python'])
    subprocess.call([sys.executable, "-m", "pip", "install", 'paramiko'])
    subprocess.call([sys.executable, "-m", "pip", "install", 'pandas'])
    subprocess.call([sys.executable, "-m", "pip", "install", 'python3-pymysql'])
    subprocess.call([sys.executable, "-m", "pip", "install", 'sshtunnel'])
    subprocess.call([sys.executable, "-m", "pip", "install", 'sasl'])
    subprocess.call([sys.executable, "-m", "pip", "install", 'thrift'])
    subprocess.call([sys.executable, "-m", "pip", "install", 'pyhive'])
    subprocess.call([sys.executable, "-m", "pip", "install", 'thrift-sasl'])

# creates the enduser database in the mysql server hosted on the mysql (MagicMySQL) docker container
def create_mysql_magic_enduser_database():
    executeMySQLQueryViaSSH(query="CREATE DATABASE IF NOT EXISTS MagicTheGathering;", databaseName="")

# We create the user_magic_cards table in the mysql database.
def create_mysql_user_magic_cards_table():
    query = '''CREATE TABLE IF NOT EXISTS user_magic_cards (
        name VARCHAR(60),
        multiverseid VARCHAR(10),
        imageUrl VARCHAR(150)
    );'''
    executeMySQLQueryViaSSH(query, databaseName="MagicTheGathering")

# We use ssh to connect to the mysql docker container and execute the query. dataStream is empty by default. If we want to send data to the mysql server we have to pass data via the dataStream parameter.
def executeMySQLQueryViaSSH(query, databaseName, dataStream=None):
    import pymysql
    import paramiko
    import pandas as pd
    from paramiko import SSHClient
    from sshtunnel import SSHTunnelForwarder

    import mysql.connector

    sqlPort = 3306
    sshHost = GCloudIp
    sshUser = 'kaczynskilucas'
    sshPort = 22

    sshHostname = '172.17.0.2'
    sqlUsername = 'root'
    swlPassword = 'MagicPassword'
    sqlMainDatabase = databaseName

    # path to the private_key to connect to the mysql docker container
    mypkey = paramiko.RSAKey.from_private_key_file('/home/airflow/airflow/dags/keyfile.txt')
    
    # We use the SSHTunnelForwarder to connect to the mysql docker container via ssh
    with SSHTunnelForwarder(
        (sshHost, sshPort),
        ssh_username=sshUser,
        ssh_pkey=mypkey,
        remote_bind_address=(sshHostname, sqlPort)) as tunnel:
        conn = pymysql.connect(host='127.0.0.1', user=sqlUsername,
                passwd=swlPassword, db=sqlMainDatabase,
                port=tunnel.local_bind_port)
        cursor = conn.cursor()
        if dataStream!=None:
            cursor.executemany(query, dataStream)
            conn.commit()
        else:
            cursor.execute(query)
        cursor.close()
        conn.close()

# this get's the data of the table magic_cards_reduced from the hive database and writes it to the mysql MagicTheGathering database.
def getHiveTableDataViaSSH(query):
    # we import the
    import pymysql
    import paramiko
    import pandas as pd
    from paramiko import SSHClient
    from sshtunnel import SSHTunnelForwarder
    from pyhive import hive
    import mysql.connector

    sshHost = GCloudIp
    sshUser = 'kaczynskilucas'
    sshPort = 22
 
    hive_host = "172.17.0.1"
    hive_port = 10000
    hive_user = "hadoop"

    mypkey = paramiko.RSAKey.from_private_key_file('/home/airflow/airflow/dags/keyfile.txt')
    
    # We use the SSHTunnelForwarder to connect to the mysql docker container via ssh
    with SSHTunnelForwarder(
        (sshHost, sshPort),
        ssh_username=sshUser,
        ssh_pkey=mypkey,
        remote_bind_address=(hive_host, hive_port)) as tunnel:
            hiveConnection = hive.Connection(
            host="127.0.0.1", 
            port=tunnel.local_bind_port,
            username=hive_user
            )
            cursor = hiveConnection.cursor()
            query = "SELECT * FROM default.magic_cards_reduced"
            cursor.execute(query)
            return cursor.fetchall()

def load_data_from_hive_to_mysql():
    hiveFetchQuery = "SELECT * FROM default.magic_cards_reduced"
    hiveData = getHiveTableDataViaSSH(hiveFetchQuery)
    
    mysqlInsertQuery = '''INSERT INTO user_magic_cards(name, multiverseid, imageurl) VALUES (%s, %s, %s)'''
    executeMySQLQueryViaSSH(query=mysqlInsertQuery, databaseName="MagicTheGathering", dataStream=hiveData)

# we get all MTG Cards from the Magic-API. We give ds (curr. DateTime from airflow) via ds.
def getAllMTGCards(ds, **kwargs):
    response = requests.get("https://api.magicthegathering.io/v1/cards?pageSize=100&page=1")
    totalCount = response.headers["Total-Count"]
    totalCount = int((int(totalCount) / 100))
    cards = response.json()["cards"]
    foreignCards = getForeignCards(cards)

    for i in range(2,totalCount):
        print(str(i) + "von" + str(totalCount))
        response = requests.get("https://api.magicthegathering.io/v1/cards?pageSize=100&page=" + str(i))
        responseCards = response.json()["cards"]
        foreignCards = foreignCards + getForeignCards(responseCards)
        cards = cards + response.json()["cards"]

    for i in range(len(cards)):
        if "foreignNames" in cards[i]:
            del cards[i]["foreignNames"]
    
    cardsJson = toJSON(cards)
    text_file = open("/home/airflow/mtg/mtgcards_"+ds+".json", "w")
    text_file.write(cardsJson)

    foreignCardsJson = toJSON(foreignCards)
    text_file = open("/home/airflow/mtg/foreign_mtgcards_"+ds+".json", "w")
    text_file.write(foreignCardsJson)
    return

def getForeignCards(cards): 
    foreignCards = []
    for card in cards:
        if "foreignNames" in card:
            for foreignCard in card["foreignNames"]:
                foreignCard["cardid"] = card["id"]
                foreignCards.append(foreignCard)
    return foreignCards

def toJSON(cards):
    for i in range(len(cards)):
        cards[i] = json.dumps(cards[i])
    cardsJson = ",\n".join(cards)
    return cardsJson

# def getAllMTGCards(ds, **kwargs):
#     webResponse = requests.get("https://api.magicthegathering.io/v1/cards?pageSize=100&page=1")
#     totalWebCount = webResponse.headers["Total-Count"]
#     totalWebCount = int((int(totalWebCount) / 100))
#     print("1" + " von " + str(totalWebCount))
#     cards = webResponse.json()["cards"]
#     foreignCards = getForeignCards(cards)

#     # we iterate over all pages and get all cards
#     for i in range(2, totalWebCount-1):
#         print(str(i) + " von " + str(totalWebCount))
#         webResponse = requests.get("https://api.magicthegathering.io/v1/cards?pageSize=100&page=" + str(i))
#         responseCards = webResponse.json()["cards"]
#         foreignCards = foreignCards + getForeignCards(responseCards)
#         cards = cards + webResponse.json()["cards"]

#     # we iterate over all foreign cards
#     for i in range(len(cards)):
#         if "foreignNames" in cards[i]:
#             del cards[i]["foreignNames"]

#     # we write all cards (except foreign cards) to a json file and save it with the dateTime as filename
#     cardsInJson = toJSON(cards)
#     textFile = open("/home/airflow/mtg/mtgcards_"+ds+".json", "w")
#     textFile.write(cardsInJson)

#     # we write all foreign cards to a json file and save it with the dateTime as filename
#     foreignCardsInJson = toJSON(foreignCards)
#     textFile = open("/home/airflow/mtg/foreign_mtgcards_"+ds+".json", "w")
#     textFile.write(foreignCardsInJson)
#     return

# # we serialize the objectdata to json string with json.dumps and enter with "\n" a new line for each card
# def toJSON(cards):
#     for i in range(len(cards)):
#         cards[i] = json.dumps(cards[i])
#     cardsInJson = ",\n".join(cards)
#     return cardsInJson

# # we iterate over all cards and get all foreign cards
# def getForeignCards(cards): 
#     foreignCards = []
#     for card in cards:
#         if "foreignNames" in card:
#             for foreignCard in card["foreignNames"]:
#                 foreignCard["cardid"] = card["id"]
#                 foreignCards.append(foreignCard)
#     return foreignCards

# this hive sql query creates the table magic_cards and fills it with the data from the json file from hdfs:///user/hadoop/mtg/raw/magic_cards'.
# we read the json with JsonSerDe
hiveSQL_create_magic_cards='''
CREATE EXTERNAL TABLE IF NOT EXISTS magic_cards(
    id STRING,
	name STRING,
    manaCost STRING,
	cmc FLOAT,
    colors ARRAY<STRING>,
	colorIdentity ARRAY<STRING>,
    type STRING,
    types ARRAY<STRING>,
    subtypes ARRAY<STRING>,
    rarity STRING,
    setName STRING,
	text STRING,
	flavor STRING,
	artist STRING,
    power STRING,
	toughness STRING,
	layout STRING,
    multiverseid STRING,
    imageUrl STRING,
    variations ARRAY<STRING>,
	printings ARRAY<STRING>,
	originalText STRING,
	originalType STRING,
	legalities ARRAY<STRUCT<format:STRING, legality:STRING>>,
	names ARRAY<STRING>
    ) PARTITIONED BY(partition_year INT, partition_month INT, partition_day INT) 
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' LOCATION 'hdfs:///user/hadoop/mtg/raw/magic_cards';
'''

# this hive sql query creates the table foreign_magic_cards and fills it with the data from the json file from hdfs:///user/hadoop/mtg/raw/foreign_magic_cards'.
# we read the json with JsonSerDe
hiveSQL_create_table_foreign_cards='''
CREATE EXTERNAL TABLE IF NOT EXISTS foreign_magic_cards(
	name STRING,
    text STRING,
    type STRING,
    flavor STRING,
    imageUrl STRING,
    language STRING,
    multiverseid STRING,
    cardid STRING
) PARTITIONED BY(partition_year INT, partition_month INT, partition_day INT)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE LOCATION 'hdfs:///user/hadoop/mtg/raw/foreign_magic_cards';
'''

# this hive sql query creates the table magic_cards_reduced and fills it with the data from the textfile stored at hdfs:///user/hadoop/mtg/final/magic_cards'.
hiveSQL_create_magic_cards_reduced='''
CREATE EXTERNAL TABLE IF NOT EXISTS magic_cards_reduced (
    name STRING,
    multiverseid STRING,
    imageUrl STRING
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = '"'
    )
STORED AS TEXTFILE LOCATION '/user/hadoop/mtg/final/magic_cards';
'''

# partitions the entries in magic_cards by year, month and day
hiveSQL_add_partition_magic_cards='''
ALTER TABLE magic_cards
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/mtg/raw/magic_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}';
'''
# partitions the entries in foreign_magic_cards by year, month and day
hiveSQL_add_partition_foreign_magic_cards='''
ALTER TABLE foreign_magic_cards
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/mtg/raw/foreign_magic_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}';
'''

# merges the data from the table magic_cards and foreign_magic_cards into the table magic_cards_reduced.
# it only imports the attributes needed for the enduser
# with SELECT DISTINCT we make sure that we only have unique entries
hiveSQL_insert_foreign_cards_into_cards_reduced_table='''
ADD JAR /home/hadoop/hive/lib/hive-hcatalog-core-3.1.2.jar;
INSERT OVERWRITE TABLE magic_cards_reduced
SELECT DISTINCT
    m.name,
    m.multiverseid,
    a.imageUrl
FROM
    magic_cards m
    JOIN foreign_magic_cards a ON (m.id = a.cardid)
WHERE
    a.language = "German";
'''

# creates a local import directory in airflow to download raw data
create_local_import_dir = CreateDirectoryOperator(
    task_id="create_import_dir",
    path="/home/airflow/",
    directory="mtg",
    dag=dag,
)

# clears the import directory to make sure all "old" data is deleted
clear_local_import_dir = ClearDirectoryOperator(
    task_id="clear_import_dir",
    directory="/home/airflow/mtg/",
    pattern="*",
    dag=dag,
)

# creates a partioned directory structure on hdfs for magic_cards to save data based on year, month and day
create_hdfs_all_cards_partition_dir = HdfsMkdirFileOperator(
    task_id="mkdir_hdfs_raw_dir_magic_cards",
    directory='/user/hadoop/mtg/raw/magic_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id="hdfs",
    dag=dag,
)

# creates a partioned directory structure on hdfs for foreign_magic_cards to save data based on year, month and day
create_hdfs_foreign_cards_partition_dir = HdfsMkdirFileOperator(
    task_id="mkdir_hdfs_raw_dir_foreign_magic_cards",
    directory='/user/hadoop/mtg/raw/foreign_magic_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id="hdfs",
    dag=dag,
)

# copy the file mtgcards_yyyy-mm-dd.json from the local import directory to hdfs
hdfs_put_all_magic_cards = HdfsPutFileOperator(
    task_id="hdfs_put_all_magic_cards",
    local_file="/home/airflow/mtg/mtgcards_{{ ds }}.json",
    remote_file='/user/hadoop/mtg/raw/magic_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/mtgcards_{{ ds }}.json',
    hdfs_conn_id="hdfs",
    dag=dag,
)

# copy the file foreign_mtgcards_yyyy-mm-dd.json from the local import directory to hdfs
hdfs_put_foreign_magic_cards = HdfsPutFileOperator(
    task_id="hdfs_put_foreign_magic_cards",
    local_file="/home/airflow/mtg/foreign_mtgcards_{{ ds }}.json",
    remote_file='/user/hadoop/mtg/raw/foreign_magic_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/foreign_mtgcards_{{ ds }}.json',
    hdfs_conn_id="hdfs",
    dag=dag,
)

# calls the hive sql to create the table magic_cards
create_HiveTable_magic_cards = HiveOperator(
    task_id='hiveSQL_create_magic_cards',
    hql=hiveSQL_create_magic_cards,
    hive_cli_conn_id='beeline',
    dag=dag,
)

# calls the hive sql to create the table foreign_magic_cards
create_HiveTable_foreign_magic_cards = HiveOperator(
    task_id='create_foreign_magic_cards_table',
    hql=hiveSQL_create_table_foreign_cards,
    hive_cli_conn_id='beeline',
    dag=dag,
) 

# calls the hive sql to partiotion the table magic_cards based on year, month and day
addPartition_HiveTable_magic_cards = HiveOperator(
    task_id='addPartition_HiveTable_magic_cards',
    hql=hiveSQL_add_partition_magic_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

# calls the hive sql to partiotion the table magic_cards based on year, month and day
addPartition_HiveTable_foreign_cards = HiveOperator(
    task_id='addPartition_HiveTable_foreign_cards',
    hql=hiveSQL_add_partition_foreign_magic_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

# calls the hive sql to create the table magic_cards_reduced
create_HiveTable_magic_cards_reduced = HiveOperator(
    task_id='create_magic_cards_reduced',
    hql=hiveSQL_create_magic_cards_reduced,
    hive_cli_conn_id='beeline',
    dag=dag)

# calls the hive sql to merge data from magic_cards and foreign_magic_cards into magic_cards_reduced
hive_merge_foreign_and_magic_cards_in_reduced_table = HiveOperator(
    task_id='hive_merge_foreign_and_magic_cards_in_reduced_table',
    hql=hiveSQL_insert_foreign_cards_into_cards_reduced_table,
    hive_cli_conn_id='beeline',
    dag=dag)

# call the hive sql to add jar dependencies to hive. This is needed to use ROW FORMAT SERDE to serialize the json data
add_JAR_dependencies = HiveOperator(
    task_id='add_jar_dependencies',
    hql=hiveSQL_add_Jar_dependency,
    hive_cli_conn_id='beeline',
    dag=dag)

# a dummy operator used to link the tasks together
dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)

# call python function that downloads all magic cards with PythonOperator
download_all_magic_cards = PythonOperator(
    task_id="download_all_magic_cards", provide_context=True, python_callable=getAllMTGCards, xcom_push=True,
    dag=dag
)

# creates the enduser database via python function and ssh
create_mysql_magic_enduser_database = PythonOperator(
    task_id='create_mysql_magic_enduser_database',
    python_callable = create_mysql_magic_enduser_database,
    op_kwargs = {},
    dag=dag
)

# creates the enduser table via python function and ssh
mySQL_create_user_magic_cards_table = PythonOperator(
    task_id='mySQL_create_user_magic_cards_table',
    python_callable = create_mysql_user_magic_cards_table,
    op_kwargs = {},
    dag=dag
)

# loads the data from hive sql to mysql via python function and ssh
load_data_hive_to_mysql_mtg_cards = PythonOperator(
    task_id='load_data_hive_to_mysql_mtg_cards',
    python_callable = load_data_from_hive_to_mysql,
    op_kwargs = {},
    dag=dag
)

# call python function with python operator. This function installs all pip dependencies
installPipDependencies = PythonOperator(
    task_id="installPipDependencies", python_callable=installDependencies, dag=dag
)

installPipDependencies >> create_local_import_dir >> clear_local_import_dir >> add_JAR_dependencies >> download_all_magic_cards

download_all_magic_cards  >> create_hdfs_all_cards_partition_dir >> hdfs_put_all_magic_cards  >> create_HiveTable_magic_cards >> addPartition_HiveTable_magic_cards >> dummy_op

download_all_magic_cards >> create_hdfs_foreign_cards_partition_dir >> hdfs_put_foreign_magic_cards  >> create_HiveTable_foreign_magic_cards >> addPartition_HiveTable_foreign_cards >> dummy_op

dummy_op  >> create_HiveTable_magic_cards_reduced >> hive_merge_foreign_and_magic_cards_in_reduced_table >> create_mysql_magic_enduser_database >> mySQL_create_user_magic_cards_table >> load_data_hive_to_mysql_mtg_cards