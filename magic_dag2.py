# -*- coding: utf-8 -*-
# create or edit with:
# vi /home/airflow/airflow/dags/magic_dag.py
# %d - delete all text in vim -> strg v this code to test it via airflow
# Total amount of work: 22 Hours

# needs to be run twice to work!

# Du musst das erstellten der zweiten Table für foreign_cards und die add_partition hinzufügen, bzw. fixen.
# define Google Cloud IP to allow ssh-connection
GCloudIp = "35.246.117.16"

# CSV exportieren hive -e 'select * from your_Table' | sed 's/[\t]/,/g'  > /home/yourfile.csv doesn't work
# INSERT OVERWRITE LOCAL DIRECTORY '/home/lvermeer/temp' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select * from table; doesnt create a file
# Für Sqoop benötigen wir ein Passwort
# hive -e 'select * from your_Table' | sed 's/[\t]/,/g'  > /home/yourfile.csv
# wenn Dateipfad auf Airflow angegben wurde, dann erstellt er nichts
# wenn Dateipfad auf Hive angegeben wurde, dann sagt er, das er die Datei namens Hive nicht findet.

import requests
import json
from datetime import datetime
import subprocess
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

dag = DAG(
    "MagicDAG",
    default_args=args,
    description="The DAG to get all Magic Cards",
    schedule_interval="56 18 * * *",
    start_date=datetime(2019, 10, 16),
    catchup=False,
    max_active_runs=1,
)

#install dependencies
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

hiveSQL_add_Jar_dependency='''
ADD JAR /home/hadoop/hive/lib/hive-hcatalog-core-3.1.2.jar;
'''

add_JAR_dependencies = HiveOperator(
    task_id='add_jar_dependencies',
    hql=hiveSQL_add_Jar_dependency,
    hive_cli_conn_id='beeline',
    dag=dag)

# call python function with PythonOperator
installPipDependencies = PythonOperator(
    task_id="installPipDependencies", python_callable=installDependencies, dag=dag
)

def create_mysql_magic_enduser_database():
    execute_mysql_ssh_query(query="CREATE DATABASE IF NOT EXISTS MagicTheGathering;", database_name="")

def mySQL_drop_user_magic_cards_table():
    query = '''DROP TABLE IF EXISTS user_magic_cards;'''
    execute_mysql_ssh_query(query, "MagicTheGathering")

def create_mysql_user_magic_cards_table():
    query = '''CREATE TABLE IF NOT EXISTS user_magic_cards (
        name VARCHAR(60),
        multiverseid VARCHAR(10),
        imageUrl VARCHAR(150)
    );'''
    execute_mysql_ssh_query(query, database_name="MagicTheGathering")

 
def execute_mysql_ssh_query(query, database_name, data=None):
    import pymysql
    import paramiko
    import pandas as pd
    from paramiko import SSHClient
    from sshtunnel import SSHTunnelForwarder

    import mysql.connector
    # create with vim a ssh file for the private key
    mypkey = paramiko.RSAKey.from_private_key_file('/home/airflow/airflow/dags/keyfile.txt')
    sql_hostname = '172.17.0.2'
    sql_username = 'root'
    sql_password = 'MagicPassword'
    sql_main_database = database_name
    sql_port = 3306
    ssh_host = GCloudIp
    ssh_user = 'kaczynskilucas'
    ssh_port = 22
 
    with SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=mypkey,
        remote_bind_address=(sql_hostname, sql_port)) as tunnel:
        conn = pymysql.connect(host='127.0.0.1', user=sql_username,
                passwd=sql_password, db=sql_main_database,
                port=tunnel.local_bind_port)
        cursor = conn.cursor()
        if data!=None:
            cursor.executemany(query, data)
            conn.commit()
        else:
            cursor.execute(query)
        cursor.close()
        conn.close()

def get_hive_table_data(query):
    import pymysql
    import paramiko
    import pandas as pd
    from paramiko import SSHClient
    from sshtunnel import SSHTunnelForwarder
    from pyhive import hive
    import mysql.connector

    mypkey = paramiko.RSAKey.from_private_key_file('/home/airflow/airflow/dags/keyfile.txt')
    # if you want to use ssh password use - ssh_password='your ssh password', bellow
    ssh_host = GCloudIp
    ssh_user = 'kaczynskilucas'
    ssh_port = 22
 
    hive_host = "172.17.0.1"
    hive_port = 10000
    hive_user = "hadoop"
    
    with SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=mypkey,
        remote_bind_address=(hive_host, hive_port)) as tunnel:
            hive_conn = hive.Connection(
            host="127.0.0.1", 
            port=tunnel.local_bind_port,
            username=hive_user
            )
            cursor = hive_conn.cursor()
            query = "SELECT * FROM default.magic_cards_reduced"
            cursor.execute(query)
            return cursor.fetchall()

def load_data_from_hive_to_mysql():
    hive_fetch_query = "SELECT * FROM default.magic_cards_reduced"
    hive_data = get_hive_table_data(hive_fetch_query)
    
    mysql_insert_query = '''INSERT INTO user_magic_cards(name, multiverseid, imageurl) VALUES (%s, %s, %s)'''
    execute_mysql_ssh_query(query=mysql_insert_query, database_name="MagicTheGathering", data=hive_data)

hiveSQL_create_table_all_cards='''
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

hiveSQL_add_partition_all_magic_cards='''
ALTER TABLE magic_cards
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/mtg/raw/magic_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}';
'''

hiveSQL_add_partition_foreign_magic_cards='''
ALTER TABLE foreign_magic_cards
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/mtg/raw/foreign_magic_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}';
'''

create_local_import_dir = CreateDirectoryOperator(
    task_id="create_import_dir",
    path="/home/airflow/",
    directory="mtg",
    dag=dag,
)

hiveSQL_drop_cards_table='''
DROP TABLE IF EXISTS magic_cards;
'''

hiveSQL_drop_foreign_cards_table='''
DROP TABLE IF EXISTS foreign_magic_cards;
'''

hiveSQL_drop_cards_reduced_table='''
DROP TABLE IF EXISTS magic_cards_reduced;
'''

hiveSQL_insert_foreign_cards_into_cards_reduced_table='''
ADD JAR /home/hadoop/hive/lib/hive-hcatalog-core-3.1.2.jar;
INSERT OVERWRITE TABLE magic_cards_reduced
SELECT
    m.name,
    m.multiverseid,
    a.imageUrl
FROM
    magic_cards m
    JOIN foreign_magic_cards a ON (m.id = a.cardid)
WHERE
    a.language = "German";
'''

drop_HiveTable_magic_cards = HiveOperator(
    task_id='drop_HiveTable_magic_cards',
    hql=hiveSQL_drop_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

drop_HiveTable_foreign_magic_cards = HiveOperator(
    task_id='drop_HiveTable_foreign_magic_cards',
    hql=hiveSQL_drop_foreign_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

dop_HiveTable_magic_cards_reduced = HiveOperator(
    task_id='drop_HiveTable_magic_cards_reduced',
    hql=hiveSQL_drop_cards_reduced_table,
    hive_cli_conn_id='beeline',
    dag=dag)

clear_local_import_dir = ClearDirectoryOperator(
    task_id="clear_import_dir",
    directory="/home/airflow/mtg/",
    pattern="*",
    dag=dag,
)
# Übergebe das aktuelle Datum Airflow via {{ ds }}
def getAllMTGCards(ds, **kwargs):
    response = requests.get("https://api.magicthegathering.io/v1/cards?pageSize=100&page=1")
    totalCount = response.headers["Total-Count"]
    totalCount = int((int(totalCount) / 100))
    cards = response.json()["cards"]
    foreignCards = getForeignCards(cards)

    for i in range(2, 3):
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

def toJSON(cards):
    for i in range(len(cards)):
        cards[i] = json.dumps(cards[i])
    cardsJson = ",\n".join(cards)
    return cardsJson

def getForeignCards(cards): 
    foreignCards = []
    for card in cards:
        if "foreignNames" in card:
            for foreignCard in card["foreignNames"]:
                foreignCard["cardid"] = card["id"]
                foreignCards.append(foreignCard)
    return foreignCards



# call python function with PythonOperator
download_all_magic_cards = PythonOperator(
    task_id="download_all_magic_cards", provide_context=True, python_callable=getAllMTGCards, xcom_push=True,
    dag=dag
)

create_hdfs_all_cards_partition_dir = HdfsMkdirFileOperator(
    task_id="mkdir_hdfs_raw_dir_magic_cards",
    directory='/user/hadoop/mtg/raw/magic_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id="hdfs",
    dag=dag,
)

create_hdfs_foreign_cards_partition_dir = HdfsMkdirFileOperator(
    task_id="mkdir_hdfs_raw_dir_foreign_magic_cards",
    directory='/user/hadoop/mtg/raw/foreign_magic_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id="hdfs",
    dag=dag,
)

hdfs_put_all_magic_cards = HdfsPutFileOperator(
    task_id="hdfs_put_all_magic_cards",
    local_file="/home/airflow/mtg/mtgcards_{{ ds }}.json",
    remote_file='/user/hadoop/mtg/raw/magic_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/mtgcards_{{ ds }}.json',
    hdfs_conn_id="hdfs",
    dag=dag,
)

hdfs_put_foreign_magic_cards = HdfsPutFileOperator(
    task_id="hdfs_put_foreign_magic_cards",
    local_file="/home/airflow/mtg/foreign_mtgcards_{{ ds }}.json",
    remote_file='/user/hadoop/mtg/raw/foreign_magic_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/foreign_mtgcards_{{ ds }}.json',
    hdfs_conn_id="hdfs",
    dag=dag,
)

create_HiveTable_all_magic_cards = HiveOperator(
    task_id='create_all_magic_cards_table',
    hql=hiveSQL_create_table_all_cards,
    hive_cli_conn_id='beeline',
    dag=dag,
)

create_HiveTable_foreign_magic_cards = HiveOperator(
    task_id='create_foreign_magic_cards_table',
    hql=hiveSQL_create_table_foreign_cards,
    hive_cli_conn_id='beeline',
    dag=dag,
) 

addPartition_HiveTable_all_cards = HiveOperator(
    task_id='addPartition_HiveTable_all_cards',
    hql=hiveSQL_add_partition_all_magic_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

addPartition_HiveTable_foreign_cards = HiveOperator(
    task_id='addPartition_HiveTable_foreign_cards',
    hql=hiveSQL_add_partition_foreign_magic_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

create_HiveTable_magic_cards_reduced = HiveOperator(
    task_id='create_magic_cards_reduced',
    hql=hiveSQL_create_magic_cards_reduced,
    hive_cli_conn_id='beeline',
    dag=dag)

hive_merge_foreign_and_magic_cards_in_reduced_table = HiveOperator(
    task_id='hive_merge_foreign_and_magic_cards_in_reduced_table',
    hql=hiveSQL_insert_foreign_cards_into_cards_reduced_table,
    hive_cli_conn_id='beeline',
    dag=dag)


dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)

delete_MySQLTable_user_magic_cards = PythonOperator(
    task_id='delete_mysql_user_magic_cards_table',
    python_callable = mySQL_drop_user_magic_cards_table,
    op_kwargs = {},
    dag=dag
)

create_mysql_magic_enduser_database = PythonOperator(
    task_id='create_mysql_magic_enduser_database',
    python_callable = create_mysql_magic_enduser_database,
    op_kwargs = {},
    dag=dag
)

mySQL_create_user_magic_cards_table = PythonOperator(
    task_id='mySQL_create_user_magic_cards_table',
    python_callable = create_mysql_user_magic_cards_table,
    op_kwargs = {},
    dag=dag
)

load_data_hive_to_mysql_mtg_cards = PythonOperator(
    task_id='load_data_hive_to_mysql_mtg_cards',
    python_callable = load_data_from_hive_to_mysql,
    op_kwargs = {},
    dag=dag
)

installPipDependencies >> create_mysql_magic_enduser_database  >> delete_MySQLTable_user_magic_cards >> mySQL_create_user_magic_cards_table >> create_local_import_dir >> clear_local_import_dir >> add_JAR_dependencies >> download_all_magic_cards

download_all_magic_cards  >> create_hdfs_all_cards_partition_dir >> hdfs_put_all_magic_cards >> drop_HiveTable_magic_cards >> create_HiveTable_all_magic_cards >> addPartition_HiveTable_all_cards >> dummy_op

download_all_magic_cards >> create_hdfs_foreign_cards_partition_dir >> hdfs_put_foreign_magic_cards >> drop_HiveTable_foreign_magic_cards >> create_HiveTable_foreign_magic_cards >> addPartition_HiveTable_foreign_cards >> dummy_op

dummy_op >> dop_HiveTable_magic_cards_reduced >> create_HiveTable_magic_cards_reduced >> hive_merge_foreign_and_magic_cards_in_reduced_table >> load_data_hive_to_mysql_mtg_cards