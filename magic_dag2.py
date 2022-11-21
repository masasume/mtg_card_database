# -*- coding: utf-8 -*-
# create or edit with:
# vi /home/airflow/airflow/dags/magic_dag.py
# %d - delete all text in vim -> strg v this code to test it via airflow
# Total amount of work: 15 Hours

# Du musst das erstellten der zweiten Table für foreign_cards und die add_partition hinzufügen, bzw. fixen.

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

#install mysql-connector
def install():
    subprocess.call([sys.executable, "-m", "pip", "install", 'mysql-connector-python'])

# call python function with PythonOperator
installMySQLConnector = PythonOperator(
    task_id="installMySQLConnector", python_callable=install, dag=dag
)

def createMySQLUserTable():
    import mysql.connector
    mydb = mysql.connector.connect(
    host="0.0.0.0",
    user="root",
    password="MagicPassword"
    )
    cursor = mydb.cursor()
    sql_create_db = "Create database userMagicCards"
    cursor.execute(sql_create_db)
    mydb.close()

# call python function with PythonOperator
CreateMySQLUserTable = PythonOperator(
    task_id="CreateMySQLUserTable", python_callable=createMySQLUserTable, dag=dag
)

hiveSQL_add_Jar_dependency='''
ADD JAR /home/hadoop/hive/lib/hive-hcatalog-core-3.1.2.jar;
'''

add_JAR_dependencies = HiveOperator(
    task_id='add_jar_dependencies',
    hql=hiveSQL_add_Jar_dependency,
    hive_cli_conn_id='beeline',
    dag=dag)

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
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hadoop/mtg/final/magic_cards';
'''

hiveSQL_create_foreign_magic_cards_reduced='''
CREATE EXTERNAL TABLE IF NOT EXISTS foreign_magic_cards_reduced (
    name STRING,
    multiverseid STRING,
    imageUrl STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hadoop/mtg/final/foreign_magic_cards';
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

clear_local_import_dir = ClearDirectoryOperator(
    task_id="clear_import_dir",
    directory="/home/airflow/mtg/",
    pattern="*",
    dag=dag,
)

def getAllMTGCards():
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

    yearMonthDay = datetime.now().strftime('%Y-%m-%d')
    cardsJson = toJSON(cards)
    text_file = open("/home/airflow/mtg/mtgcards_"+yearMonthDay+".json", "w")
    text_file.write(cardsJson)

    foreignCardsJson = toJSON(foreignCards)
    text_file = open("/home/airflow/mtg/foreign_mtgcards_"+yearMonthDay+".json", "w")
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
    task_id="download_all_magic_cards", python_callable=getAllMTGCards, dag=dag
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


create_local_import_dir >> clear_local_import_dir >> add_JAR_dependencies >> installMySQLConnector >> CreateMySQLUserTable >> download_all_magic_cards

download_all_magic_cards  >> create_hdfs_all_cards_partition_dir >> hdfs_put_all_magic_cards >> drop_HiveTable_magic_cards >> create_HiveTable_all_magic_cards >> addPartition_HiveTable_all_cards >> dummy_op

download_all_magic_cards >> create_hdfs_foreign_cards_partition_dir >> hdfs_put_foreign_magic_cards >> drop_HiveTable_foreign_magic_cards >> create_HiveTable_foreign_magic_cards >> addPartition_HiveTable_foreign_cards >> dummy_op

dummy_op >> create_HiveTable_magic_cards_reduced >> hive_merge_foreign_and_magic_cards_in_reduced_table


