# -*- coding: utf-8 -*-
# create or edit with:
# vi /home/airflow/airflow/dags/magic_dag.py
# %d - delete all text in vim -> strg v this code to test it via airflow
# Total amount of work: 11 Hours

# Du musst das erstellten der zweiten Table für foreign_cards und die add_partition hinzufügen, bzw. fixen.

import requests
import json
from datetime import datetime
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
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' LOCATION 'hdfs:///user/hadoop/mtg/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}';
'''

hiveSQL_add_partition_all_magic_cards='''
ALTER TABLE magic_cards
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/mtg/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}';
'''

create_local_import_dir = CreateDirectoryOperator(
    task_id="create_import_dir",
    path="/home/airflow/",
    directory="mtg",
    dag=dag,
)

hiveSQL_select_all_cards ='''
SELECT * from magic_cards LIMIT 100;
'''

hiveSQL_drop_cards_table='''
DROP TABLE IF EXISTS magic_cards;
'''

select_all_cards = HiveOperator(
    task_id="select_all_cards",
    hql=hiveSQL_select_all_cards,
    hive_cli_conn_id="beeline",
    dag=dag,
)

drop_HiveTable_magic_cards = HiveOperator(
    task_id='drop_HiveTable_magic_cards',
    hql=hiveSQL_drop_cards_table,
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
    task_id="mkdir_hdfs_raw_dir",
    directory='/user/hadoop/mtg/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id="hdfs",
    dag=dag,
)

hdfs_put_all_magic_cards = HdfsPutFileOperator(
    task_id="hdfs_put_all_magic_cards",
    local_file="/home/airflow/mtg/mtgcards_{{ ds }}.json",
    remote_file='/user/hadoop/mtg/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/mtgcards_{{ ds }}.json',
    hdfs_conn_id="hdfs",
    dag=dag,
)

create_HiveTable_all_magic_cards = HiveOperator(
    task_id='create_all_magic_cards_table',
    hql=hiveSQL_create_table_all_cards,
    hive_cli_conn_id='beeline',
    dag=dag,
) 

addPartition_HiveTable_all_cards = HiveOperator(
    task_id='addPartition_HiveTable_all_cards',
    hql=hiveSQL_add_partition_all_magic_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)


create_local_import_dir >> clear_local_import_dir 
clear_local_import_dir >> download_all_magic_cards >> add_JAR_dependencies >> create_hdfs_all_cards_partition_dir >> hdfs_put_all_magic_cards >> drop_HiveTable_magic_cards >> create_HiveTable_all_magic_cards >> addPartition_HiveTable_all_cards >> dummy_op
dummy_op
