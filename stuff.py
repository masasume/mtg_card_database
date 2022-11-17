# -*- coding: utf-8 -*-

from datetime import datetime
import requests
import json
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

hiveSQL_create_cards_table='''
CREATE EXTERNAL TABLE IF NOT EXISTS cards(
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
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE LOCATION 'hdfs:///user/hadoop/mtg/raw/cards';
'''

hiveSQL_create_foreign_cards_table='''
CREATE EXTERNAL TABLE IF NOT EXISTS foreign_cards(
	name STRING,
    text STRING,
    type STRING,
    flavor STRING,
    imageUrl STRING,
    language STRING,
    multiverseid STRING,
    cardid STRING
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE LOCATION 'hdfs:///user/hadoop/mtg/raw/foreignCards';
'''

hiveSQL_add_Jar_dependency='''
ADD JAR /home/hadoop/hive/lib/hive-hcatalog-core-3.1.2.jar;
'''

hiveSQL_drop_cards_table='''
DROP TABLE cards;
'''

hiveSQL_drop_foreign_cards_table='''
DROP TABLE foreign_cards;
'''

hiveSQL_load_json_cards_table='''
LOAD DATA LOCAL INPATH '/user/hadoop/mtg/raw/cards/mtg_cards.json' OVERWRITE INTO TABLE cards;
'''

hiveSQL_load_json_foreign_cards_table='''
LOAD DATA LOCAL INPATH '/user/hadoop/mtg/raw/foreignCards/mtg_foreign_cards.json' OVERWRITE INTO TABLE foreign_cards;
'''

hiveSQL_create_cards_reduced='''
CREATE EXTERNAL TABLE IF NOT EXISTS cards_reduced (
    name STRING,
    multiverseid STRING,
    imageUrl STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hadoop/mtg/final/cards';
'''

hiveSQL_insertoverwrite_cards_reduced='''
ADD JAR /home/hadoop/hive/lib/hive-hcatalog-core-3.1.2.jar;
INSERT OVERWRITE TABLE cards_reduced
SELECT
    c.name,
    c.multiverseid,
    f.imageUrl
FROM
    cards c
    JOIN foreign_cards f ON (c.id = f.cardid)
WHERE
    f.language = "German";
'''

def getMTGCards():
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
    text_file = open("/home/airflow/mtg/mtg_cards.json", "w")
    text_file.write(cardsJson)

    foreignCardsJson = toJSON(foreignCards)
    text_file = open("/home/airflow/mtg/mtg_foreign_cards.json", "w")
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

dag = DAG('MTG-API', default_args=args, description='MTG-API Import',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_directory',
    path='/home/airflow',
    directory='mtg',
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_directory',
    directory='/home/airflow/mtg',
    pattern='*',
    dag=dag,
)

download_mtg_cards = PythonOperator(
    task_id='download_mtg_cards',
    python_callable = getMTGCards,
    op_kwargs = {},
    dag=dag
)

create_cards_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_cards_dir',
    directory='/user/hadoop/mtg/raw/cards',
    hdfs_conn_id='hdfs',
    dag=dag,
)

create_foreign_cards_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_foreign_cards_dir',
    directory='/user/hadoop/mtg/raw/foreignCards',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_cards = HdfsPutFileOperator(
    task_id='upload_cards_to_hdfs',
    local_file='/home/airflow/mtg/mtg_cards.json',
    remote_file='/user/hadoop/mtg/raw/cards/mtg_cards.json',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_foreign_cards = HdfsPutFileOperator(
    task_id='upload_foreign_cards_to_hdfs',
    local_file='/home/airflow/mtg/mtg_foreign_cards.json',
    remote_file='/user/hadoop/mtg/raw/foreignCards/mtg_foreign_cards.json',
    hdfs_conn_id='hdfs',
    dag=dag,
)

delete_HiveTable_cards = HiveOperator(
    task_id='delete_cards_table',
    hql=hiveSQL_drop_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

delete_HiveTable_foreign_cards = HiveOperator(
    task_id='delete_foreign_cards_table',
    hql=hiveSQL_drop_foreign_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

add_JAR_dependencies = HiveOperator(
    task_id='add_jar_dependencies',
    hql=hiveSQL_add_Jar_dependency,
    hive_cli_conn_id='beeline',
    dag=dag)

# load_HiveTable_cards = HiveOperator(
#     task_id='load_cards_table',
#     hql=hiveSQL_load_json_cards_table,
#     hive_cli_conn_id='beeline',
#     dag=dag)

# load_HiveTable_foreign_cards = HiveOperator(
#     task_id='load_foreign_cards_table',
#     hql=hiveSQL_load_json_foreign_cards_table,
#     hive_cli_conn_id='beeline',
#     dag=dag)

create_HiveTable_cards = HiveOperator(
    task_id='create_cards_table',
    hql=hiveSQL_create_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

create_HiveTable_foreign_cards = HiveOperator(
    task_id='create_foreign_cards_table',
    hql=hiveSQL_create_foreign_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)

create_HiveTable_cards_reduced = HiveOperator(
    task_id='create_cards_reduced',
    hql=hiveSQL_create_cards_reduced,
    hive_cli_conn_id='beeline',
    dag=dag)

hive_insert_overwrite_cards_reduced = HiveOperator(
    task_id='hive_write_cards_reduced_table',
    hql=hiveSQL_insertoverwrite_cards_reduced,
    hive_cli_conn_id='beeline',
    dag=dag)

create_local_import_dir >> clear_local_import_dir >> download_mtg_cards
download_mtg_cards >> create_cards_dir >> hdfs_put_cards >> delete_HiveTable_cards >> create_HiveTable_cards >> dummy_op
download_mtg_cards >> create_foreign_cards_dir >> hdfs_put_foreign_cards >> delete_HiveTable_foreign_cards >> create_HiveTable_foreign_cards >> dummy_op
dummy_op >> create_HiveTable_cards_reduced >> add_JAR_dependencies >> hive_insert_overwrite_cards_reduced