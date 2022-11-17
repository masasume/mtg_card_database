# create or edit with:
# vi /home/airflow/airflow/dags/magic_dag.py
# %d - delete all text in vim -> strg v this code to test it via airflow
# Total amount of work: 4 Hours

from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import (
    HdfsPutFileOperator,
    HdfsGetFileOperator,
    HdfsMkdirFileOperator,
)
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator

import requests
import json

args = {"owner": "airflow"}

dag = DAG(
    "MagicDAG",
    default_args=args,
    description="The DAG to get all Magic Cards",
    schedule_interval="56 18 * * *",
    start_date=datetime(2019, 10, 16),
    catchup=False,
    max_active_runs=1,
)

create_local_import_dir = CreateDirectoryOperator(
    task_id="create_import_dir",
    path="/home/airflow/",
    directory="mtg",
    dag=dag,
)

create_hdfs_all_cards_partition_dir = HdfsMkdirFileOperator(
    task_id="mkdir_hdfs_raw_dir",
    directory='/user/hadoop/mtg/raw{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id="hdfs",
    dag=dag,
)

hdfs_put_all_magic_cards = HdfsPutFileOperator(
    task_id="hdfs_put_all_magic_cards",
    local_file="/home/airflow/mtg/mtgcards.json",
    remote_file='/user/hadoop/mtg/raw/mtgcards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/mtgcards.json',
    hdfs_conn_id="hdfs",
    dag=dag,
)

hiveSQL_create_table_all_cards='''
CREATE EXTERNAL TABLE IF NOT EXISTS magic_cards(
	name            STRING,
    mana_cost       STRING,
    cmc             INT,
    colors          ARRAY<STRING>,
    color_identity  ARRAY<STRING>,
    type            STRING,
    subtypes        ARRAY<STRING>,
    rarity          STRING,
    set_code        STRING,
    set_name        STRING,
    text            STRING,
    artist          STRING,
    number          STRING,
    power           STRING,
    toughness       STRING,
    layout          STRING,
    multiverse_id   INT,
    image_url       STRING,
    variations      ARRAY<STRING>,
    foreign_names   ARRAY
    <
        STRUCT
        <
        name:STRING, 
        text:STRING, 
        type:STRING, 
        flavor:STRING, 
        image_url:STRING, 
        language:STRING, 
        multiverse_id:STRING
        >
    >,
	printings       ARRAY<STRING>,
	original_text   STRING,
	original_type   STRING,
	legalities      ARRAY
    <
        STRUCT
        <
        format:STRING, 
        legality:STRING
        >
    >,
	id STRING
    ) 
    COMMENT 'MTG_Cards' PARTITIONED BY (partition_year int, partition_month int, partition_day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE LOCATION '/user/hadoop/mtg/raw/mtgcards'
TBLPROPERTIES ('skip.header.line.count'='1');
'''

hiveSQL_select_all_cards ='''
SELECT * from magic_cards;
'''

select_all_cards = HiveOperator(
    task_id="select_all_cards",
    hql=hiveSQL_select_all_cards,
    hive_cli_conn_id="beeline",
    dag=dag,
)

create_HiveTable_all_magic_cards = HiveOperator(
    task_id='create_all_magic_cards_table',
    hql=hiveSQL_create_table_all_cards,
    hive_cli_conn_id='beeline',
    dag=dag,
) 


# def a python function
def getAllMTGCards():
    response = requests.get(
        "https://api.magicthegathering.io/v1/cards?pageSize=100&page=1"
    )
    totalCount = response.headers["Total-Count"]
    totalCount = int((int(totalCount) / 100))
    cards = response.json()["cards"]
    for i in range(2, 3):
        print(str(i) + "von" + str(totalCount))
        response = requests.get(    
            "https://api.magicthegathering.io/v1/cards?pageSize=100&page=" + str(i)
        )
        cards = cards + response.json()["cards"]
    cardsJson = json.dumps(cards)
    text_file = open("/home/airflow/mtg/mtgcards.json", "w")
    text_file.write(cardsJson)
    return


# call python function with PythonOperator
download_all_magic_cards = PythonOperator(
    task_id="download_all_magic_cards", python_callable=getAllMTGCards, dag=dag
)

# def a python function
def my_function(x):
    return x + " This is a Python function."


# call python function with PythonOperator
t1 = PythonOperator(
    task_id="print",
    python_callable=my_function,
    op_kwargs={"x": "Apache Airflow"},
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id="clear_import_dir",
    directory="/home/airflow/mtg/",
    pattern="*",
    dag=dag,
)

download_test_magic_card = HttpDownloadOperator(
    task_id="get_test_magic_card",
    download_uri="https://api.magicthegathering.io/v1/cards/4711",
    save_to="/home/airflow/mtg/4711_{{ ds }}.json",
    dag=dag,
)

dummy_op = DummyOperator(task_id="dummy", dag=dag)

# create_local_import_dir >> clear_local_import_dir >> download_test_magic_card
create_local_import_dir >> clear_local_import_dir >> download_all_magic_cards >> create_hdfs_all_cards_partition_dir >> hdfs_put_all_magic_cards >> create_HiveTable_all_magic_cards >> select_all_cards

# create_hdfs_all_cards_partition_dir
# Call python task with taskname (e.g. t1)
# t1
