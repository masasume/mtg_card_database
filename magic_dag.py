# create or edit with:
# vi /home/airflow/airflow/dags/magic_dag.py
# Total amount of work: 4 Hours

from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator

import requests
import json

args = {
    'owner': 'airflow'
}

dag = DAG('MagicDAG', default_args=args, description='The DAG to get all Magic Cards',
        schedule_interval='56 18 * * *',
        start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow/',
    directory='mtg',
    dag=dag,
)

create_hdfs_all_cards_partition_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_raw_dir',
    directory='/user/hadoop/mtg/raw{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_all_magic_cards = HdfsPutFileOperator(
    task_id='hdfs_put_all_magic_cards',
    local_file='/home/airflow/mtg/mtgcards.json',
    remote_file='/user/hadoop/mtg/raw/mtgcards_{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/title.ratings_{{ ds }}.tsv',
    hdfs_conn_id='hdfs',
    dag=dag,
)


# def a python function
def getAllMTGCards():
    response = requests.get("https://api.magicthegathering.io/v1/cards?pageSize=100&page=1")
    totalCount = response.headers["Total-Count"]
    totalCount = int((int(totalCount) / 100))
    cards = response.json()["cards"]
    for i in range(2, 5):
        print(str(i) + "von" + str(totalCount))
        response = requests.get("https://api.magicthegathering.io/v1/cards?pageSize=100&page=" + str(i))
        cards = cards + response.json()["cards"]
    cardsJson = json.dumps(cards)
    text_file = open("/home/airflow/mtg/mtgcards.json", "w")
    text_file.write(cardsJson)
    return

# call python function with PythonOperator
download_all_magic_cards = PythonOperator(
    task_id='download_all_magic_cards',
    python_callable = getAllMTGCards,
    dag=dag
)

# def a python function
def my_function(x):
    return x + " This is a Python function."

# call python function with PythonOperator
t1 = PythonOperator(
    task_id='print',
    python_callable = my_function,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory='/home/airflow/mtg/',
    pattern='*',
    dag=dag,
)

download_test_magic_card = HttpDownloadOperator(
        task_id='get_test_magic_card',
        download_uri='https://api.magicthegathering.io/v1/cards/4711',
        save_to='/home/airflow/mtg/4711_{{ ds }}.json',
        dag=dag,
        )

dummy_op = DummyOperator(
        task_id='dummy',
        dag=dag)

create_local_import_dir >> clear_local_import_dir >> download_test_magic_card
clear_local_import_dir >> download_all_magic_cards >> create_hdfs_all_cards_partition_dir >> hdfs_put_all_magic_cards
#create_hdfs_all_cards_partition_dir
# Call python task with taskname (e.g. t1)
t1