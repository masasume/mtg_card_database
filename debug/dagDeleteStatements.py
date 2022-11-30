# The following tasks are great to debug the airflow dag.

# drops the table magic_cards
hiveSQL_drop_cards_table='''
DROP TABLE IF EXISTS magic_cards;
'''

# drops the table foreign_magic_cards
hiveSQL_drop_foreign_cards_table='''
DROP TABLE IF EXISTS foreign_magic_cards;
'''

# drops the table magic_cards_reduced
hiveSQL_drop_cards_reduced_table='''
DROP TABLE IF EXISTS magic_cards_reduced;
'''

# to assure seemless updates we drop the user_magic_cards table and recreate it in the following function.
def mySQL_drop_user_magic_cards_table():
    query = '''DROP TABLE IF EXISTS user_magic_cards;'''
    executeMySQLQueryViaSSH(query, "MagicTheGathering")

# calls the hive sql to drop the hive table magic_cards
drop_HiveTable_magic_cards = HiveOperator(
    task_id='drop_HiveTable_magic_cards',
    hql=hiveSQL_drop_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

# calls the hive sql to drop the hive table foreign_magic_cards
drop_HiveTable_foreign_magic_cards = HiveOperator(
    task_id='drop_HiveTable_foreign_magic_cards',
    hql=hiveSQL_drop_foreign_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

# calls the hive sql to drop the hive table magic_cards_reduced
drop_HiveTable_magic_cards_reduced = HiveOperator(
    task_id='drop_HiveTable_magic_cards_reduced',
    hql=hiveSQL_drop_cards_reduced_table,
    hive_cli_conn_id='beeline',
    dag=dag)

# call the hive sql to drop the table magic_cards
delete_MySQLTable_user_magic_cards = PythonOperator(
    task_id='delete_mysql_user_magic_cards_table',
    python_callable = mySQL_drop_user_magic_cards_table,
    op_kwargs = {},
    dag=dag
)

# drop all tables in hive and in mysql
drop_HiveTable_magic_cards >> drop_HiveTable_foreign_magic_cards >> drop_HiveTable_magic_cards_reduced >> delete_MySQLTable_user_magic_cards