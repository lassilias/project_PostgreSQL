from datetime import timedelta
from datetime import datetime
# L'objet DAG nous sert à instancier notre séquence de tâches.
from airflow import DAG

# On importe les Operators dont nous avons besoin.
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Les arguments qui suivent vont être attribués à chaque Operators.
# Il est bien évidemment possible de changer les arguments spécifiquement pour un Operators.
# Vous pouvez vous renseigner sur la Doc d'Airflow des différents paramètres que l'on peut définir.



default_args = {
    "owner": "elyase",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "postgres_conn_id": "postgre_sql_db"
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("etl_LASSOULI", default_args=default_args, schedule_interval=timedelta(1))

import requests
from io import StringIO
import pandas as pd

# Création du DAG

###########################################################extract and transform task ################################################################################
def extract_and_transform(ti):

    r = requests.get(
    url='https://raw.githubusercontent.com/martj42/international_results/master/results.csv'
    )
    s=str(r.content,'utf-8')

    data = StringIO(s) 

    df=pd.read_csv(data)

    df['home_score'] = df['home_score'].fillna(0)
    df['away_score'] = df['away_score'].fillna(0)
    df['home_score'] = df['home_score'].astype(int)
    df['away_score'] = df['away_score'].astype(int)
    df.neutral = df.neutral.convert_dtypes()

    df.to_csv('/home/airflow/results.csv',index=False)

    r = requests.get(
    url='https://raw.githubusercontent.com/martj42/international_results/master/shootouts.csv'
    )
    s=str(r.content,'utf-8')

    data = StringIO(s) 

    df2 = pd.read_csv(data)
    df2['date'] = pd.to_datetime( df2['date'] )

    df2.to_csv('/home/airflow/shootouts.csv',index=False)


t1 = PythonOperator(
    task_id='extract_and_transform',
    python_callable=extract_and_transform,
    dag=dag
)
#########################################################create database MYSQL task###############################################################################

from airflow.providers.postgres.operators.postgres import PostgresOperator

t2 = PostgresOperator(
    task_id='TEST',
    sql=r""" SELECT 1; """,
    dag=dag
)


#################################################### transfer local files to server location ########################################################################
def dtype_mapping():
    return {'object' : 'TEXT',
        'int64' : 'INTEGER',
        'float64' : 'decimal',
        'datetime64' : 'DATE',
        'bool' : 'boolean',
        'boolean' : 'boolean',
        'datetime64[ns]' : 'DATE',
        'category' : 'TEXT',
        'int32': 'SMALLINT',
        'timedelta[ns]' : 'TEXT'}

def gen_tbl_cols_sql(df):
    dmap = dtype_mapping()
    sql = "pi_db_uid SERIAL PRIMARY KEY"
    df1 = df.rename(columns = {"" : "nocolname"})
    hdrs = df1.dtypes.index
    hdrs_list = [(hdr, str(df1[hdr].dtype)) for hdr in hdrs]
    for i, hl in enumerate(hdrs_list):
        sql += " ,{0} {1}".format(hl[0], dmap[hl[1]])
    return sql

def mapping_pandas_sql(ti):

    df=pd.read_csv('/home/airflow/results.csv')
    df['date'] = pd.to_datetime( df['date'] )
    tbl_cols_sql = gen_tbl_cols_sql(df)
    print(tbl_cols_sql.replace(',',',\n'))

    df2=pd.read_csv('/home/airflow/shootouts.csv')
    df2['date'] = pd.to_datetime( df2['date'] )
    tbl_cols_sql2 = gen_tbl_cols_sql(df2)

    ti.xcom_push(key='nom_col_sql', value=tbl_cols_sql.replace(',',',\n'))
    ti.xcom_push(key='nom_col_sql2', value=tbl_cols_sql2.replace(',',',\n'))


t3 = PythonOperator(
    task_id='mapping_pandas_sql',
    python_callable=mapping_pandas_sql,
    dag=dag,
    do_xcom_push=True
)

#########################################################create table in Database####################################################################

t4 = PostgresOperator(
    task_id='create_table_postgres_external_file',
    sql=r"""
    CREATE TABLE IF NOT EXISTS results(
    {{ task_instance.xcom_pull(key='nom_col_sql',task_ids='mapping_pandas_sql') }}
    );
    COPY results(date, home_team, away_team, home_score,away_score,tournament,city,country,neutral) FROM '/home/results.csv' DELIMITER ',' CSV HEADER;
    """,#.format(test=ti.xcom_pull(key='model_accuracy', task_ids=['training_model_A'])),
    dag=dag
)

t5 = PostgresOperator(
    task_id='create_table_mysql_external_file2',
    sql=r"""
    CREATE TABLE IF NOT EXISTS shootouts(
    {{ task_instance.xcom_pull(key='nom_col_sql2',task_ids='mapping_pandas_sql') }}
    );
    COPY shootouts(date, home_team, away_team, winner) FROM '/home/shootouts.csv' DELIMITER ',' CSV HEADER;
    ;""",#.format(test=ti.xcom_pull(key='model_accuracy', task_ids=['training_model_A'])),
    dag=dag
)


t6 = PostgresOperator(
    task_id='create_table_mysql_external_file3',
    sql=r"""
    CREATE TABLE IF NOT EXISTS credentials(
    Username TEXT ,
    Password TEXT );
    COPY credentials(Username, Password) FROM '/home/credentials.csv' DELIMITER ',' CSV HEADER;
    ;""",#.format(test=ti.xcom_pull(key='model_accuracy', task_ids=['training_model_A'])),
    dag=dag
)


t1 >> t2 >> t3 >> t4 >> t5 >> t6


