from datetime import datetime
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


pg_hostname = 'host.docker.internal'
pg_port = '5430'
pg_username = 'postgres'
pg_pass = 'postgres'
pd_db = 'test'


def hello():
	print('Airflow')


def connect_to_psql():
	conn = psycopg2.connect(
		host=pg_hostname,
		port=pg_port,
		user=pg_username,
		password=pg_pass,
		database=pd_db
	)
	cur = conn.cursor()
	cur.execute('CREATE TABLE test_table (id serial PRIMARY KEY, num integer, data varchar);')
	cur.execute('INSERT INTO test_table (num, data) VALUES (%s, %s)', (100, "abc'def"))

	# cur.fetchall()
	conn.commit()
	cur.close()
	conn.close()


def read_from_psql():
	conn = psycopg2.connect(
		host=pg_hostname,
		port=pg_port,
		user=pg_username,
		password=pg_pass,
		database=pd_db
	)
	cur = conn.cursor()
	cur.execute('SELECT * FROM test_table')
	print(cur.fetchall())
	cur.close()
	conn.close()


# A DAG represents a workflow, a collection of tasks.
with DAG(dag_id='second_dag', start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:
	conn_to_psql_task = PythonOperator(task_id='conn_to_psql', python_callable=connect_to_psql)
	read_from_psql_task = PythonOperator(task_id='read_from_psql', python_callable=read_from_psql)
	# Set dependencies between tasks
	conn_to_psql_task >> read_from_psql_task
