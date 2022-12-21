import random
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


RAND_FILE_PATH = '/opt/airflow/data/random_numbers.txt'

def hello():
	print('Airflow')


def random_numbers():
	num_1 = random.randint(1, 999999)
	num_2 = random.randint(1, 999999)
	with open(RAND_FILE_PATH, 'r+') as file:
		lines = file.readlines()
		file.seek(0)
		file.truncate()
		file.writelines(lines[:-1])
		file.write(f'{num_1} {num_2}\n')


def columns_difference():
	with open(RAND_FILE_PATH, 'r') as file:
		col_1_sum, col_2_sum = 0, 0
		for line in file:
			n1, n2 = line.split(' ')
			col_1_sum += int(n1)
			col_2_sum += int(n2)
	with open(RAND_FILE_PATH, 'a+') as file:
		file.write(f'{col_1_sum - col_2_sum}\n')


# A DAG represents a workflow, a collection of tasks.
with DAG(dag_id='hw_dag', start_date=datetime(2022, 12, 21), schedule="37-42 19 * * *") as dag:
	# Tasks are represented as operators
	bash_task = BashOperator(task_id='hello', bash_command="echo hello")
	python_task = PythonOperator(task_id='world', python_callable=hello)
	random_number_task = PythonOperator(task_id='random', python_callable=random_numbers)
	diff_task = PythonOperator(task_id='diff', python_callable=columns_difference)
	# Set dependencies between tasks
	bash_task >> python_task >> random_number_task >> diff_task
