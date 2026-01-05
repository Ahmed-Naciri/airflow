from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'ahmed naciri',
    'start_date': days_ago(0),
    'email': ['ahmednaciri1980@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval='@daily',
    default_args=default_args,
    description='Apache Airflow Final Assignment'
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvf tolldata.tgz -C .',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d "," -f 1,2,3,4 vehicle-data.csv > csv_data.csv',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f 2,3,4 tollplaza-data.tsv > tsv_data.csv',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 1-5,6-10 payment-data.txt > fixed_width_data.csv',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='''
    cut -f1-3 extracted_data.csv > part1.csv
    cut -f4 extracted_data.csv | tr '[:lower:]' '[:upper:]' > part2.csv
    cut -f5-9 extracted_data.csv > part3.csv
    paste part1.csv part2.csv part3.csv > transformed_data.csv
    ''',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'ahmed naciri',
    'start_date': days_ago(0),
    'email': ['ahmednaciri1980@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval='@daily',
    default_args=default_args,
    description='Apache Airflow Final Assignment'
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvf tolldata.tgz -C .',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d "," -f 1,2,3,4 vehicle-data.csv > csv_data.csv',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f 2,3,4 tollplaza-data.tsv > tsv_data.csv',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 1-5,6-10 payment-data.txt > fixed_width_data.csv',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='''
    cut -f1-3 extracted_data.csv > part1.csv
    cut -f4 extracted_data.csv | tr '[:lower:]' '[:upper:]' > part2.csv
    cut -f5-9 extracted_data.csv > part3.csv
    paste part1.csv part2.csv part3.csv > transformed_data.csv
    ''',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'ahmed naciri',
    'start_date': days_ago(0),
    'email': ['ahmednaciri1980@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval='@daily',
    default_args=default_args,
    description='Apache Airflow Final Assignment'
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvf tolldata.tgz -C .',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d "," -f 1,2,3,4 vehicle-data.csv > csv_data.csv',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f 2,3,4 tollplaza-data.tsv > tsv_data.csv',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 1-5,6-10 payment-data.txt > fixed_width_data.csv',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='''
    cut -f1-3 extracted_data.csv > part1.csv
    cut -f4 extracted_data.csv | tr '[:lower:]' '[:upper:]' > part2.csv
    cut -f5-9 extracted_data.csv > part3.csv
    paste part1.csv part2.csv part3.csv > transformed_data.csv
    ''',
    cwd='/home/project/airflow/dags/finalassignment',
    dag=dag
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data



