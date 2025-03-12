from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
import pandas as pd
import json
import os

#default arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

#define DAG
dag = DAG(
    'pipeline',
    default_args=default_args,
    description='ETL pipeline for career analytics data',
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    start_date=datetime(2025, 3, 12),
    catchup=False,
    tags=['career_analytics', 'etl'],
    max_active_runs=1
)

def extract_data(**kwargs):
    """
    Extract data from MongoDB and convert to JSON file
    Incremental extraction based on last_updated timestamp
    """
    with open('/opt/airflow/data/professionals_nested.json', 'r') as f:
        data = json.load(f)

    staging_file = f"/opt/airflow/data/staging/extract_{kwargs['ts_nodash']}.json"
    os.makedirs(os.path.dirname(staging_file), exist_ok=True)
    
    with open(staging_file, 'w') as f:
        json.dump(data, f)
    
    return staging_file

def transform_professionals(extraction_file, **kwargs):
    with open(extraction_file, 'r') as f:
        data = json.load(f)
    
    professionals = data['professionals']

    professionals_df = pd.DataFrame([{
        'professional_id': p['professional_id'],
        'years_experience': p['years_experience'],
        'current_industry': p['current_industry'],
        'current_role': p['current_role'],
        'education_level': p['education_level']
    } for p in professionals])

    output_file = f"/opt/airflow/data/staging/dim_professionals_{kwargs['ts_nodash']}.csv"
    professionals_df.to_csv(output_file, index=False)
    
    return output_file

def transform_jobs(extraction_file, **kwargs):
    with open(extraction_file, 'r') as f:
        data = json.load(f)
    
    professionals = data['professionals']
    jobs_list = []
    for p in professionals:
        for job in p['jobs']:
            job_dict = job.copy()
            job_dict['professional_id'] = p['professional_id']
            jobs_list.append(job_dict)
    
    jobs_df = pd.DataFrame(jobs_list)
    output_file = f"/opt/airflow/data/staging/fact_jobs_{kwargs['ts_nodash']}.csv"
    jobs_df.to_csv(output_file, index=False)
    
    return output_file

def transform_skills(extraction_file, **kwargs):
    with open(extraction_file, 'r') as f:
        data = json.load(f)
    
    professionals = data['professionals']

    skills_list = []
    for p in professionals:
        for skill in p['skills']:
            skill_dict = skill.copy()
            skill_dict['professional_id'] = p['professional_id']
            skills_list.append(skill_dict)
    
    skills_df = pd.DataFrame(skills_list)
    output_file = f"/opt/airflow/data/staging/dim_skills_{kwargs['ts_nodash']}.csv"
    skills_df.to_csv(output_file, index=False)
    
    return output_file

def transform_certifications(extraction_file, **kwargs):
    with open(extraction_file, 'r') as f:
        data = json.load(f)
    
    professionals = data['professionals']

    certs_list = []
    for p in professionals:
        for cert in p.get('certifications', []):
            cert_dict = cert.copy()
            cert_dict['professional_id'] = p['professional_id']
            certs_list.append(cert_dict)
    
    certs_df = pd.DataFrame(certs_list)
    output_file = f"/opt/airflow/data/staging/dim_certifications_{kwargs['ts_nodash']}.csv"
    certs_df.to_csv(output_file, index=False)
    
    return output_file

def transform_education(extraction_file, **kwargs):
    with open(extraction_file, 'r') as f:
        data = json.load(f)
    
    professionals = data['professionals']
    
    education_list = []
    for p in professionals:
        for edu in p['education']:
            edu_dict = edu.copy()
            edu_dict['professional_id'] = p['professional_id']
            education_list.append(edu_dict)
    
    education_df = pd.DataFrame(education_list)
    
    output_file = f"/opt/airflow/data/staging/dim_education_{kwargs['ts_nodash']}.csv"
    education_df.to_csv(output_file, index=False)
    
    return output_file

def load_to_warehouse(**kwargs):


#create tasks
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

#unfinished for taskgroup
with TaskGroup(group_id='transform_tasks', dag=dag) as transform_group:
    transform_professionals_task = PythonOperator(
        task_id='transform_professionals',
        python_callable=transform_professionals,
        op_kwargs={'extraction_file': "{{ ti.xcom_pull(task_ids='extract_data') }}"},
        provide_context=True,
        dag=dag
    )
    """
    need to add following tasks
    transform_jobs_task
    transform_skills_task
    transform_certifications_task
    transform_education_task
    """
validate_data_task = PythonOperator(
    task_id='validate_data'
    python_callable=validate_data
)

load_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    provide_context=True,
    dag=dag
)
#need to add validation and generate metrics

start >> extract_task >> transform_group >> validate_data_task >> generate_metrics_task >> load_task >> end