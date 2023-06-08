import datetime
from pytz import timezone
from datetime import timedelta
import airflow
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook

GCE_INSTANCE = 'myinst0'
GCE_ZONE = 'us-central1-a'
GCP_PROJECT_ID = 'project-test1-389013'

t1_bash="""
/home/amar_gcplearning7872/python_projects/python_envs/myenv/bin/python /home/amar_gcplearning7872/python_projects/test_project1/main.py
"""

with airflow.DAG(
        dag_id = 'ETL1_process',
       schedule_interval='*/3 * * * *',
       start_date=datetime.datetime(2023, 6, 8, 17,0,0,tzinfo=timezone("Europe/Paris")),
          dagrun_timeout=timedelta(seconds=120)
        ) as dag:

  ssh_task = SSHOperator(
      task_id='composer_compute_ssh_task',
      ssh_hook=ComputeEngineSSHHook(
          instance_name=GCE_INSTANCE,
          zone=GCE_ZONE,
          project_id=GCP_PROJECT_ID,
          user='amar_gcplearning7872',
          use_oslogin=False,
          use_iap_tunnel=False,
          use_internal_ip=True),
      command=t1_bash,
      dag=dag)