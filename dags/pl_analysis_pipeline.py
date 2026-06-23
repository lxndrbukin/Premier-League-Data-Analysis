from airflow.decorators import dag, task
from datetime import datetime
import sys
sys.path.append('/home/lxndrbukin/Coding/projects/pl_data_analysis')
from ingest import main
import subprocess

@dag(
    'pl_analysis',
    start_date=datetime(2026,6,1),
    schedule_interval='@weekly',
    catchup=False,
    description=''
)
def pl_analysis_pipeline():
    @task
    def ingest_data():
        main()
    
    @task
    def run_emr_job():
        result = subprocess.run(
            ['/home/lxndrbukin/Coding/projects/pl_data_analysis/aws_emr.bash'],
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"Bash script failed: {result.stderr}")

    ingest_data() >> run_emr_job()

pl_analysis_pipeline()