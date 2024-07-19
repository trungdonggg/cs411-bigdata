
import textwrap
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow.models.dag import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    "start_training_process",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),

    },
    description="start_training_process",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["training"],
) as dag:
    templated_command = textwrap.dedent(
    """
    python /home/acma2k3/cs411/orchestrator/start_training.py;
    """
    )
    start_training = BashOperator(
        task_id="start_training_process...",
        bash_command=templated_command,
    )

    start_training