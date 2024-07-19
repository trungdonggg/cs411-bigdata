
import textwrap
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow.models.dag import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    "submit_to_spark",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),

    },
    description="submit_to_spark",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["spark"],
) as dag:
    templated_command = textwrap.dedent(
    """
    python /home/acma2k3/cs411/orchestrator/start_submitting.py;
    """
    )
    start_submitting = BashOperator(
        task_id="start_submitting...",
        bash_command=templated_command,
    )

    start_submitting