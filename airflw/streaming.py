
import textwrap
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow.models.dag import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    "start_kafka_streaming",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),

    },
    description="start_kafka_streaming",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["kafka"],
) as dag:
    templated_command = textwrap.dedent(
    """
    python /home/acma2k3/cs411/orchestrator/start_kafka_stream.py;
    """
    )
    start_streaming = BashOperator(
        task_id="start_kafka_streaming...",
        bash_command=templated_command,
    )

    start_streaming