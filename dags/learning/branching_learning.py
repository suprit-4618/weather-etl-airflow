from datetime import datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator


def choose_branch():
    condition = True  # Change this to False and test
    if condition:
        return "task_if_true"
    else:
        return "task_if_false"


with DAG(
    dag_id="branching_learning",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
):
    branch = BranchPythonOperator(
        task_id="branch_decision", python_callable=choose_branch
    )

    task_true = BashOperator(
        task_id="task_if_true", bash_command="echo TRUE branch executed"
    )

    task_false = BashOperator(
        task_id="task_if_false", bash_command="echo FALSE branch executed"
    )

    branch >> [task_true, task_false]
