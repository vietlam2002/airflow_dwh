# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from datetime import datetime

# def check_value(ti, **kwargs):
#     value = kwargs.get('value', 'default_value')
#     if value == 'A':
#         ti.xcom_push(key='next_task', value='task_A')
#     else:
#         ti.xcom_push(key='next_task', value='task_B')

# def task_a():
#     print("Executing Task A")

# def task_b():
#     print("Executing Task B")

# with DAG(dag_id='example_task_branching', start_date=datetime(2025, 3, 13), schedule_interval=None) as dag:

#     check_task = BranchPythonOperator(
#         task_id='check_task',
#         python_callable=check_value,
#         op_kwargs={'value': 'A'},
#     )
#     task_A = PythonOperator(
#         task_id='task_A',
#         python_callable=task_a,
#     )

#     task_B = PythonOperator(
#         task_id='task_B',
#         python_callable=task_b,
#     )

#     check_task >> [task_A, task_B]