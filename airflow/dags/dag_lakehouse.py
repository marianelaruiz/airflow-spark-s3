import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dag_lakehouse",
    start_date=datetime(2024, 4, 22),
    schedule_interval=None,  # Execute manually
    catchup=False,
    description="Test DAG to simulate Lakehouse architecture",
    tags=["lakehouse"],
) as dag:
    
    # PYSPARK_PATH = "/home/marianela/Documentos/BulkConsulting/Cursos/3-Engenharia-de-dados/3-Apache-Airflow/lakehouse-airflow/pyspark"
    AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
    PYSPARK_PATH = f"{os.path.dirname(AIRFLOW_HOME)}/pyspark"
    PACKAGES = "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367"

    # Bronze layer
    bronze_customers = BashOperator(
        task_id="bronze_customers",
        bash_command=f"spark-submit {PACKAGES} {PYSPARK_PATH}/bronze/bronze_customers.py"
    )
    bronze_orders = BashOperator(
        task_id="bronze_orders",
        bash_command=f"spark-submit {PACKAGES} {PYSPARK_PATH}/bronze/bronze_orders.py"
    )
    bronze_items = BashOperator(
        task_id="bronze_order_items",
        bash_command=f"spark-submit {PACKAGES} {PYSPARK_PATH}/bronze/bronze_order_item.py"
    )

    

    # Silver layer    

    silver_customers = BashOperator(
        task_id="silver_customers",
        bash_command=f"spark-submit {PACKAGES} {PYSPARK_PATH}/silver/silver_customers.py"
    )
    silver_orders = BashOperator(
        task_id="silver_orders",
        bash_command=f"spark-submit {PACKAGES} {PYSPARK_PATH}/silver/silver_orders.py"
    )
    silver_items = BashOperator(
        task_id="silver_order_items",
        bash_command=f"spark-submit {PACKAGES} {PYSPARK_PATH}/silver/silver_order_item.py"
    )


    # Gold layer
    gold_task = BashOperator(
    task_id="gold_report",
    bash_command= f"spark-submit {PACKAGES} {PYSPARK_PATH}/gold/gold_report.py"    
    )
   
    # Dependencias
    bronze_customers >> silver_customers
    bronze_orders >> silver_orders
    bronze_items >> silver_items

    silver_customers >> gold_task
    silver_orders >> gold_task
    silver_items >> gold_task
