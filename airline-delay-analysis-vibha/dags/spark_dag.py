# Import modern annotations
from __future__ import annotations

# Importing necessary libraries
import pendulum                # For timezone-aware datetime
import logging
from airflow.models import DAG, Variable   # DAG definition and Airflow Variables
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.email import send_email

# Initialize logger for the DAG
logger = logging.getLogger("airline_dag")


# Airflow Variables (with defaults)

# Connection IDs and config variables fetched from Airflow Variables, with fallback default values
SPARK_CONN_ID = Variable.get("SPARK_CONN_ID", default_var="spark_default")
MYSQL_CONN_ID = Variable.get("MYSQL_CONN_ID", default_var="mysql_default")
SLACK_CONN_ID = Variable.get("SLACK_CONN_ID", default_var="slack_connection")

# Paths and class names for Spark JAR jobs
JARS_PATH = Variable.get("JARS_PATH", default_var="/opt/airflow/jars")
SCHEMA_FILE = Variable.get("SCHEMA_FILE", default_var="/schema.sql")
CLASS_INGEST = Variable.get("CLASS_INGEST", default_var="org.spark.DataIngestion")
CLASS_CLEAN = Variable.get("CLASS_CLEAN", default_var="org.spark.DataCleaning")
CLASS_TRANSFORM = Variable.get("CLASS_TRANSFORM", default_var="org.spark.DataTransformation")


# Paths for JAR files
JAR_INGEST = f"{JARS_PATH}/data-ingestion-job-jar-with-dependencies.jar"
JAR_CLEAN = f"{JARS_PATH}/data-cleaning-job-jar-with-dependencies.jar"
JAR_TRANSFORM = f"{JARS_PATH}/data-transformation-job-jar-with-dependencies.jar"

# Notification Functions
# Function called when any task succeeds
def task_success(context):
    logger.info(f"SUCCESS: {context['task_instance'].task_id}")   # Logs success message in Airflow logs

    subject = f"Airflow DAG success: {context['dag'].dag_id}"     # Email subject line
    to="mail2vibha0406@gmail.com"                                 # Recipient email
    html_content = f"""
    <h3>DAG Success! {context['dag'].dag_id}</h3>
    <p>SUCCESS: {context['task_instance'].task_id}</p>
    """
    # Send email notification on success
    send_email(to=to, subject=subject, html_content=html_content)

    # Send Slack message using Webhook
    SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id=SLACK_CONN_ID,                      # Slack connection ID
        message=":large_green_circle: Task *{task}* succeeded in DAG *{dag}*".format(
            task=context['task_instance'].task_id,                # Replace placeholders with task info
            dag=context['dag'].dag_id,
        ),
        username="airflow",                                       # Sender name on Slack
    ).execute(context=context)                                    # Execute immediately (not scheduled as a task)

# Function called when any task fails
def task_fail(context):
    logger.error(f"FAILED: {context['task_instance'].task_id}")   # Logs failure message

    subject = f"Airflow DAG failed: {context['dag'].dag_id}"      # Email subject
    to = f"mail2vibha0406@gmail.com"                              # Recipient
    html_content = f"""
    <h3>DAG Failed! {context['dag'].dag_id}</h3>
    <p>FAILED: {context['task_instance'].task_id}</p>
    """
    # Send email notification on failure
    send_email(to=to, subject=subject, html_content=html_content)

    # Send Slack message on failure
    SlackWebhookOperator(
        task_id="notify_failure",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=":red_circle: Task *{task}* failed in DAG *{dag}*".format(
            task=context['task_instance'].task_id,
            dag=context['dag'].dag_id,
        ),
        username="airflow",
    ).execute(context=context)


# Default arguments for all tasks in DAG
default_args = {
    "owner": "Vibha",                     # DAG owner name
    "depends_on_past": False,             # Task doesn’t depend on previous runs
    "retries": 0,                         # Retry once on failure
#     "on_failure_callback": task_fail,     # Function to run on failure
#     "on_success_callback": task_success,  # Function to run on success
    "verbose": True,                      # Enables detailed logs
}

# Define DAG
with DAG(
    dag_id="java_spark_etl_dag",                              # Unique name for DAG
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Kolkata"),  # Start date and timezone
    catchup=False,                                             # Prevents backfilling old runs
    schedule_interval="None",                                  # Runs once daily - @daily
    tags=["spark", "java", "mysql", "etl"],                    # Tags for organization
    default_args=default_args,                                 # Common arguments applied to all tasks
) as dag:

    # Step 1: Create schema in MySQL database
    create_schema = MySqlOperator(
        task_id="create_schema",              # Task name in Airflow UI
        mysql_conn_id=MYSQL_CONN_ID,          # MySQL connection
        sql=SCHEMA_FILE,                      # SQL file path to execute
    )

    # Step 2: Data Ingestion
    task_ingest = SparkSubmitOperator(
        task_id="data_ingestion",             # Task name
        conn_id=SPARK_CONN_ID,                # Spark connection ID
        application=JAR_INGEST,               # Path to ingestion JAR
        java_class=CLASS_INGEST,              # Main class inside the JAR
    )

    # Step 3: Data Cleaning
    task_clean = SparkSubmitOperator(
        task_id="data_cleaning",
        conn_id=SPARK_CONN_ID,
        application=JAR_CLEAN,
        java_class=CLASS_CLEAN,
    )

    # Step 4: Data Transformation
    task_transform = SparkSubmitOperator(
        task_id="data_transformation",
        conn_id=SPARK_CONN_ID,
        application=JAR_TRANSFORM,
        java_class=CLASS_TRANSFORM,
    )


    # Ensures sequential execution
    create_schema >> task_ingest >> task_clean >> task_transform
