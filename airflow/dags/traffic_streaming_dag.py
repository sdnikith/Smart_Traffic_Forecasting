"""
Traffic Streaming DAG - Advanced Airflow DAG for real-time processing
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago

from airflow.plugins.custom_operators import (
    KafkaProducerOperator,
    SnowflakeOperator,
    DeltaLakeOperator,
    ObservabilityOperator
)

# Default arguments
default_args = {
    'owner': 'traffic-platform',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
    'pool': 'traffic_streaming_pool'
}

# Create DAG
dag = DAG(
    'traffic_streaming_pipeline',
    default_args=default_args,
    description='Real-time traffic data streaming pipeline with Kafka, Spark, Delta Lake, and Snowflake',
    schedule_interval='@continuous',
    catchup=False,
    tags=['traffic', 'streaming', 'real-time'],
    max_active_runs=1
)

# Task 1: Start Kafka Traffic Producer
start_kafka_producer = KafkaProducerOperator(
    task_id='start_kafka_traffic_producer',
    kafka_config={
        'bootstrap_servers': Variable.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'topic': 'traffic-sensor-events',
        'producer_script': '/opt/airflow/dags/kafka/producer_traffic.py',
        'replay_speed_ms': 100,
        'schema_registry_url': Variable.get('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
    },
    dag=dag
)

# Task 2: Start Kafka Weather Producer
start_weather_producer = KafkaProducerOperator(
    task_id='start_kafka_weather_producer',
    kafka_config={
        'bootstrap_servers': Variable.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'topic': 'weather-updates',
        'producer_script': '/opt/airflow/dags/kafka/producer_weather.py',
        'api_key': Variable.get('WEATHER_API_KEY', ''),
        'mock_mode': Variable.get('WEATHER_MOCK', 'false'),
        'schema_registry_url': Variable.get('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
    },
    dag=dag
)

# Task 3: Run Spark Structured Streaming
spark_streaming = SparkSubmitOperator(
    task_id='spark_structured_streaming',
    application='/opt/airflow/dags/streaming/spark_traffic_consumer.py',
    conn_id='spark_default',
    driver_memory='2g',
    executor_memory='4g',
    executor_cores='2',
    num_executors='3',
    conf={
        'spark.sql.streaming.schemaInference': 'true',
        'spark.sql.shuffle.partitions': '10',
        'spark.databricks.delta.retentionDurationCheck.enabled': 'false',
        'spark.databricks.delta.schema.autoMerge.enabled': 'true'
    },
    env_vars={
        'KAFKA_BOOTSTRAP_SERVERS': Variable.get('KAFKA_BOOTSTRAP_SERVERS'),
        'SCHEMA_REGISTRY_URL': Variable.get('SCHEMA_REGISTRY_URL'),
        'S3_PATH': 's3://traffic-platform-lake/bronze/traffic_events/',
        'CHECKPOINT_LOCATION': 's3://traffic-platform-lake/checkpoints/traffic-streaming/'
    },
    dag=dag
)

# Task 4: Delta Lake Maintenance (runs daily)
delta_maintenance = DeltaLakeOperator(
    task_id='delta_lake_maintenance',
    delta_config={
        'operation': 'maintenance',
        's3_path': 's3://traffic-platform-lake/bronze/traffic_events/',
        'vacuum_hours': 168,  # 7 days
        'optimize_zorder_cols': 'sensor_id,timestamp'
    },
    dag=dag
)

# Task 5: DBT Run (runs hourly)
dbt_run = BashOperator(
    task_id='dbt_transformations',
    bash_command="""
        cd /opt/airflow/dags/dbt_project
        dbt run --target dev --models +staging +analytics
        dbt test --target dev
    """,
    env={
        'SNOWFLAKE_ACCOUNT': Variable.get('SNOWFLAKE_ACCOUNT'),
        'SNOWFLAKE_USER': Variable.get('SNOWFLAKE_USER'),
        'SNOWFLAKE_PASSWORD': Variable.get('SNOWFLAKE_PASSWORD'),
        'DBT_PROFILES_DIR': '/opt/airflow/dags/dbt_project'
    },
    dag=dag
)

# Task 6: Data Observability Check
observability_check = ObservabilityOperator(
    task_id='data_observability_check',
    observability_config={
        'freshness_check': True,
        'anomaly_detection': True,
        'schema_drift_check': True,
        'lineage_tracking': True,
        'cloudwatch_namespace': 'TrafficPlatform/Observability'
    },
    dag=dag
)

# Task 7: Snowflake Table Optimization (runs weekly)
snowflake_optimization = SnowflakeOperator(
    task_id='snowflake_optimization',
    snowflake_config={
        'operation': 'optimize',
        'warehouse': 'TRAFFIC_ANALYTICS_WH',
        'tables': ['MART_HOURLY_TRAFFIC', 'MART_DAILY_SUMMARY', 'MART_CONGESTION_ANALYSIS']
    },
    dag=dag
)

# Task 8: Health Check Alert
health_check_alert = EmailOperator(
    task_id='health_check_alert',
    to=['traffic-ops@company.com'],
    subject='Traffic Platform Health Check - {{ ds }}',
    html_content="""
        <h3>Traffic Platform Health Check Report</h3>
        <p><strong>Date:</strong> {{ ds }}</p>
        <p><strong>Status:</strong> {{ ti.xcom_pull(task_ids='data_observability_check')['status'] }}</p>
        <p><strong>Issues Found:</strong> {{ ti.xcom_pull(task_ids='data_observability_check')['issues_count'] }}</p>
        <hr>
        <p><em>This is an automated health check from the Traffic Analytics Platform.</em></p>
    """,
    dag=dag
)

# Define task dependencies
# Start producers in parallel
[start_kafka_producer, start_weather_producer] >> spark_streaming

# Spark streaming runs continuously, triggers other tasks
spark_streaming >> delta_maintenance
spark_streaming >> dbt_run
spark_streaming >> observability_check

# Daily/weekly tasks
delta_maintenance >> snowflake_optimization

# Observability triggers alert if issues found
observability_check >> health_check_alert

# Add SLA monitoring
sla_monitor = PythonOperator(
    task_id='sla_monitor',
    python_callable=lambda **context: {
        'dag_id': context['dag'].dag_id,
        'execution_date': context['execution_date'],
        'sla_status': 'passed' if context['task_instance'].duration < timedelta(hours=1) else 'failed'
    },
    dag=dag
)

# SLA monitoring depends on main pipeline
dbt_run >> sla_monitor
