#!/usr/bin/env python3
"""
Setup script for Traffic Platform Airflow
"""

import os
import sys
import logging
import subprocess
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AirflowSetup:
    """Setup Airflow for Traffic Platform"""
    
    def __init__(self):
        self.airflow_home = Path("/opt/airflow")
        self.dags_dir = self.airflow_home / "dags"
        self.plugins_dir = self.airflow_home / "plugins"
        self.logs_dir = self.airflow_home / "logs"
        
    def create_directories(self):
        """Create necessary directories"""
        logger.info("Creating Airflow directories")
        
        directories = [
            self.dags_dir,
            self.plugins_dir,
            self.logs_dir,
            self.dags_dir / "kafka",
            self.dags_dir / "streaming",
            self.dags_dir / "batch",
            self.dags_dir / "ml",
            self.plugins_dir / "custom_operators",
            self.plugins_dir / "custom_sensors"
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {directory}")
    
    def setup_airflow_config(self):
        """Setup Airflow configuration"""
        logger.info("Setting up Airflow configuration")
        
        config_content = """
# Airflow configuration for Traffic Platform
[core]
executor = CeleryExecutor
sql_alchemy_conn_id = postgres_default
sql_alchemy_conn_string = postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
load_examples = False
dags_folder = /opt/airflow/dags
plugins_folder = /opt/airflow/plugins
fernet_key = your-fernet-key-here
secret_key = your-secret-key-here

[celery]
broker_url = redis://redis:6379/0
result_backend = db+postgresql://airflow:airflow@postgres:5432/airflow

[webserver]
secret_key = your-secret-key-here
authenticate = True
auth_backend = airflow.api.auth.backend.basic_auth
executors = CeleryExecutor
        """
        
        config_file = self.airflow_home / "airflow.cfg"
        with open(config_file, 'w') as f:
            f.write(config_content)
        
        logger.info(f"Created Airflow config: {config_file}")
    
    def setup_connections(self):
        """Setup Airflow connections"""
        logger.info("Setting up Airflow connections")
        
        # This would typically be done via Airflow UI or CLI
        # Here we create a script for setting up connections
        setup_script = """
#!/bin/bash
# Setup Airflow connections

# Snowflake connection
airflow connections add \
    --conn-type snowflake \
    --conn-id snowflake_default \
    --conn-host your_account.snowflakecomputing.com \
    --conn-login your_username \
    --conn-password your_password \
    --conn-extra '{"account": "your_account", "warehouse": "TRAFFIC_TRANSFORM_WH", "database": "TRAFFIC_DB", "schema": "ANALYTICS"}'

# Kafka connection
airflow connections add \
    --conn-type kafka \
    --conn-id kafka_default \
    --conn-host localhost:9092 \
    --conn-extra '{"bootstrap.servers": "localhost:9092"}'

# S3 connection
airflow connections add \
    --conn-type aws \
    --conn-id aws_default \
    --conn-host s3.amazonaws.com \
    --conn-login your_aws_access_key \
    --conn-password your_aws_secret_key \
    --conn-extra '{"region": "us-east-1"}'

# PostgreSQL connection
airflow connections add \
    --conn-type postgres \
    --conn-id postgres_default \
    --conn-host localhost \
    --conn-schema postgres \
    --conn-login postgres \
    --conn-password postgres \
    --conn-port 5432 \
    --conn-extra '{"database": "airflow"}'
        """
        
        script_file = self.airflow_home / "setup_connections.sh"
        with open(script_file, 'w') as f:
            f.write(setup_script)
        
        # Make executable
        os.chmod(script_file, 0o755)
        
        logger.info(f"Created connection setup script: {script_file}")
    
    def setup_variables(self):
        """Setup Airflow variables"""
        logger.info("Setting up Airflow variables")
        
        variables_script = """
#!/bin/bash
# Setup Airflow variables

# Kafka configuration
airflow variables set KAFKA_BOOTSTRAP_SERVERS "localhost:9092,localhost:9093"
airflow variables set SCHEMA_REGISTRY_URL "http://localhost:8081"

# Snowflake configuration
airflow variables set SNOWFLAKE_ACCOUNT "your_account"
airflow variables set SNOWFLAKE_USER "your_username"
airflow variables set SNOWFLAKE_PASSWORD "your_password"

# S3 configuration
airflow variables set S3_BUCKET "traffic-platform-lake"

# API configuration
airflow variables set WEATHER_API_KEY "your_openweather_api_key"
airflow variables set WEATHER_MOCK "false"

# Monitoring thresholds
airflow variables set FRESHNESS_THRESHOLD_HOURS "2"
airflow variables set ANOMALY_THRESHOLD_STD "2.5"
airflow variables set SCHEMA_DRIFT_CHECK_ENABLED "true"
        """
        
        script_file = self.airflow_home / "setup_variables.sh"
        with open(script_file, 'w') as f:
            f.write(variables_script)
        
        # Make executable
        os.chmod(script_file, 0o755)
        
        logger.info(f"Created variables setup script: {script_file}")
    
    def install_plugins(self):
        """Install custom plugins"""
        logger.info("Installing custom plugins")
        
        # Create __init__.py files for plugins
        init_files = [
            self.plugins_dir / "__init__.py",
            self.plugins_dir / "custom_operators" / "__init__.py",
            self.plugins_dir / "custom_sensors" / "__init__.py"
        ]
        
        for init_file in init_files:
            init_file.parent.mkdir(parents=True, exist_ok=True)
            with open(init_file, 'w') as f:
                f.write("# Custom plugins for Traffic Platform\n")
        
        logger.info("Created plugin init files")
    
    def setup_permissions(self):
        """Setup proper permissions"""
        logger.info("Setting up permissions")
        
        # Set ownership and permissions
        commands = [
            f"chown -R airflow:airflow {self.airflow_home}",
            f"chmod -R 755 {self.airflow_home}",
            f"chmod -R 777 {self.logs_dir}",
            f"chmod -R 755 {self.dags_dir}",
            f"chmod -R 755 {self.plugins_dir}"
        ]
        
        for cmd in commands:
            try:
                subprocess.run(cmd, check=True, shell=True)
                logger.info(f"Executed: {cmd}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to execute {cmd}: {e}")
    
    def run_setup(self):
        """Run complete setup"""
        logger.info("Starting Airflow setup for Traffic Platform")
        
        try:
            # Create directories
            self.create_directories()
            
            # Setup configuration
            self.setup_airflow_config()
            
            # Setup connections and variables
            self.setup_connections()
            self.setup_variables()
            
            # Install plugins
            self.install_plugins()
            
            # Setup permissions
            self.setup_permissions()
            
            logger.info("Airflow setup completed successfully!")
            
            # Print next steps
            print("\n" + "="*50)
            print("Airflow Setup Completed!")
            print("="*50)
            print("\nNext Steps:")
            print("1. Update connection credentials in setup_connections.sh")
            print("2. Update API keys in setup_variables.sh")
            print("3. Run: docker-compose up -d")
            print("4. Access Airflow UI at: http://localhost:8080")
            print("5. Access Flower at: http://localhost:5555")
            print("6. Access Spark UI at: http://localhost:8080")
            print("\nDefault credentials:")
            print("Username: admin")
            print("Password: admin")
            print("="*50)
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            sys.exit(1)

def main():
    """Main setup function"""
    setup = AirflowSetup()
    setup.run_setup()

if __name__ == "__main__":
    main()
