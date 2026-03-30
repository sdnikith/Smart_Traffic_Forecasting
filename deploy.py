#!/usr/bin/env python3
"""
Traffic Platform Deployment Script
Sets up the entire traffic analytics platform
"""

import os
import sys
import logging
import subprocess
import time
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TrafficPlatformDeployer:
    """Deploy the complete Traffic Analytics Platform"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.env_file = self.project_root / ".env"
        
    def check_prerequisites(self):
        """Make sure we have everything needed to deploy"""
        logger.info("Checking prerequisites...")
        
        # Check Docker
        try:
            subprocess.run(["docker", "--version"], check=True, capture_output=True)
            logger.info("✓ Docker is available")
        except subprocess.CalledProcessError:
            logger.error("❌ Docker is not installed or not running")
            return False
        
        # Check Docker Compose
        try:
            subprocess.run(["docker-compose", "--version"], check=True, capture_output=True)
            logger.info("✓ Docker Compose is available")
        except subprocess.CalledProcessError:
            logger.error("❌ Docker Compose is not installed")
            return False
        
        # Check Python version
        if sys.version_info < (3, 8):
            logger.error("❌ Python 3.8+ is required")
            return False
        else:
            logger.info(f"✓ Python {sys.version_info.major}.{sys.version_info.minor} is available")
        
        # Check environment file
        if not self.env_file.exists():
            logger.warning("⚠️  .env file not found, creating template")
            self.create_env_template()
        else:
            logger.info("✓ .env file exists")
        
        return True
    
    def create_env_template(self):
        """Create environment file template"""
        logger.info("Creating .env template...")
        
        env_content = """# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093
SCHEMA_REGISTRY_URL=http://localhost:8081

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account.snowflakecomputing.com
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password

# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
S3_BUCKET=traffic-platform-lake
AWS_DEFAULT_REGION=us-east-1

# API Configuration
WEATHER_API_KEY=your_openweather_api_key
WEATHER_MOCK=false

# Monitoring Configuration
FRESHNESS_THRESHOLD_HOURS=2
ANOMALY_THRESHOLD_STD=2.5
SCHEMA_DRIFT_CHECK_ENABLED=true

# Airflow Configuration
AIRFLOW__CORE__FERNET_KEY=your-fernet-key-here
AIRFLOW__WEBSERVER__SECRET_KEY=your-secret-key-here
"""
        
        with open(self.env_file, 'w') as f:
            f.write(env_content)
        
        logger.info(f"Created .env template at {self.env_file}")
        logger.info("Please edit .env with your actual credentials")
    
    def install_python_dependencies(self):
        """Install Python dependencies"""
        logger.info("Installing Python dependencies...")
        
        requirements = [
            "apache-airflow==2.3.4",
            "apache-airflow-providers-apache-spark",
            "apache-airflow-providers-amazon",
            "apache-airflow-providers-postgres",
            "confluent-kafka",
            "snowflake-connector-python",
            "dbt-snowflake",
            "pyspark",
            "delta-lake",
            "boto3",
            "requests",
            "numpy",
            "pandas",
            "streamlit",
            "jupyter",
            "pytest"
        ]
        
        for req in requirements:
            try:
                subprocess.run([sys.executable, "-m", "pip", "install", req], 
                             check=True, capture_output=True)
                logger.info(f"✓ Installed {req}")
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Failed to install {req}: {e}")
                return False
        
        return True
    
    def deploy_kafka_infrastructure(self):
        """Deploy Kafka infrastructure"""
        logger.info("Deploying Kafka infrastructure...")
        
        kafka_dir = self.project_root / "kafka"
        compose_file = kafka_dir / "docker-compose-kafka.yml"
        
        if not compose_file.exists():
            logger.error(f"❌ Kafka compose file not found: {compose_file}")
            return False
        
        try:
            os.chdir(kafka_dir)
            subprocess.run(["docker-compose", "up", "-d"], check=True)
            logger.info("✓ Kafka infrastructure started")
            
            # Wait for Kafka to be ready
            logger.info("Waiting for Kafka to be ready...")
            time.sleep(30)
            
            # Check if Kafka is running
            result = subprocess.run(["docker-compose", "ps"], capture_output=True, text=True)
            if "Up" in result.stdout:
                logger.info("✓ Kafka is running")
            else:
                logger.error("❌ Kafka failed to start")
                return False
                
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to start Kafka: {e}")
            return False
        finally:
            os.chdir(self.project_root)
        
        return True
    
    def deploy_airflow_infrastructure(self):
        """Deploy Airflow infrastructure"""
        logger.info("Deploying Airflow infrastructure...")
        
        airflow_dir = self.project_root / "airflow"
        compose_file = airflow_dir / "docker-compose.yml"
        
        if not compose_file.exists():
            logger.error(f"❌ Airflow compose file not found: {compose_file}")
            return False
        
        try:
            os.chdir(airflow_dir)
            
            # Run setup script
            setup_script = airflow_dir / "setup_airflow.py"
            if setup_script.exists():
                subprocess.run([sys.executable, str(setup_script)], check=True)
                logger.info("✓ Airflow setup completed")
            
            # Start Airflow services
            subprocess.run(["docker-compose", "up", "-d"], check=True)
            logger.info("✓ Airflow infrastructure started")
            
            # Wait for Airflow to be ready
            logger.info("Waiting for Airflow to be ready...")
            time.sleep(60)
            
            # Check if Airflow is running
            result = subprocess.run(["docker-compose", "ps"], capture_output=True, text=True)
            if "Up" in result.stdout:
                logger.info("✓ Airflow is running")
            else:
                logger.error("❌ Airflow failed to start")
                return False
                
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to start Airflow: {e}")
            return False
        finally:
            os.chdir(self.project_root)
        
        return True
    
    def setup_snowflake(self):
        """Setup Snowflake database and warehouses"""
        logger.info("Setting up Snowflake...")
        
        snowflake_dir = self.project_root / "snowflake"
        sql_files = [
            "setup_warehouse.sql",
            "snowpipe_config.sql", 
            "rbac_security.sql",
            "cost_monitoring.sql"
        ]
        
        for sql_file in sql_files:
            sql_path = snowflake_dir / sql_file
            if sql_path.exists():
                logger.info(f"Please execute {sql_file} in your Snowflake account")
                logger.info(f"File location: {sql_path}")
            else:
                logger.warning(f"SQL file not found: {sql_path}")
        
        logger.info("⚠️  Please manually execute the SQL files in Snowflake")
        return True
    
    def run_dbt_setup(self):
        """Setup DBT project"""
        logger.info("Setting up DBT project...")
        
        dbt_dir = self.project_root / "dbt_project"
        
        if not dbt_dir.exists():
            logger.error(f"❌ DBT project directory not found: {dbt_dir}")
            return False
        
        try:
            os.chdir(dbt_dir)
            
            # Install DBT dependencies
            subprocess.run(["dbt", "deps"], check=True)
            logger.info("✓ DBT dependencies installed")
            
            # Run DBT
            subprocess.run(["dbt", "run"], check=True)
            logger.info("✓ DBT models executed")
            
            # Run tests
            subprocess.run(["dbt", "test"], check=True)
            logger.info("✓ DBT tests passed")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ DBT setup failed: {e}")
            return False
        finally:
            os.chdir(self.project_root)
        
        return True
    
    def start_dashboards(self):
        """Start monitoring dashboards"""
        logger.info("Starting monitoring dashboards...")
        
        dashboard_files = [
            "monitoring/simple_dashboard.py",
            "monitoring/traffic_prediction_dashboard.py",
            "monitoring/professional_dashboard.py"
        ]
        
        for dashboard_file in dashboard_files:
            dashboard_path = self.project_root / dashboard_file
            if dashboard_path.exists():
                logger.info(f"Dashboard available: {dashboard_path}")
                logger.info(f"Run: streamlit run {dashboard_path}")
            else:
                logger.warning(f"Dashboard not found: {dashboard_path}")
        
        return True
    
    def run_health_checks(self):
        """Run health checks on all components"""
        logger.info("Running health checks...")
        
        health_status = {
            "kafka": self.check_kafka_health(),
            "airflow": self.check_airflow_health(),
            "snowflake": self.check_snowflake_health(),
            "dbt": self.check_dbt_health()
        }
        
        logger.info("Health Check Results:")
        for service, status in health_status.items():
            status_icon = "✓" if status else "❌"
            logger.info(f"{status_icon} {service}: {'Healthy' if status else 'Unhealthy'}")
        
        return all(health_status.values())
    
    def check_kafka_health(self):
        """Check Kafka health"""
        try:
            kafka_dir = self.project_root / "kafka"
            os.chdir(kafka_dir)
            result = subprocess.run(["docker-compose", "ps"], capture_output=True, text=True)
            return "Up" in result.stdout and "kafka" in result.stdout
        except:
            return False
        finally:
            os.chdir(self.project_root)
    
    def check_airflow_health(self):
        """Check Airflow health"""
        try:
            airflow_dir = self.project_root / "airflow"
            os.chdir(airflow_dir)
            result = subprocess.run(["docker-compose", "ps"], capture_output=True, text=True)
            return "Up" in result.stdout and "airflow" in result.stdout
        except:
            return False
        finally:
            os.chdir(self.project_root)
    
    def check_snowflake_health(self):
        """Check Snowflake health"""
        # This would require actual Snowflake connection test
        logger.info("⚠️  Snowflake health check requires manual verification")
        return True
    
    def check_dbt_health(self):
        """Check DBT health"""
        try:
            dbt_dir = self.project_root / "dbt_project"
            os.chdir(dbt_dir)
            subprocess.run(["dbt", "debug"], check=True, capture_output=True)
            return True
        except:
            return False
        finally:
            os.chdir(self.project_root)
    
    def print_access_info(self):
        """Print access information for all services"""
        logger.info("\n" + "="*60)
        logger.info("🚀 TRAFFIC PLATFORM DEPLOYMENT COMPLETE!")
        logger.info("="*60)
        
        access_info = [
            ("Airflow UI", "http://localhost:8080", "admin/admin"),
            ("Kafka UI", "http://localhost:8081", "No auth"),
            ("Flower (Celery)", "http://localhost:5555", "admin/admin"),
            ("Spark UI", "http://localhost:8080", "No auth"),
            ("Jupyter", "http://localhost:8888", "No auth"),
        ]
        
        logger.info("\n📊 Service Access:")
        for service, url, auth in access_info:
            logger.info(f"  {service:<20} {url:<25} {auth}")
        
        logger.info("\n🎯 Dashboards:")
        logger.info("  Simple Dashboard: streamlit run monitoring/simple_dashboard.py")
        logger.info("  Prediction Dashboard: streamlit run monitoring/traffic_prediction_dashboard.py")
        logger.info("  Professional Dashboard: streamlit run monitoring/professional_dashboard.py")
        
        logger.info("\n📋 Next Steps:")
        logger.info("  1. Execute SQL files in Snowflake")
        logger.info("  2. Update .env with your credentials")
        logger.info("  3. Start Kafka producers")
        logger.info("  4. Enable Airflow DAGs")
        logger.info("  5. Access dashboards")
        
        logger.info("\n📚 Documentation:")
        logger.info("  README_COMPLETE.md - Complete documentation")
        logger.info("  docs/ - Detailed guides")
        
        logger.info("="*60)
    
    def deploy(self):
        """Deploy the complete platform"""
        logger.info("🚀 Starting Traffic Platform Deployment...")
        
        # Check prerequisites
        if not self.check_prerequisites():
            logger.error("❌ Prerequisites check failed")
            return False
        
        # Install dependencies
        if not self.install_python_dependencies():
            logger.error("❌ Python dependencies installation failed")
            return False
        
        # Deploy infrastructure
        if not self.deploy_kafka_infrastructure():
            logger.error("❌ Kafka deployment failed")
            return False
        
        if not self.deploy_airflow_infrastructure():
            logger.error("❌ Airflow deployment failed")
            return False
        
        # Setup Snowflake
        if not self.setup_snowflake():
            logger.error("❌ Snowflake setup failed")
            return False
        
        # Setup DBT
        if not self.run_dbt_setup():
            logger.error("❌ DBT setup failed")
            return False
        
        # Start dashboards
        if not self.start_dashboards():
            logger.error("❌ Dashboard setup failed")
            return False
        
        # Run health checks
        if not self.run_health_checks():
            logger.warning("⚠️  Some health checks failed")
        
        # Print access information
        self.print_access_info()
        
        logger.info("✅ Traffic Platform deployment completed successfully!")
        return True

def main():
    """Main deployment function"""
    deployer = TrafficPlatformDeployer()
    
    try:
        success = deployer.deploy()
        if success:
            logger.info("🎉 Deployment completed successfully!")
        else:
            logger.error("❌ Deployment failed!")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("⚠️  Deployment interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
