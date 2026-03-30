import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import json
from typing import Dict, List, Optional
from sqlalchemy import create_engine
import logging
from abc import ABC, abstractmethod

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TrafficDataSource(ABC):
    """Abstract base class for traffic data sources"""
    
    @abstractmethod
    def fetch_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def validate_data(self, data: pd.DataFrame) -> bool:
        pass


class APITrafficSource(TrafficDataSource):
    """API-based traffic data source"""
    
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
    
    def fetch_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch traffic data from API"""
        try:
            params = {
                'api_key': self.api_key,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            }
            
            response = requests.get(f"{self.base_url}/traffic", params=params)
            response.raise_for_status()
            
            data = response.json()
            df = pd.DataFrame(data)
            
            if self.validate_data(df):
                logger.info(f"Successfully fetched {len(df)} records from API")
                return df
            else:
                raise ValueError("Invalid data received from API")
                
        except Exception as e:
            logger.error(f"Error fetching data from API: {e}")
            raise
    
    def validate_data(self, data: pd.DataFrame) -> bool:
        """Validate API response data"""
        required_columns = ['timestamp', 'location_id', 'vehicle_count', 'avg_speed']
        return all(col in data.columns for col in required_columns)


class DatabaseTrafficSource(TrafficDataSource):
    """Database-based traffic data source"""
    
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
    
    def fetch_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch traffic data from database"""
        try:
            query = f"""
            SELECT * FROM traffic_data 
            WHERE timestamp BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY timestamp
            """
            
            df = pd.read_sql(query, self.engine)
            
            if self.validate_data(df):
                logger.info(f"Successfully fetched {len(df)} records from database")
                return df
            else:
                raise ValueError("Invalid data retrieved from database")
                
        except Exception as e:
            logger.error(f"Error fetching data from database: {e}")
            raise
    
    def validate_data(self, data: pd.DataFrame) -> bool:
        """Validate database data"""
        required_columns = ['timestamp', 'location_id', 'vehicle_count', 'avg_speed']
        return all(col in data.columns for col in required_columns)


class TrafficDataCollector:
    """Main traffic data collection orchestrator"""
    
    def __init__(self, data_sources: List[TrafficDataSource]):
        self.data_sources = data_sources
    
    def collect_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Collect data from all configured sources"""
        all_data = []
        
        for source in self.data_sources:
            try:
                data = source.fetch_data(start_date, end_date)
                data['source'] = source.__class__.__name__
                all_data.append(data)
            except Exception as e:
                logger.warning(f"Failed to collect data from {source.__class__.__name__}: {e}")
                continue
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            combined_data = combined_data.sort_values('timestamp')
            logger.info(f"Collected total of {len(combined_data)} records from all sources")
            return combined_data
        else:
            raise ValueError("No data collected from any source")
    
    def generate_sample_data(self, start_date: datetime, end_date: datetime, 
                           locations: List[str]) -> pd.DataFrame:
        """Generate sample traffic data for testing"""
        date_range = pd.date_range(start=start_date, end=end_date, freq='15min')
        
        data = []
        for timestamp in date_range:
            for location in locations:
                # Simulate traffic patterns with some randomness
                hour = timestamp.hour
                base_traffic = 50 + 30 * np.sin(2 * np.pi * hour / 24)
                
                # Add rush hour effects
                if 7 <= hour <= 9 or 16 <= hour <= 18:
                    base_traffic *= 2
                
                vehicle_count = max(0, int(base_traffic + np.random.normal(0, 10)))
                avg_speed = max(5, 60 - vehicle_count / 10 + np.random.normal(0, 5))
                
                data.append({
                    'timestamp': timestamp,
                    'location_id': location,
                    'vehicle_count': vehicle_count,
                    'avg_speed': avg_speed,
                    'source': 'SampleData'
                })
        
        df = pd.DataFrame(data)
        logger.info(f"Generated {len(df)} sample records for {len(locations)} locations")
        return df
