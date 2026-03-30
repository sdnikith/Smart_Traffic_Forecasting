import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.impute import SimpleImputer

logger = logging.getLogger(__name__)


class TrafficFeatureEngineer:
    """Feature engineering for traffic forecasting"""
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.label_encoders = {}
        self.feature_columns = []
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and preprocess raw traffic data"""
        logger.info("Starting data cleaning")
        
        # Make a copy to avoid modifying original
        df_clean = df.copy()
        
        # Convert timestamp to datetime if not already
        df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'])
        
        # Remove duplicates
        before_count = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=['timestamp', 'location_id'])
        after_count = len(df_clean)
        logger.info(f"Removed {before_count - after_count} duplicate records")
        
        # Handle missing values
        df_clean = self._handle_missing_values(df_clean)
        
        # Remove outliers
        df_clean = self._remove_outliers(df_clean)
        
        # Sort by timestamp and location
        df_clean = df_clean.sort_values(['location_id', 'timestamp'])
        
        logger.info(f"Data cleaning completed. Final shape: {df_clean.shape}")
        return df_clean
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in the dataset"""
        # Forward fill for time series data
        numeric_columns = ['vehicle_count', 'avg_speed']
        
        for col in numeric_columns:
            if col in df.columns:
                # Forward fill within each location
                df[col] = df.groupby('location_id')[col].ffill()
                # Backward fill if still missing
                df[col] = df.groupby('location_id')[col].bfill()
                # Fill remaining with median
                if df[col].isnull().any():
                    median_val = df[col].median()
                    df[col] = df[col].fillna(median_val)
        
        return df
    
    def _remove_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove outliers using IQR method"""
        numeric_columns = ['vehicle_count', 'avg_speed']
        
        for col in numeric_columns:
            if col in df.columns:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                before_count = len(df)
                df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
                after_count = len(df)
                
                logger.info(f"Removed {before_count - after_count} outliers from {col}")
        
        return df
    
    def create_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create time-based features"""
        logger.info("Creating time features")
        
        df_features = df.copy()
        
        # Extract time components
        df_features['hour'] = df_features['timestamp'].dt.hour
        df_features['day_of_week'] = df_features['timestamp'].dt.dayofweek
        df_features['day_of_month'] = df_features['timestamp'].dt.day
        df_features['month'] = df_features['timestamp'].dt.month
        df_features['quarter'] = df_features['timestamp'].dt.quarter
        df_features['year'] = df_features['timestamp'].dt.year
        
        # Cyclical features
        df_features['hour_sin'] = np.sin(2 * np.pi * df_features['hour'] / 24)
        df_features['hour_cos'] = np.cos(2 * np.pi * df_features['hour'] / 24)
        df_features['day_sin'] = np.sin(2 * np.pi * df_features['day_of_week'] / 7)
        df_features['day_cos'] = np.cos(2 * np.pi * df_features['day_of_week'] / 7)
        df_features['month_sin'] = np.sin(2 * np.pi * df_features['month'] / 12)
        df_features['month_cos'] = np.cos(2 * np.pi * df_features['month'] / 12)
        
        # Rush hour indicators
        df_features['is_morning_rush'] = ((df_features['hour'] >= 7) & 
                                          (df_features['hour'] <= 9)).astype(int)
        df_features['is_evening_rush'] = ((df_features['hour'] >= 16) & 
                                          (df_features['hour'] <= 18)).astype(int)
        df_features['is_weekend'] = (df_features['day_of_week'] >= 5).astype(int)
        
        logger.info("Time features created successfully")
        return df_features
    
    def create_lag_features(self, df: pd.DataFrame, 
                           target_columns: List[str], 
                           lag_periods: List[int]) -> pd.DataFrame:
        """Create lag features for time series"""
        logger.info(f"Creating lag features for {lag_periods} periods")
        
        df_lags = df.copy()
        
        for col in target_columns:
            if col in df.columns:
                for lag in lag_periods:
                    lag_col = f"{col}_lag_{lag}"
                    df_lags[lag_col] = df_lags.groupby('location_id')[col].shift(lag)
        
        return df_lags
    
    def create_rolling_features(self, df: pd.DataFrame, 
                               target_columns: List[str], 
                               windows: List[int]) -> pd.DataFrame:
        """Create rolling window features"""
        logger.info(f"Creating rolling features for windows: {windows}")
        
        df_rolling = df.copy()
        
        for col in target_columns:
            if col in df.columns:
                for window in windows:
                    # Rolling mean
                    mean_col = f"{col}_rolling_mean_{window}"
                    df_rolling[mean_col] = df_rolling.groupby('location_id')[col].transform(
                        lambda x: x.rolling(window=window, min_periods=1).mean()
                    )
                    
                    # Rolling std
                    std_col = f"{col}_rolling_std_{window}"
                    df_rolling[std_col] = df_rolling.groupby('location_id')[col].transform(
                        lambda x: x.rolling(window=window, min_periods=1).std()
                    )
                    
                    # Rolling max
                    max_col = f"{col}_rolling_max_{window}"
                    df_rolling[max_col] = df_rolling.groupby('location_id')[col].transform(
                        lambda x: x.rolling(window=window, min_periods=1).max()
                    )
        
        return df_rolling
    
    def create_location_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create location-based features"""
        logger.info("Creating location features")
        
        df_loc = df.copy()
        
        # Encode location IDs
        if 'location_id' not in self.label_encoders:
            self.label_encoders['location_id'] = LabelEncoder()
            df_loc['location_encoded'] = self.label_encoders['location_id'].fit_transform(
                df_loc['location_id']
            )
        else:
            df_loc['location_encoded'] = self.label_encoders['location_id'].transform(
                df_loc['location_id']
            )
        
        # Location-specific statistics
        location_stats = df_loc.groupby('location_id').agg({
            'vehicle_count': ['mean', 'std', 'min', 'max'],
            'avg_speed': ['mean', 'std', 'min', 'max']
        }).round(2)
        
        location_stats.columns = ['_'.join(col).strip() for col in location_stats.columns]
        
        # Merge location statistics back
        df_loc = df_loc.merge(location_stats, left_on='location_id', 
                             right_index=True, how='left')
        
        return df_loc
    
    def create_interaction_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create interaction features"""
        logger.info("Creating interaction features")
        
        df_interactions = df.copy()
        
        # Traffic density feature (inverse relationship with speed)
        if 'vehicle_count' in df.columns and 'avg_speed' in df.columns:
            df_interactions['traffic_density'] = df_interactions['vehicle_count'] / (
                df_interactions['avg_speed'] + 1
            )
        
        # Speed variance during rush hours
        if 'avg_speed' in df.columns and 'is_morning_rush' in df.columns:
            df_interactions['rush_hour_speed_factor'] = (
                df_interactions['avg_speed'] * df_interactions['is_morning_rush']
            )
        
        if 'avg_speed' in df.columns and 'is_evening_rush' in df.columns:
            df_interactions['evening_rush_speed_factor'] = (
                df_interactions['avg_speed'] * df_interactions['is_evening_rush']
            )
        
        return df_interactions
    
    def prepare_features(self, df: pd.DataFrame, 
                        target_column: str = 'vehicle_count',
                        lag_periods: List[int] = [1, 2, 3, 6, 12, 24],
                        rolling_windows: List[int] = [3, 6, 12, 24]) -> pd.DataFrame:
        """Complete feature engineering pipeline"""
        logger.info("Starting complete feature engineering pipeline")
        
        # Clean data
        df_clean = self.clean_data(df)
        
        # Create all feature types
        df_features = self.create_time_features(df_clean)
        df_features = self.create_lag_features(df_features, [target_column], lag_periods)
        df_features = self.create_rolling_features(df_features, [target_column], rolling_windows)
        df_features = self.create_location_features(df_features)
        df_features = self.create_interaction_features(df_features)
        
        # Remove rows with NaN values created by lag features
        df_features = df_features.dropna()
        
        # Store feature columns (excluding target and timestamp)
        exclude_columns = [target_column, 'timestamp', 'location_id', 'source']
        self.feature_columns = [col for col in df_features.columns if col not in exclude_columns]
        
        logger.info(f"Feature engineering completed. Final shape: {df_features.shape}")
        logger.info(f"Total features created: {len(self.feature_columns)}")
        
        return df_features
    
    def scale_features(self, df: pd.DataFrame, fit: bool = True) -> pd.DataFrame:
        """Scale numerical features"""
        df_scaled = df.copy()
        
        if fit:
            df_scaled[self.feature_columns] = self.scaler.fit_transform(
                df[self.feature_columns]
            )
        else:
            df_scaled[self.feature_columns] = self.scaler.transform(
                df[self.feature_columns]
            )
        
        return df_scaled
