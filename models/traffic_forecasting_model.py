import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
from abc import ABC, abstractmethod
import pickle
import joblib

# ML imports
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split, TimeSeriesSplit, GridSearchCV

# Deep learning imports
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout, Conv1D, MaxPooling1D, Flatten
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau

logger = logging.getLogger(__name__)


class BaseTrafficModel(ABC):
    """Abstract base class for traffic forecasting models"""
    
    def __init__(self, model_name: str):
        self.model_name = model_name
        self.model = None
        self.is_trained = False
        self.feature_columns = []
        self.target_column = 'vehicle_count'
    
    @abstractmethod
    def train(self, X: pd.DataFrame, y: pd.Series) -> None:
        pass
    
    @abstractmethod
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        pass
    
    @abstractmethod
    def save_model(self, filepath: str) -> None:
        pass
    
    @abstractmethod
    def load_model(self, filepath: str) -> None:
        pass
    
    def evaluate(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, float]:
        """Evaluate model performance"""
        if not self.is_trained:
            raise ValueError("Model must be trained before evaluation")
        
        predictions = self.predict(X)
        
        metrics = {
            'mae': mean_absolute_error(y, predictions),
            'mse': mean_squared_error(y, predictions),
            'rmse': np.sqrt(mean_squared_error(y, predictions)),
            'r2': r2_score(y, predictions),
            'mape': np.mean(np.abs((y - predictions) / y)) * 100
        }
        
        return metrics


class LinearTrafficModel(BaseTrafficModel):
    """Linear regression model for traffic forecasting"""
    
    def __init__(self, model_type: str = 'ridge'):
        super().__init__(f"Linear_{model_type}")
        if model_type == 'ridge':
            self.model = Ridge(alpha=1.0)
        elif model_type == 'lasso':
            self.model = Lasso(alpha=1.0)
        else:
            self.model = LinearRegression()
    
    def train(self, X: pd.DataFrame, y: pd.Series) -> None:
        logger.info(f"Training {self.model_name}")
        
        self.feature_columns = X.columns.tolist()
        self.model.fit(X, y)
        self.is_trained = True
        
        logger.info(f"{self.model_name} training completed")
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if not self.is_trained:
            raise ValueError("Model must be trained before prediction")
        
        return self.model.predict(X)
    
    def save_model(self, filepath: str) -> None:
        model_data = {
            'model': self.model,
            'feature_columns': self.feature_columns,
            'model_name': self.model_name,
            'is_trained': self.is_trained
        }
        joblib.dump(model_data, filepath)
        logger.info(f"Model saved to {filepath}")
    
    def load_model(self, filepath: str) -> None:
        model_data = joblib.load(filepath)
        self.model = model_data['model']
        self.feature_columns = model_data['feature_columns']
        self.model_name = model_data['model_name']
        self.is_trained = model_data['is_trained']
        logger.info(f"Model loaded from {filepath}")


class EnsembleTrafficModel(BaseTrafficModel):
    """Ensemble model for traffic forecasting"""
    
    def __init__(self, model_type: str = 'random_forest'):
        super().__init__(f"Ensemble_{model_type}")
        
        if model_type == 'random_forest':
            self.model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42
            )
        elif model_type == 'gradient_boosting':
            self.model = GradientBoostingRegressor(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                random_state=42
            )
        else:
            raise ValueError(f"Unknown ensemble model type: {model_type}")
    
    def train(self, X: pd.DataFrame, y: pd.Series) -> None:
        logger.info(f"Training {self.model_name}")
        
        self.feature_columns = X.columns.tolist()
        self.model.fit(X, y)
        self.is_trained = True
        
        logger.info(f"{self.model_name} training completed")
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if not self.is_trained:
            raise ValueError("Model must be trained before prediction")
        
        return self.model.predict(X)
    
    def save_model(self, filepath: str) -> None:
        model_data = {
            'model': self.model,
            'feature_columns': self.feature_columns,
            'model_name': self.model_name,
            'is_trained': self.is_trained
        }
        joblib.dump(model_data, filepath)
        logger.info(f"Model saved to {filepath}")
    
    def load_model(self, filepath: str) -> None:
        model_data = joblib.load(filepath)
        self.model = model_data['model']
        self.feature_columns = model_data['feature_columns']
        self.model_name = model_data['model_name']
        self.is_trained = model_data['is_trained']
        logger.info(f"Model loaded from {filepath}")


class LSTMTrafficModel(BaseTrafficModel):
    """LSTM neural network model for traffic forecasting"""
    
    def __init__(self, sequence_length: int = 24, n_features: int = 1):
        super().__init__("LSTM_Neural_Network")
        self.sequence_length = sequence_length
        self.n_features = n_features
        self.model = None
    
    def _build_model(self, input_shape: Tuple[int, int]) -> Sequential:
        """Build LSTM model architecture"""
        model = Sequential([
            LSTM(50, return_sequences=True, input_shape=input_shape),
            Dropout(0.2),
            LSTM(50, return_sequences=True),
            Dropout(0.2),
            LSTM(50),
            Dropout(0.2),
            Dense(25),
            Dense(1)
        ])
        
        model.compile(
            optimizer=Adam(learning_rate=0.001),
            loss='mse',
            metrics=['mae']
        )
        
        return model
    
    def _create_sequences(self, data: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Create sequences for LSTM training"""
        X, y = [], []
        
        for i in range(len(data) - self.sequence_length):
            X.append(data[i:(i + self.sequence_length)])
            y.append(data[i + self.sequence_length])
        
        return np.array(X), np.array(y)
    
    def train(self, X: pd.DataFrame, y: pd.Series) -> None:
        logger.info("Training LSTM Neural Network")
        
        self.feature_columns = X.columns.tolist()
        
        # Combine features and target for sequence creation
        data = X.values
        target = y.values
        
        # Create sequences
        X_seq, y_seq = self._create_sequences(data)
        
        # Build model
        input_shape = (self.sequence_length, X.shape[1])
        self.model = self._build_model(input_shape)
        
        # Callbacks
        early_stopping = EarlyStopping(
            monitor='val_loss',
            patience=10,
            restore_best_weights=True
        )
        
        reduce_lr = ReduceLROnPlateau(
            monitor='val_loss',
            factor=0.2,
            patience=5,
            min_lr=0.0001
        )
        
        # Train model
        history = self.model.fit(
            X_seq, y_seq,
            epochs=100,
            batch_size=32,
            validation_split=0.2,
            callbacks=[early_stopping, reduce_lr],
            verbose=1
        )
        
        self.is_trained = True
        logger.info("LSTM Neural Network training completed")
        
        return history
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if not self.is_trained:
            raise ValueError("Model must be trained before prediction")
        
        data = X.values
        X_seq, _ = self._create_sequences(data)
        
        predictions = self.model.predict(X_seq)
        return predictions.flatten()
    
    def save_model(self, filepath: str) -> None:
        model_data = {
            'model': self.model,
            'feature_columns': self.feature_columns,
            'model_name': self.model_name,
            'is_trained': self.is_trained,
            'sequence_length': self.sequence_length
        }
        
        # Save Keras model and additional data
        self.model.save(f"{filepath}_keras")
        joblib.dump(model_data, f"{filepath}_metadata")
        logger.info(f"LSTM model saved to {filepath}")
    
    def load_model(self, filepath: str) -> None:
        # Load Keras model and metadata
        self.model = tf.keras.models.load_model(f"{filepath}_keras")
        model_data = joblib.load(f"{filepath}_metadata")
        
        self.feature_columns = model_data['feature_columns']
        self.model_name = model_data['model_name']
        self.is_trained = model_data['is_trained']
        self.sequence_length = model_data['sequence_length']
        
        logger.info(f"LSTM model loaded from {filepath}")


class TrafficModelTrainer:
    """Orchestrates training and evaluation of multiple traffic models"""
    
    def __init__(self):
        self.models = {}
        self.evaluation_results = {}
    
    def add_model(self, name: str, model: BaseTrafficModel) -> None:
        """Add a model to the trainer"""
        self.models[name] = model
        logger.info(f"Added model: {name}")
    
    def train_all_models(self, X: pd.DataFrame, y: pd.Series, 
                        test_size: float = 0.2) -> Dict[str, Dict]:
        """Train all models and evaluate them"""
        logger.info("Starting training for all models")
        
        # Split data with time series consideration
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42
        )
        
        results = {}
        
        for name, model in self.models.items():
            try:
                logger.info(f"Training {name}")
                
                # Train model
                model.train(X_train, y_train)
                
                # Evaluate model
                train_metrics = model.evaluate(X_train, y_train)
                test_metrics = model.evaluate(X_test, y_test)
                
                results[name] = {
                    'train_metrics': train_metrics,
                    'test_metrics': test_metrics,
                    'model': model
                }
                
                logger.info(f"{name} - Test RMSE: {test_metrics['rmse']:.2f}")
                
            except Exception as e:
                logger.error(f"Error training {name}: {e}")
                results[name] = {'error': str(e)}
        
        self.evaluation_results = results
        return results
    
    def get_best_model(self, metric: str = 'rmse') -> Tuple[str, BaseTrafficModel]:
        """Get the best performing model based on specified metric"""
        best_model_name = None
        best_score = float('inf') if metric in ['mae', 'mse', 'rmse', 'mape'] else float('-inf')
        
        for name, result in self.evaluation_results.items():
            if 'error' in result:
                continue
            
            score = result['test_metrics'].get(metric)
            if score is not None:
                if metric in ['mae', 'mse', 'rmse', 'mape']:
                    if score < best_score:
                        best_score = score
                        best_model_name = name
                else:
                    if score > best_score:
                        best_score = score
                        best_model_name = name
        
        if best_model_name:
            return best_model_name, self.models[best_model_name]
        else:
            raise ValueError("No valid models found")
    
    def save_all_models(self, directory: str) -> None:
        """Save all trained models"""
        import os
        os.makedirs(directory, exist_ok=True)
        
        for name, result in self.evaluation_results.items():
            if 'error' not in result:
                model = result['model']
                filepath = os.path.join(directory, f"{name}.pkl")
                model.save_model(filepath)
        
        logger.info(f"All models saved to {directory}")
    
    def predict_with_ensemble(self, X: pd.DataFrame, 
                           weights: Optional[Dict[str, float]] = None) -> np.ndarray:
        """Make ensemble predictions from all trained models"""
        predictions = []
        model_weights = []
        
        for name, result in self.evaluation_results.items():
            if 'error' not in result:
                model = result['model']
                pred = model.predict(X)
                predictions.append(pred)
                
                # Use inverse RMSE as weight if not provided
                if weights and name in weights:
                    model_weights.append(weights[name])
                else:
                    rmse = result['test_metrics']['rmse']
                    model_weights.append(1.0 / rmse)
        
        if not predictions:
            raise ValueError("No trained models available for prediction")
        
        # Weighted average
        model_weights = np.array(model_weights)
        model_weights = model_weights / model_weights.sum()
        
        ensemble_pred = np.average(predictions, axis=0, weights=model_weights)
        return ensemble_pred
