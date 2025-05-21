import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from typing import Dict, List, Optional, Tuple
from sklearn.preprocessing import RobustScaler, MinMaxScaler
import ta
aaaa
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("CassandraLoader")

class CassandraLoader:
    """
    Class for loading and processing crypto data from Cassandra
    """
    
    def __init__(self, 
                 hosts: List[str] = ['cassandra'], 
                 port: int = 9042,
                 username: Optional[str] = None, 
                 password: Optional[str] = None,
                 keyspace: str = 'coinbase'):
        """
        Initialize connection to Cassandra
        
        Args:
            hosts: List of Cassandra host addresses
            port: Cassandra port
            username: Cassandra username (if auth enabled)
            password: Cassandra password (if auth enabled)
            keyspace: Cassandra keyspace to use
        """
        self.hosts = hosts
        self.port = port
        self.keyspace = keyspace
        
        # Connect to Cassandra
        if username and password:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            self.cluster = Cluster(hosts, port=port, auth_provider=auth_provider)
        else:
            self.cluster = Cluster(hosts, port=port)
            
        self.session = self.cluster.connect(keyspace)
        logger.info(f"Connected to Cassandra: {', '.join(hosts)}:{port} (keyspace: {keyspace})")
        
        # Initialize scalers
        self.scalers = {
            'price': RobustScaler(),
            'volume': RobustScaler(),
            'indicators': MinMaxScaler(feature_range=(-1, 1)),
            'time': MinMaxScaler(feature_range=(0, 1))
        }
        
        # Flag to track if scalers are fitted
        self.fitted = False
    
    def load_price_data(self, 
                      product_ids: List[str], 
                      start_time: Optional[datetime] = None,
                      end_time: Optional[datetime] = None,
                      limit: int = 10000) -> Dict[str, pd.DataFrame]:
        """
        Load price data from Cassandra
        
        Args:
            product_ids: List of product IDs to retrieve
            start_time: Start timestamp (None for no limit)
            end_time: End timestamp (None for current time)
            limit: Maximum number of records per product
            
        Returns:
            Dictionary of product_id -> DataFrame with price data
        """
        if end_time is None:
            end_time = datetime.now()
            
        if start_time is None:
            # Default to 24 hours before end time
            start_time = end_time - timedelta(hours=24)
            
        logger.info(f"Loading price data for {len(product_ids)} products from {start_time} to {end_time}")
        
        result = {}
        
        for product_id in product_ids:
            # Prepare query
            query = f"""
                SELECT product_id, time, price 
                FROM prices 
                WHERE product_id = %s AND time >= %s AND time <= %s
                LIMIT {limit}
            """
            
            # Execute query
            rows = self.session.execute(query, (product_id, start_time, end_time))
            
            # Convert to DataFrame
            df = pd.DataFrame(list(rows))
            
            if not df.empty:
                # Sort by time
                df = df.sort_values('time')
                df.set_index('time', inplace=True)
                logger.info(f"Loaded {len(df)} price points for {product_id}")
                result[product_id] = df
            else:
                logger.warning(f"No data found for {product_id}")
                
        return result
        
    def load_candle_data(self, 
                        product_ids: List[str], 
                        start_time: Optional[datetime] = None,
                        end_time: Optional[datetime] = None,
                        limit: int = 10000) -> Dict[str, pd.DataFrame]:
        """
        Load candle data from Cassandra
        
        Args:
            product_ids: List of product IDs to retrieve
            start_time: Start timestamp (None for no limit)
            end_time: End timestamp (None for current time)
            limit: Maximum number of records per product
            
        Returns:
            Dictionary of product_id -> DataFrame with candle data
        """
        if end_time is None:
            end_time = datetime.now()
            
        if start_time is None:
            # Default to 24 hours before end time
            start_time = end_time - timedelta(hours=24)
            
        logger.info(f"Loading candle data for {len(product_ids)} products from {start_time} to {end_time}")
        
        result = {}
        
        for product_id in product_ids:
            # Prepare query
            query = f"""
                SELECT product_id, start_time, end_time, open, high, low, close, volume
                FROM candles 
                WHERE product_id = %s AND start_time >= %s AND start_time <= %s
                LIMIT {limit}
            """
            
            # Execute query
            rows = self.session.execute(query, (product_id, start_time, end_time))
            
            # Convert to DataFrame
            df = pd.DataFrame(list(rows))
            
            if not df.empty:
                # Sort by time
                df = df.sort_values('start_time')
                df.set_index('start_time', inplace=True)
                logger.info(f"Loaded {len(df)} candles for {product_id}")
                result[product_id] = df
            else:
                logger.warning(f"No candle data found for {product_id}")
                
        return result
    
    def prepare_model_features(self, 
                           price_data: Dict[str, pd.DataFrame],
                           candle_data: Optional[Dict[str, pd.DataFrame]] = None,
                           sequence_length: int = 288,
                           resample_freq: str = '5T') -> Dict[str, np.ndarray]:
        """
        Prepare feature data for model prediction
        
        Args:
            price_data: Dictionary of product_id -> price DataFrame
            candle_data: Optional dictionary of product_id -> candle DataFrame
            sequence_length: Required sequence length for model
            resample_freq: Frequency for resampling data
            
        Returns:
            Dictionary of product_id -> feature array
        """
        result = {}
        
        for product_id, price_df in price_data.items():
            try:
                # Resample to regular intervals if needed
                if len(price_df) > 0:
                    # Get candle data if available
                    candle_df = None
                    if candle_data and product_id in candle_data:
                        candle_df = candle_data[product_id]
                    
                    # Process and create features
                    feature_df = self._enhance_crypto_features(price_df, candle_df, resample_freq)
                    
                    # Scale features
                    feature_array = self._scale_features(feature_df, fit=not self.fitted)
                    
                    # Set fitted flag after first scaling
                    if not self.fitted:
                        self.fitted = True
                    
                    # Ensure we have enough data for the sequence
                    if len(feature_array) >= sequence_length:
                        # Take the last sequence_length points
                        feature_array = feature_array[-sequence_length:]
                        result[product_id] = feature_array
                        logger.info(f"Prepared features for {product_id}: shape {feature_array.shape}")
                    else:
                        logger.warning(f"Not enough data for {product_id}: {len(feature_array)} < {sequence_length}")
                else:
                    logger.warning(f"Empty price data for {product_id}")
            except Exception as e:
                logger.error(f"Error preparing features for {product_id}: {str(e)}")
                
        return result
        
    def _enhance_crypto_features(self, 
                               price_df: pd.DataFrame, 
                               candle_df: Optional[pd.DataFrame] = None,
                               freq: str = '5T') -> pd.DataFrame:
        """
        Enhance raw price data with technical indicators and features
        
        Args:
            price_df: DataFrame with price data
            candle_df: Optional DataFrame with candle data
            freq: Frequency for resampling
            
        Returns:
            DataFrame with enhanced features
        """
        # Create a new DataFrame with regular time intervals
        # This handles irregular time series data
        df = price_df.copy()
        
        # If we have candle data, merge it
        if candle_df is not None and not candle_df.empty:
            # Keep only necessary columns from candle data
            candle_df = candle_df[['open', 'high', 'low', 'close', 'volume']]
            
            # Merge candle data with price data
            df = df.join(candle_df, how='left')
            
            # Fill NaN values
            df['open'] = df['open'].fillna(df['price'])
            df['high'] = df['high'].fillna(df['price'])
            df['low'] = df['low'].fillna(df['price'])
            df['close'] = df['close'].fillna(df['price'])
            df['volume'] = df['volume'].fillna(df['volume'].median())
        else:
            # Create OHLC from price data only
            df['open'] = df['price']
            df['high'] = df['price']
            df['low'] = df['price']
            df['close'] = df['price']
            df['volume'] = 0  # No volume data available
        
        # Resample if needed to ensure regular intervals
        if freq:
            # Resample price data
            ohlc_dict = {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'price': 'last',
                'volume': 'sum'
            }
            df = df.resample(freq).apply(ohlc_dict).ffill()
        
        # Calculate additional features
        # 1. Returns and log returns
        df['returns'] = df['price'].pct_change()
        df['log_returns'] = np.log1p(df['returns'])
        
        # 2. Price ratios
        df['price_ma_ratio'] = df['price'] / df['price'].rolling(24, min_periods=1).mean()
        df['price_spread'] = (df['high'] - df['low']) / df['price']
        
        # 3. Volume features (if available)
        if 'volume' in df.columns and df['volume'].sum() > 0:
            df['volume_zscore'] = (df['volume'] - df['volume'].rolling(24, min_periods=1).mean()) / df['volume'].rolling(24, min_periods=1).std()
            df['volume_ma_ratio'] = df['volume'] / df['volume'].rolling(24, min_periods=1).mean()
            df['liquidity'] = np.log1p(df['volume'] * df['price'])
        else:
            # Dummy values if no volume data
            df['volume_zscore'] = 0
            df['volume_ma_ratio'] = 1
            df['liquidity'] = np.log1p(df['price'])
        
        # 4. Technical indicators
        df['rsi'] = ta.momentum.RSIIndicator(df['price'], window=14).rsi()
        df['macd'] = ta.trend.MACD(df['price'], window_slow=26, window_fast=12).macd_diff()
        df['atr'] = ta.volatility.AverageTrueRange(df['high'], df['low'], df['price'], window=14).average_true_range()
        
        # 5. Volatility features
        for window in [6, 12, 24]:
            df[f'volatility_{window}'] = df['log_returns'].rolling(window, min_periods=1).std()
        
        # 6. Momentum features
        df['momentum_3_6'] = df['price'].rolling(3, min_periods=1).mean() - df['price'].rolling(6, min_periods=1).mean()
        df['momentum_6_12'] = df['price'].rolling(6, min_periods=1).mean() - df['price'].rolling(12, min_periods=1).mean()
        
        # 7. Time features
        df['hour'] = df.index.hour
        df['minute'] = df.index.minute
        df['dayofweek'] = df.index.dayofweek
        
        # 8. Cyclical encoding of time
        df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        df['dow_sin'] = np.sin(2 * np.pi * df['dayofweek'] / 7)
        df['dow_cos'] = np.cos(2 * np.pi * df['dayofweek'] / 7)
        
        # Fill NaN values
        df = df.fillna(method='ffill').fillna(method='bfill').fillna(0)
        
        return df
        
    def _scale_features(self, df: pd.DataFrame, fit: bool = True) -> np.ndarray:
        """
        Scale features using stored scalers
        
        Args:
            df: DataFrame with features
            fit: Whether to fit scalers on this data
            
        Returns:
            Scaled numpy array
        """
        # Define feature groups
        price_cols = ['open', 'high', 'low', 'close', 'price', 'price_ma_ratio', 'price_spread']
        volume_cols = ['volume', 'volume_zscore', 'volume_ma_ratio', 'liquidity']
        indicator_cols = ['rsi', 'macd', 'atr', 'log_returns'] + \
                        [f'volatility_{w}' for w in [6, 12, 24]] + \
                        ['momentum_3_6', 'momentum_6_12']
        time_cols = ['hour_sin', 'hour_cos', 'dow_sin', 'dow_cos']
        
        # Get available columns (some might be missing)
        available_price_cols = [col for col in price_cols if col in df.columns]
        available_volume_cols = [col for col in volume_cols if col in df.columns]
        available_indicator_cols = [col for col in indicator_cols if col in df.columns]
        available_time_cols = [col for col in time_cols if col in df.columns]
        
        # Create a new DataFrame for scaled data
        scaled_data = pd.DataFrame(index=df.index)
        
        # Fit and transform or just transform
        if fit:
            if available_price_cols:
                scaled_data[available_price_cols] = self.scalers['price'].fit_transform(df[available_price_cols])
            if available_volume_cols:
                scaled_data[available_volume_cols] = self.scalers['volume'].fit_transform(df[available_volume_cols])
            if available_indicator_cols:
                scaled_data[available_indicator_cols] = self.scalers['indicators'].fit_transform(df[available_indicator_cols])
            if available_time_cols:
                scaled_data[available_time_cols] = self.scalers['time'].fit_transform(df[available_time_cols])
        else:
            if available_price_cols:
                scaled_data[available_price_cols] = self.scalers['price'].transform(df[available_price_cols])
            if available_volume_cols:
                scaled_data[available_volume_cols] = self.scalers['volume'].transform(df[available_volume_cols])
            if available_indicator_cols:
                scaled_data[available_indicator_cols] = self.scalers['indicators'].transform(df[available_indicator_cols])
            if available_time_cols:
                scaled_data[available_time_cols] = self.scalers['time'].transform(df[available_time_cols])
        
        # Convert to numpy array
        return scaled_data.values
    
    def get_price_scaler(self):
        """Get the price scaler for inverse transformation"""
        return self.scalers['price']
    
    def save_prediction(self, 
                       product_id: str, 
                       predictions: np.ndarray, 
                       timestamp: Optional[datetime] = None) -> bool:
        """
        Save predictions to Cassandra
        
        Args:
            product_id: Product ID
            predictions: Predictions array
            timestamp: Timestamp for predictions (defaults to current time)
            
        Returns:
            Success flag
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        try:
            # Create predictions table if not exists
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS predictions (
                    product_id TEXT,
                    timestamp TIMESTAMP,
                    prediction_horizon INT,
                    predicted_price DOUBLE,
                    PRIMARY KEY (product_id, timestamp, prediction_horizon)
                )
            """)
            
            # Create predictions_by_horizon table if not exists
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS predictions_by_horizon (
                    product_id TEXT,
                    prediction_horizon INT,
                    timestamp TIMESTAMP,
                    predicted_price DOUBLE,
                    PRIMARY KEY ((product_id, prediction_horizon), timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC)
            """)
            
            # Insert predictions
            for i, pred in enumerate(predictions):
                # Calculate prediction horizon timestamp
                horizon_min = (i + 1) * 5  # Assuming 5-minute intervals
                horizon_timestamp = timestamp + timedelta(minutes=horizon_min)
                predicted_value = float(pred[0]) if pred.ndim > 0 else float(pred)
                
                # Insert into predictions table
                query1 = """
                    INSERT INTO predictions (product_id, timestamp, prediction_horizon, predicted_price)
                    VALUES (%s, %s, %s, %s)
                """
                self.session.execute(query1, (product_id, timestamp, i+1, predicted_value))
                
                # Insert into predictions_by_horizon table
                query2 = """
                    INSERT INTO predictions_by_horizon (product_id, prediction_horizon, timestamp, predicted_price)
                    VALUES (%s, %s, %s, %s)
                """
                self.session.execute(query2, (product_id, i+1, timestamp, predicted_value))
                
            logger.info(f"Saved {len(predictions)} predictions for {product_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving predictions for {product_id}: {str(e)}")
            return False
    
    def close(self):
        """Close connection to Cassandra"""
        if hasattr(self, 'session') and self.session:
            self.session.close()
        if hasattr(self, 'cluster') and self.cluster:
            self.cluster.shutdown()
        logger.info("Cassandra connection closed")