import os
import sys
import time
import logging
import threading
import schedule
import signal
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# Add model source directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'model', 'Crypto-TS-Model-master', 'src'))

# Import modules from model directory
from predictor import CryptoPredictor
from cassandra_loader import CassandraLoader

# Configure logging for Docker environment
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)  # Output to stdout for Docker logs
    ]
)
logger = logging.getLogger("PredictionService")

# Custom exceptions
class PredictionServiceError(Exception):
    """Base class for prediction service errors"""
    pass

class ModelLoadError(PredictionServiceError):
    """Error loading model"""
    pass

class CassandraConnectionError(PredictionServiceError):
    """Error connecting to Cassandra"""
    pass

class PredictionService:
    """Service for real-time crypto price prediction"""
    
    def __init__(self, config_path: str, checkpoint_path: str):
        """
        Initialize the prediction service
        
        Args:
            config_path: Path to model configuration YAML
            checkpoint_path: Path to model checkpoint
        """
        # Verify file existence - important in Docker environment
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        if not os.path.exists(checkpoint_path):
            raise FileNotFoundError(f"Checkpoint file not found: {checkpoint_path}")
        
        self.config_path = config_path
        self.checkpoint_path = checkpoint_path
        
        # Get settings from environment variables - Docker ready
        self.cassandra_hosts = os.getenv('CASSANDRA_HOSTS', 'cassandra').split(',')
        self.cassandra_port = int(os.getenv('CASSANDRA_PORT', '9042'))
        self.cassandra_user = os.getenv('CASSANDRA_USER', None)
        self.cassandra_password = os.getenv('CASSANDRA_PASSWORD', None)
        self.cassandra_keyspace = os.getenv('CASSANDRA_KEYSPACE', 'coinbase')
        
        # List of cryptocurrencies to predict
        self.product_ids = os.getenv('PRODUCT_IDS', 'BTC-USD,ETH-USD,XRP-USD').split(',')
        
        # Prediction interval (minutes)
        self.prediction_interval = int(os.getenv('PREDICTION_INTERVAL', '5'))
        
        # Additional Docker-friendly environment variables
        self.retry_delay = int(os.getenv('RETRY_DELAY', '30'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '10'))
        
        # Initialize predictor
        try:
            logger.info(f"Initializing predictor with config: {config_path}, checkpoint: {checkpoint_path}")
            self.predictor = CryptoPredictor(config_path, checkpoint_path)
        except Exception as e:
            logger.error(f"Failed to initialize predictor: {str(e)}")
            raise ModelLoadError(f"Failed to initialize predictor: {str(e)}")
        
        # Initialize data loader with retry mechanism
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                logger.info(f"Connecting to Cassandra: {self.cassandra_hosts}:{self.cassandra_port} (keyspace: {self.cassandra_keyspace})")
                self.loader = CassandraLoader(
                    hosts=self.cassandra_hosts,
                    port=self.cassandra_port,
                    username=self.cassandra_user,
                    password=self.cassandra_password,
                    keyspace=self.cassandra_keyspace
                )
                break
            except Exception as e:
                retry_count += 1
                if retry_count < self.max_retries:
                    logger.warning(f"Cassandra connection attempt {retry_count} failed: {str(e)}. Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to connect to Cassandra after {self.max_retries} attempts: {str(e)}")
                    raise CassandraConnectionError(f"Failed to connect to Cassandra after {self.max_retries} attempts: {str(e)}")
        
        # Get required sequence length from model
        self.seq_len = self.predictor.seq_len
        self.pred_len = self.predictor.pred_len
        
        logger.info(f"Prediction service initialized with sequence length: {self.seq_len}, prediction length: {self.pred_len}")
        logger.info(f"Will predict for: {', '.join(self.product_ids)}")
        
        # Set up signal handling for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, sig, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {sig}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def run_prediction_cycle(self):
        """Run a complete prediction cycle"""
        try:
            start_time = time.time()
            logger.info(f"Starting prediction cycle at {datetime.now().isoformat()}")
            
            # 1. Load recent price data
            end_time = datetime.now()
            # Use 2x sequence length to ensure enough data after processing
            start_time_data = end_time - timedelta(minutes=self.seq_len*5*2)
            
            price_data = self.loader.load_price_data(
                product_ids=self.product_ids,
                start_time=start_time_data,
                end_time=end_time
            )
            
            # 2. Load candle data if available
            candle_data = self.loader.load_candle_data(
                product_ids=self.product_ids,
                start_time=start_time_data,
                end_time=end_time
            )
            
            # 3. Prepare features
            feature_data = self.loader.prepare_model_features(
                price_data=price_data,
                candle_data=candle_data,
                sequence_length=self.seq_len
            )
            
            # 4. Generate predictions
            predictions = self.predictor.predict_multi_coin(feature_data)
            
            # 5. Save predictions to Cassandra
            timestamp = datetime.now()
            for product_id, prediction in predictions.items():
                # Get price scaler for inverse transform
                price_scaler = self.loader.get_price_scaler()
                
                # Create a dummy array with the same shape as what the scaler expects
                # Assuming the predicted output represents the 'price' column
                price_index = 0  # Index of 'price' in your feature array
                
                # Create a dummy array with the same shape as what the scaler expects
                dummy = np.zeros((prediction.shape[0], price_scaler.n_features_in_))
                dummy[:, price_index] = prediction.flatten()
                
                # Inverse transform
                unscaled_predictions = price_scaler.inverse_transform(dummy)[:, price_index].reshape(-1, 1)
                
                # Save predictions
                self.loader.save_prediction(product_id, unscaled_predictions, timestamp)
            
            end_time = time.time()
            logger.info(f"Prediction cycle completed in {end_time - start_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error in prediction cycle: {str(e)}", exc_info=True)
    
    def schedule_predictions(self):
        """Schedule regular predictions"""
        # Run immediately once
        self.run_prediction_cycle()
        
        # Schedule regular predictions
        schedule.every(self.prediction_interval).minutes.do(self.run_prediction_cycle)
        
        logger.info(f"Scheduled predictions every {self.prediction_interval} minutes")
        
        # Run the scheduler in a loop
        while self.running:
            schedule.run_pending()
            time.sleep(1)
    
    def start(self):
        """Start the prediction service"""
        logger.info("Starting prediction service")
        self.running = True
        
        # Start scheduler in a separate thread
        self.scheduler_thread = threading.Thread(target=self.schedule_predictions)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()
        
        try:
            # Keep main thread alive
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt detected, shutting down...")
        finally:
            # Clean up
            self.stop()
    
    def stop(self):
        """Stop the prediction service"""
        logger.info("Stopping prediction service")
        self.running = False
        
        # Close Cassandra connection
        if hasattr(self, 'loader'):
            self.loader.close()
        
        logger.info("Prediction service stopped")


# Health check endpoint
def setup_health_check():
    """Set up a simple health check endpoint"""
    try:
        from flask import Flask
        app = Flask(__name__)
        
        @app.route('/health')
        def health_check():
            """Health check endpoint for Docker"""
            return {"status": "healthy"}
        
        # Run in a thread
        def run_health_check():
            app.run(host='0.0.0.0', port=8000)
        
        health_thread = threading.Thread(target=run_health_check)
        health_thread.daemon = True
        health_thread.start()
        logger.info("Health check endpoint started on port 8000")
    except ImportError:
        logger.warning("Flask not installed, health check endpoint disabled")


if __name__ == "__main__":
    # Get paths from environment variables
    config_path = os.getenv('MODEL_CONFIG_PATH', '/app/model/configs/train_config.yaml')
    checkpoint_path = os.getenv('MODEL_CHECKPOINT_PATH', '/app/model/checkpoints/best_epoch_16.pt')
    
    # Setup health check if enabled
    if os.getenv('ENABLE_HEALTH_CHECK', 'false').lower() == 'true':
        setup_health_check()
    
    # Create and start service
    service = PredictionService(config_path, checkpoint_path)
    service.start()