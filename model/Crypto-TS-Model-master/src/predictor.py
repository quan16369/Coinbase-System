import torch
import yaml
import os
import numpy as np
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import time

# Import model architectures
from lstm_model import LSTMModel
from lstm_attention_model import LSTMAttentionModel
from cnn_lstm_model import CNNLSTMModel
from cnn_lstm_attention_model import CNNLSTMAttentionModel
from lstm_attention_hybrid_model import LSTMAttentionHybrid
from optimize_model import OptimizedLSTMAttentionModel

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("CryptoPredictor")

class CryptoPredictor:
    """Handles loading and running predictions with pre-trained crypto time series models"""
    
    def __init__(self, config_path: str, checkpoint_path: str = None, device: str = None):
        """
        Initialize the predictor with configuration
        
        Args:
            config_path: Path to YAML configuration file
            checkpoint_path: Path to model checkpoint file
            device: 'cpu' or 'cuda' (if None, will determine automatically)
        """
        self.config_path = config_path
        self.checkpoint_path = checkpoint_path
        
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Determine device
        if device is None:
            self.device = 'cuda' if torch.cuda.is_available() else 'cpu'
        else:
            self.device = device
            
        logger.info(f"Using device: {self.device}")
        
        # Load model architecture based on config
        self.model = self._load_model_architecture()
        
        # Load model weights if checkpoint provided
        if checkpoint_path:
            self._load_checkpoint()
            logger.info(f"Model loaded from checkpoint: {checkpoint_path}")
        
        # Move model to correct device
        self.model = self.model.to(self.device)
        self.model.eval()  # Set to evaluation mode
        
        # Get model input requirements
        self.seq_len = self.config['model']['seq_len']
        self.pred_len = self.config['model']['pred_len']
        self.input_dim = self.config['model']['enc_in']
    
    def _load_model_architecture(self):
        """
        Load the model architecture based on configuration
        
        Returns:
            Model instance
        """
        model_type = self.config['model'].get('model_type', 'lstm').lower()
        
        logger.info(f"Loading model architecture: {model_type}")
        
        if model_type == 'lstm':
            return LSTMModel(self.config)
        elif model_type == 'lstm_attention':
            return LSTMAttentionModel(self.config)
        elif model_type == 'cnn_lstm':
            return CNNLSTMModel(self.config)
        elif model_type == 'cnn_lstm_attention':
            return CNNLSTMAttentionModel(self.config)
        elif model_type == 'lstm_hybridattention':
            return LSTMAttentionHybrid(self.config)
        elif model_type == 'optimize':
            return OptimizedLSTMAttentionModel(self.config)
        else:
            raise ValueError(f"Unsupported model type: {model_type}")
    
    def _load_checkpoint(self):
        """Load model weights from checkpoint"""
        if not os.path.exists(self.checkpoint_path):
            raise FileNotFoundError(f"Checkpoint not found: {self.checkpoint_path}")
        
        # Load checkpoint
        checkpoint = torch.load(self.checkpoint_path, map_location=self.device)
        
        # Load model state dict
        if 'model_state_dict' in checkpoint:
            self.model.load_state_dict(checkpoint['model_state_dict'])
        else:
            # Direct state dict
            self.model.load_state_dict(checkpoint)
            
        logger.info(f"Model weights loaded from {self.checkpoint_path}")
        
    def predict(self, input_data: torch.Tensor) -> torch.Tensor:
        """
        Run prediction with the model
        
        Args:
            input_data: Input tensor of shape [batch_size, seq_len, input_dim]
            
        Returns:
            Prediction tensor of shape [batch_size, pred_len, 1]
        """
        with torch.no_grad():
            input_data = input_data.to(self.device)
            predictions = self.model(input_data)
            return predictions.cpu()
            
    def predict_next_steps(self, input_sequence: np.ndarray) -> np.ndarray:
        """
        Predict the next steps using the last sequence data
        
        Args:
            input_sequence: Input features of shape [seq_len, input_dim]
            
        Returns:
            Predictions array of shape [pred_len, 1]
        """
        # Ensure input has correct dimensions
        if len(input_sequence.shape) == 2:  # [seq_len, features]
            if input_sequence.shape[0] < self.seq_len:
                raise ValueError(f"Input sequence length ({input_sequence.shape[0]}) is less than required ({self.seq_len})")
            
            # Take the last seq_len points
            input_sequence = input_sequence[-self.seq_len:]
            
            # Add batch dimension
            input_tensor = torch.from_numpy(input_sequence).float().unsqueeze(0)  # [1, seq_len, features]
        else:
            raise ValueError(f"Expected input of shape [seq_len, features], got {input_sequence.shape}")
        
        # Run prediction
        with torch.no_grad():
            predictions = self.predict(input_tensor)
            
        # Convert to numpy
        return predictions.squeeze(0).numpy()  # [pred_len, 1]
    
    def scale_predictions(self, predictions: np.ndarray, scaler):
        """
        Scale predictions back to original space using inverse transform
        
        Args:
            predictions: Scaled predictions
            scaler: Scaler object used for transformation
            
        Returns:
            Unscaled predictions
        """
        # Reshape for inverse transform if needed
        return scaler.inverse_transform(predictions)
        
    def predict_multi_coin(self, coin_data: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        """
        Predict for multiple coins
        
        Args:
            coin_data: Dictionary of coin_id -> input_sequence array
            
        Returns:
            Dictionary of coin_id -> predictions array
        """
        results = {}
        for coin_id, sequence in coin_data.items():
            try:
                predictions = self.predict_next_steps(sequence)
                results[coin_id] = predictions
                logger.info(f"Successfully predicted for {coin_id} - shape: {predictions.shape}")
            except Exception as e:
                logger.error(f"Error predicting for {coin_id}: {str(e)}")
        
        return results

    @staticmethod
    def calculate_prediction_metrics(predictions: np.ndarray, actuals: np.ndarray) -> Dict[str, float]:
        """
        Calculate prediction error metrics
        
        Args:
            predictions: Predicted values
            actuals: Actual values
            
        Returns:
            Dictionary of metric names to values
        """
        if len(predictions) != len(actuals):
            raise ValueError("Predictions and actuals must have the same length")
            
        # Calculate metrics
        mae = np.mean(np.abs(predictions - actuals))
        mse = np.mean(np.square(predictions - actuals))
        rmse = np.sqrt(mse)
        
        # Calculate directional accuracy
        pred_dir = np.sign(predictions[1:] - predictions[:-1])
        actual_dir = np.sign(actuals[1:] - actuals[:-1])
        dir_acc = np.mean(pred_dir == actual_dir)
        
        return {
            "mae": float(mae),
            "mse": float(mse),
            "rmse": float(rmse),
            "dir_acc": float(dir_acc)
        }
        
    def save_to_csv(self, predictions: Dict[str, np.ndarray], output_path: str):
        """
        Save predictions to CSV file
        
        Args:
            predictions: Dictionary of coin_id -> predictions array
            output_path: Path to output file
        """
        data = []
        timestamp = datetime.now()
        
        for coin_id, prediction_array in predictions.items():
            for i, pred_val in enumerate(prediction_array):
                # Calculate prediction time (5-min intervals)
                pred_time = timestamp + timedelta(minutes=(i+1)*5)
                
                # Append to data
                value = float(pred_val[0]) if pred_val.ndim > 0 else float(pred_val)
                data.append({
                    'product_id': coin_id,
                    'timestamp': timestamp.isoformat(),
                    'prediction_time': pred_time.isoformat(),
                    'horizon_steps': i+1,
                    'predicted_value': value
                })
        
        # Create DataFrame and save to CSV
        if data:
            import pandas as pd
            df = pd.DataFrame(data)
            df.to_csv(output_path, index=False)
            logger.info(f"Predictions saved to {output_path}")
        else:
            logger.warning("No predictions to save")
    
    def load_from_csv(self, csv_path: str) -> Dict[str, np.ndarray]:
        """
        Load predictions from CSV file
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            Dictionary of coin_id -> predictions array
        """
        import pandas as pd
        
        try:
            df = pd.read_csv(csv_path)
            result = {}
            
            for product_id in df['product_id'].unique():
                product_df = df[df['product_id'] == product_id].sort_values('horizon_steps')
                predictions = product_df['predicted_value'].values.reshape(-1, 1)
                result[product_id] = predictions
                
            logger.info(f"Loaded predictions for {len(result)} products from {csv_path}")
            return result
        except Exception as e:
            logger.error(f"Error loading predictions from CSV: {e}")
            return {}