import os
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Any
from kafka import KafkaProducer
import yfinance as yf
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockDataProducer:
    """
    Stock data producer that fetches real-time stock data from Yahoo Finance
    and publishes it to a Kafka topic for stream processing.
    """
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092', topic: str = 'stock-data'):
        """
        Initialize the Kafka producer with connection parameters.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses
            topic: Kafka topic name for publishing stock data
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self._initialize_producer()
    
    def _initialize_producer(self):
        """Initialize Kafka producer with appropriate configuration."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(','),
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,   # Retry failed sends
                batch_size=16384,  # Batch size in bytes
                linger_ms=10,      # Wait time for batching
                buffer_memory=33554432  # Total memory for buffering
            )
            logger.info(f"Kafka producer initialized successfully for {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def fetch_stock_data(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch current stock data for given symbols using yfinance.
        
        Args:
            symbols: List of stock symbols to fetch data for
            
        Returns:
            List of dictionaries containing stock data
        """
        stock_data = []
        
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                # Get intraday data with 1-minute intervals
                data = ticker.history(period="1d", interval="1m")
                
                if not data.empty:
                    # Get the latest data point
                    latest = data.iloc[-1]
                    
                    stock_record = {
                        'symbol': symbol,
                        'timestamp': datetime.now().isoformat(),
                        'open': float(latest['Open']),
                        'high': float(latest['High']),
                        'low': float(latest['Low']),
                        'close': float(latest['Close']),
                        'volume': int(latest['Volume']),
                        'fetch_time': datetime.now().isoformat()
                    }
                    
                    stock_data.append(stock_record)
                    logger.info(f"Fetched data for {symbol}: ${latest['Close']:.2f}")
                    
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {e}")
                continue
        
        return stock_data
    
    def publish_data(self, data: List[Dict[str, Any]]):
        """
        Publish stock data to Kafka topic.
        
        Args:
            data: List of stock data dictionaries to publish
        """
        for record in data:
            try:
                # Use stock symbol as the key for partitioning
                key = record['symbol']
                
                # Send the record to Kafka
                future = self.producer.send(
                    topic=self.topic,
                    key=key,
                    value=record
                )
                
                # Optional: Wait for confirmation (blocks execution)
                # record_metadata = future.get(timeout=10)
                # logger.info(f"Record sent to {record_metadata.topic} partition {record_metadata.partition}")
                
            except Exception as e:
                logger.error(f"Error publishing data for {record['symbol']}: {e}")
    
    def run_continuous(self, symbols: List[str], interval_seconds: int = 60):
        """
        Run the producer continuously, fetching and publishing data at intervals.
        
        Args:
            symbols: List of stock symbols to monitor
            interval_seconds: Seconds between data fetches
        """
        logger.info(f"Starting continuous production for symbols: {symbols}")
        logger.info(f"Publishing to topic: {self.topic}")
        logger.info(f"Interval: {interval_seconds} seconds")
        
        try:
            while True:
                start_time = time.time()
                
                # Fetch and publish data
                data = self.fetch_stock_data(symbols)
                if data:
                    self.publish_data(data)
                    logger.info(f"Published {len(data)} records to Kafka")
                
                # Ensure we maintain the specified interval
                elapsed = time.time() - start_time
                sleep_time = max(0, interval_seconds - elapsed)
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Shutting down producer...")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            if self.producer:
                self.producer.close()
                logger.info("Producer closed successfully")

def main():
    """Main function to run the stock data producer."""
    # Get configuration from environment variables
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic = os.getenv('KAFKA_TOPIC', 'stock-data')
    symbols = os.getenv('STOCK_SYMBOLS', 'AAPL,GOOGL,MSFT').split(',')
    
    # Initialize and run producer
    producer = StockDataProducer(bootstrap_servers, topic)
    producer.run_continuous(symbols, interval_seconds=30)

if __name__ == "__main__":
    main()
