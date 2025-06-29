import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class StockStreamProcessor:
    """
    Spark Structured Streaming processor for real-time stock market analysis
    with volume and price action-based signal generation.
    """
    
    def __init__(self):
        """Initialize Spark session with required configurations."""
        self.spark = None
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'stock-data')
        self._initialize_spark()
    
    def _initialize_spark(self):
        """Initialize Spark session with Kafka integration packages."""
        packages = [
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
            'org.apache.kafka:kafka-clients:3.2.3'
        ]
        
        self.spark = SparkSession.builder \
            .appName("StockMarketRealTimeAnalytics") \
            .config("spark.jars.packages", ",".join(packages)) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print("Spark session initialized successfully")
    
    def create_stock_schema(self):
        """Define the schema for incoming stock data."""
        return StructType([
            StructField("symbol", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("fetch_time", StringType(), True)
        ])
    
    def read_kafka_stream(self):
        """Read streaming data from Kafka topic."""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
    
    def parse_stock_data(self, kafka_df):
        """Parse JSON messages from Kafka into structured DataFrame."""
        schema = self.create_stock_schema()
        
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Convert timestamp to proper format and add processing time
        return parsed_df.withColumn(
            "timestamp", 
            to_timestamp(col("timestamp"))
        ).withColumn(
            "processing_time",
            current_timestamp()
        )
    
    def calculate_price_action_metrics(self, stock_df):
        """Calculate price action and volume analysis metrics."""
        # Define window specifications for technical analysis
        symbol_window = Window.partitionBy("symbol").orderBy("timestamp")
        volume_window = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-19, 0)
        
        return stock_df.withColumn(
            # Previous candle data for comparison
            "prev_close", lag("close", 1).over(symbol_window)
        ).withColumn(
            "prev_high", lag("high", 1).over(symbol_window)
        ).withColumn(
            "prev_low", lag("low", 1).over(symbol_window)
        ).withColumn(
            "prev_volume", lag("volume", 1).over(symbol_window)
        ).withColumn(
            # Price action metrics
            "body_size", abs(col("close") - col("open"))
        ).withColumn(
            "upper_shadow", col("high") - greatest(col("open"), col("close"))
        ).withColumn(
            "lower_shadow", least(col("open"), col("close")) - col("low")
        ).withColumn(
            "price_change", col("close") - col("prev_close")
        ).withColumn(
            "price_change_pct", 
            when(col("prev_close").isNotNull() & (col("prev_close") != 0),
                 (col("close") - col("prev_close")) / col("prev_close") * 100
            ).otherwise(0)
        ).withColumn(
            # Volume analysis metrics
            "avg_volume_20", avg("volume").over(volume_window)
        ).withColumn(
            "volume_ratio", 
            when(col("avg_volume_20") > 0, 
                 col("volume") / col("avg_volume_20")
            ).otherwise(1)
        ).withColumn(
            "volume_spike", when(col("volume_ratio") > 1.5, True).otherwise(False)
        ).withColumn(
            "high_volume_up", 
            when((col("price_change") > 0) & (col("volume_ratio") > 1.5), True).otherwise(False)
        ).withColumn(
            "high_volume_down",
            when((col("price_change") < 0) & (col("volume_ratio") > 1.5), True).otherwise(False)
        )

    def run_streaming_pipeline(self):
        """Execute the complete streaming pipeline."""
        # Import signal generation functions
        from ..processing.signal_generator import generate_trading_signals, add_risk_management_metrics
        
        print("Starting real-time stock market analytics pipeline...")
        
        # Read streaming data from Kafka
        kafka_stream = self.read_kafka_stream()
        print("Connected to Kafka stream")
        
        # Parse and process the data
        stock_data = self.parse_stock_data(kafka_stream)
        print("Parsing stock data from Kafka messages")
        
        # Apply price action and volume analysis
        enriched_data = self.calculate_price_action_metrics(stock_data)
        print("Calculating price action and volume metrics")
        
        # Generate trading signals
        signals = generate_trading_signals(enriched_data)
        signals_with_risk = add_risk_management_metrics(signals)
        
        # Output signals to console for monitoring
        console_query = signals_with_risk.select(
            "symbol", "timestamp", "close", "volume", "signal", 
            "signal_strength", "volume_ratio", "price_change_pct",
            "stop_loss_price", "take_profit_price"
        ).writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 20) \
            .trigger(processingTime='30 seconds') \
            .start()
        
        # Optional: Write to database
        # database_query = self.write_to_database(signals_with_risk)
        
        print("Pipeline started successfully. Monitoring for trading signals...")
        console_query.awaitTermination()
    
    def write_to_database(self, signals_df):
        """Write trading signals to PostgreSQL database."""
        def write_to_postgres(batch_df, batch_id):
            """Custom function to write each batch to PostgreSQL."""
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/trading") \
                .option("dbtable", "trading_signals") \
                .option("user", "postgres") \
                .option("password", "password") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
        
        return signals_df.writeStream \
            .foreachBatch(write_to_postgres) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoint/signals") \
            .trigger(processingTime='60 seconds') \
            .start()

def main():
    """Main function to run the streaming processor."""
    processor = StockStreamProcessor()
    
    try:
        processor.run_streaming_pipeline()
    except KeyboardInterrupt:
        print("Stopping pipeline...")
    finally:
        if processor.spark:
            processor.spark.stop()
            print("Spark session closed")

if __name__ == "__main__":
    main()
