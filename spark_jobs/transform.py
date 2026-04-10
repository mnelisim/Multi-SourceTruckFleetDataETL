import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, trim, upper
from pyspark.sql.types import StructType, StringType, DoubleType
from utils.logger_factory import LoggerFactory 
from config import db_url, user, password


class TruckTransformer:
    def __init__(self):
        # 1. Initialize Logger for this class
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        self.db_url = db_url
        self.user = user
        self.password = password

        # 2. Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("RealTimeTruckFleetETL") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.2") \
            .getOrCreate()
        
        self.logger.info("Spark Session initialized successfully.")

        # 3. Define Schema
        self.schema = StructType() \
            .add("truck_id", StringType()) \
            .add("timestamp", StringType()) \
            .add("latitude", DoubleType()) \
            .add("longitude", DoubleType()) \
            .add("speed_kmh", DoubleType()) \
            .add("status", StringType())

    def _read_static_data(self):
        """Reads maintenance and driver data from local storage."""
        self.logger.info("Loading static dimension data...")
        m_df = self.spark.read.csv("/opt/spark_jobs/data/maintenance_logs.csv", header=True)
        d_df = self.spark.read.option("multiLine", "true").json("/opt/spark_jobs/data/drivers.json")
        
        # Clean IDs immediately
        m_df = m_df.withColumn("truck_id", upper(trim(col("truck_id"))))
        d_df = d_df.withColumn("truck_id", upper(trim(col("truck_id"))))
        
        return m_df, d_df

    # 4. FUNCTION to write each micro-batch to Postgres
    def write_to_postgres(self, batch_df, batch_id):
        # This part runs for every new set of records arriving in Kafka
         # 1. Force the import INSIDE the function
        from config import db_url, user, password
        from utils.alerts import alert_on_empty_batch
        
        # 2. THE CRITICAL CHECK: If any of these are None, Spark/Java will crash
        if db_url is None or user is None or password is None:
            print(f"[CRITICAL ERROR] Batch {batch_id}: Database credentials are NULL.")
            print(f"Check if .env is mapped to the container. URL: {db_url}, User: {user}")
            return # Stop here to prevent the NullPointerException
        
         # 1. Get the count of validated records in this batch
        valid_count = batch_df.count()
        self.logger.info(f"Batch {batch_id}: Processing {valid_count} validated records.")

        # 2. Trigger Slack alert if the batch is empty (potential data source issue)
        if valid_count == 0:
            alert_on_empty_batch(batch_id)
            return  # Skip the write if there's no data

        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", db_url) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .option("dbtable", "enriched_truck_data") \
                .mode("append") \
                .save()
            self.logger.info(f"Batch {batch_id} successfully written to Postgres.")
        except Exception as e:
            print(f"Database write failed for batch {batch_id}: {e}")
        
    def run_pipeline(self):
        """Executes the main streaming transformation and join."""
        self.logger.info("Starting Streaming Pipeline...")
        
        # 1. Read Stream
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "raw_truck_data") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

        # 2. Load Dimensions
        m_df, d_df = self._read_static_data()

        # 3a. Transform & Clean
        df_parsed = df.select(from_json(col("value").cast("string"), self.schema).alias("data")).select("data.*")
        
        df_cleaned = df_parsed.dropna(subset=["truck_id", "latitude", "longitude"]) \
            .filter((col("latitude") != 0) & (col("longitude") != 0)) \
            .withColumn("timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("truck_id", upper(trim(col("truck_id"))))
        
        # 3b. Data Quality Validation (Speed and Latitude range checks)
        df_final = df_cleaned.filter(
            (col("speed_kmh") >= 0) & (col("speed_kmh") <= 160) &
            (col("latitude").between(-90, 90)) & 
            (col("longitude").between(-180, 180))
        )

        # 4. Join
        enriched_df = df_final.join(m_df, "truck_id").join(d_df, "truck_id")

        # 5. Start Query
        query = enriched_df.writeStream \
            .foreachBatch(self.write_to_postgres) \
            .option("checkpointLocation", "/opt/spark_jobs/checkpoints/truck_fleet") \
            .trigger(processingTime='2 seconds') \
            .start()

        query.awaitTermination()

if __name__ == "__main__":
    transformer = TruckTransformer()
    transformer.run_pipeline()
