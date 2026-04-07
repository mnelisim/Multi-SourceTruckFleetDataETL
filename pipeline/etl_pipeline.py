from storage.bq_loader import BigQueryLoader
from ingestion.data_fetching import Producer
from utils.logger_factory import LoggerFactory 


class ETLpipeline:
    def __init__(self):
        # 1. Initialize Logger for this class
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        # Initialise the layers
        self.bq_loader = BigQueryLoader()
    
    def run_batch_load(self):
        """Task 3: Batch Load (Postgres to BigQuery)"""
        self.logger.info("[TASK 3] Moving Silver (Postgres) to Gold (BigQuery)...")
        self.bq_loader.extract_silver_to_parquet()
        self.bq_loader.load_to_sandbox() # Instructions for GCP upload

if __name__ == "__main__":
    pipeline = ETLpipeline()

