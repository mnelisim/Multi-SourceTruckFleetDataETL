import pandas as pd
from sqlalchemy import create_engine
from config import db_url, user, password
import os
from utils.logger_factory import LoggerFactory 

class BigQueryLoader:
    def __init__(self):
        # 1. Initialize Logger for this class
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        
        # Convert JDBC URL to SQLAlchemy format
        clean_url = db_url.replace("jdbc:postgresql://", "")
        self.connection_uri = f"postgresql://{user}:{password}@{clean_url}"
        
        self.engine = create_engine(self.connection_uri)
        self.output_parquet = "data/truck_data_gold.parquet"

    def extract_silver_to_parquet(self):
        """Task: Pull from Postgres, save as Parquet for BigQuery"""
        self.logger.info("[GOLD LAYER] Extracting from Postgres to Parquet...")
        try:
            # Ensure the 'data' directory exists
            os.makedirs(os.path.dirname(self.output_parquet), exist_ok=True)
            
            #df = pd.read_sql("SELECT * FROM enriched_truck_data", self.engine)

            # --- START CHUNK LOGIC ---
            # 1. Open the stream with a chunksize
            chunks = pd.read_sql("SELECT * FROM enriched_truck_data", self.engine, chunksize=50000)
            
            total_rows = 0
            for i, chunk in enumerate(chunks):
                # 2. On the first chunk (i=0), create/overwrite the file. 
                # On others, append.
                if i == 0:
                    # Write the first chunk
                    chunk.to_parquet(self.output_parquet, engine='fastparquet', index=False)
                else:
                    # Append subsequent chunks
                    chunk.to_parquet(self.output_parquet, engine='fastparquet', index=False, append=True)
                
                total_rows += len(chunk)
                self.logger.info(f"Processed chunk {i+1}... Total rows so far: {total_rows}")
            # --- END CHUNK LOGIC ---

            if total_rows == 0:
                self.logger.info("[WARNING] No data found in PostgreSQL 'enriched_truck_data' table.")
                return None

            #df.to_parquet(self.output_parquet, index=False)
            self.logger.info(f"[SUCCESS] Staged {total_rows} records at {self.output_parquet}")
            return self.output_parquet
            
        except Exception as e:
            self.logger.error(f"[ERROR] Extraction failed: {e}")
            return None
        
    def load_to_sandbox(self):
        """Final Step: Instructions for the Free Cloud Upload"""
        if os.path.exists(self.output_parquet):
            self.logger.info("\n" + "="*50)
            self.logger.info("[GOLD LAYER] DATA IS READY FOR BIGQUERY!")
            self.logger.info(f"File Location: {os.path.abspath(self.output_parquet)}")
            self.logger.info("="*50)
            self.logger.info("\nINSTRUCTIONS (100% FREE):")
            self.logger.info("1. Open BigQuery Console (Sandbox mode).")
            self.logger.info("2. Select 'fleet_analytics' dataset.")
            self.logger.info("3. Click 'Create Table' -> Source: 'Upload'.")
            self.logger.info(f"4. Select '{self.output_parquet}'.")
            self.logger.info("5. Check 'Auto detect' for Schema.")
            self.logger.info("6. Click 'Create Table' to land data in the Cloud!")
            self.logger.info("="*50)
        else:
            self.logger.info("[ERROR] No Parquet file found. Run extraction first.")

if __name__ == "__main__":
    # 1. Initialize the class
    loader = BigQueryLoader()
    
    # 2. Run the extraction
    loader.extract_silver_to_parquet()

     # 2. Run the extraction (This handles the 1,000,000 rows in chunks)
    parquet_path = loader.extract_silver_to_parquet()
    
    # 3. Final Verification 
    if parquet_path:
        import pandas as pd
        # Use 'fastparquet' to just peek at the metadata
        # This doesn't load the million rows, so it's super fast.
        df_peek = pd.read_parquet(parquet_path, engine='fastparquet')
        
        print("\n" + "="*50)
        print("FINAL QUALITY CHECK:")
        print(f"Total Rows Captured: {len(df_peek):,}") # Adds commas like 1,000,000
        print(f"Columns Verified: {list(df_peek.columns)}")
        print("="*50)
        
        # 4. Print the instructions for the Cloud
        loader.load_to_sandbox()
    else:
        print("[CRITICAL] Extraction failed. Check the logs above.")
   

