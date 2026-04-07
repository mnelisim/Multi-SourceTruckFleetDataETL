
#import pandas as pd
from utils.logger_factory import LoggerFactory 
import traceback

class DatabaseSink:

    def write_to_postgres(self, batch_df, batch_id):
        """Sinks micro-batches into PostgreSQL."""

        #logger = LoggerFactory.get_logger("DatabaseSinkWorker")
        # 1. Extract to LOCAL variables so Spark can serialize them easily
        # db_user = user
        # db_pass = password
        # url = db_url
         # Local logger to avoid Driver-to-Worker serialization issues
        logger = LoggerFactory.get_logger("DatabaseSinkWorker")
        
        # PERSIST the dataframe so count() and save() use the same data
        batch_df.persist()
        
        row_count = batch_df.count()
        logger.info(f"Processing Micro-Batch ID: {batch_id} | Rows: {row_count}")

        if row_count == 0:
            logger.info(f"Batch {batch_id} is empty. Skipping.")
            batch_df.unpersist()
            return
        #logger.info(f"Processing Micro-Batch ID: {batch_id} | Rows: {batch_df.count()}")
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", "truck_db") \
                .option("dbtable", "enriched_truck_data") \
                .option("user", "postgres") \
                .option("password", "29902363") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
        except Exception as e:
            logger.error(f"[DATABASE ERROR] Failed to load batch {batch_id}: {e}")
            logger.error(traceback.format_exc())
        finally:
            # ALWAYS unpersist to free up Spark memory
            batch_df.unpersist()
    