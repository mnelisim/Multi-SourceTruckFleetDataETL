from sqlalchemy import create_engine, text
from config import db_url, user, password
from utils.logger_factory import LoggerFactory 


class DatabaseSetup:
    def __init__(self):
        # 1. Initialize Logger for this class
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

    def initialize_database(self):
        clean_url = db_url.replace("jdbc:postgresql://", "")
        self.connection_uri = f"postgresql://{user}:{password}@{clean_url}"
        self.engine = create_engine(self.connection_uri)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS enriched_truck_data (
            truck_id TEXT,
            timestamp TIMESTAMP,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            speed_kmh DOUBLE PRECISION,
            status TEXT,
            last_service_date TEXT,
            engine_condition TEXT,
            tire_pressure_psi TEXT,
            driver_name TEXT,
            experience_yrs INTEGER,
            license_type TEXT
        );
        """
         # 2. Use .begin() - it handles START and COMMIT automatically
        with self.engine.begin() as conn:
            conn.execute(text(create_table_query))
            
        self.logger.info("[SUCCESS] Database Table 'enriched_truck_data' is ready.")

       