import logging
import sys
import os

class LoggerFactory:
    @staticmethod
    def get_logger(class_name):
        logger = logging.getLogger(class_name)

        if not logger.handlers:
            logger.setLevel(logging.INFO)

            formatter = logging.Formatter(
                '%(asctime)s - [%(name)s] - %(levelname)s - %(message)s'
            )

            # Console Handler (Airflow / Docker logs)
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

            # File Handler (Persistent logs)
            log_dir = "/logs"
            os.makedirs(log_dir, exist_ok=True)

            file_handler = logging.FileHandler(f"{log_dir}/pipeline.log")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger
