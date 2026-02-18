from logging.handlers import TimedRotatingFileHandler
import os, logging


def logger(service_name: str):
    log_filename = f"storage/log/{service_name}/{service_name}.log"
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)

    logger = logging.getLogger(service_name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    file_handler = TimedRotatingFileHandler(
        filename=log_filename,
        when="midnight",
        interval=1,
        backupCount=3,
        encoding="utf-8"
    )

    console_handler = logging.StreamHandler()

    formatter = CustomFormatter(service_name)

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


class CustomFormatter(logging.Formatter):
    def __init__(self, service_name):
        super().__init__()
        self.service_name = service_name.upper()

    def format(self, record):
        filename = record.filename
        full_path = os.path.relpath(record.pathname).replace("\\", "/")
        timestamp = self.formatTime(record, "%d-%m-%Y %H:%M:%S")
        return (
            f"[{record.levelname} | {filename} | {full_path} | "
            f"{self.service_name} | {timestamp}] {record.getMessage()}"
        )
