# src/utils/logger.py
import logging
import sys
from logging.handlers import RotatingFileHandler
from src.core.config import settings # Assuming settings are loaded here

def setup_logging():
    """
    Configures logging for the application.
    Logs to console and optionally to a rotating file.
    """
    log_level_name = settings.LOG_LEVEL.upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    # Basic configuration (good for libraries, but for apps we want more control)
    # logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create a root logger instance
    # Using app_name for the logger allows specific configuration if multiple apps share this util
    logger = logging.getLogger(settings.APP_NAME)
    logger.setLevel(log_level) # Set level on the logger itself

    # Prevent duplicate handlers if called multiple times (though ideally setup once)
    if logger.hasHandlers():
        logger.handlers.clear()

    # Formatter
    log_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(process)d - %(threadName)s - %(levelname)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s"
    )

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

    # File Handler (Rotating)
    if settings.LOG_FILENAME:
        try:
            file_handler = RotatingFileHandler(
                settings.LOG_FILENAME,
                maxBytes=settings.LOG_MAX_BYTES,
                backupCount=settings.LOG_BACKUP_COUNT,
                encoding='utf-8'
            )
            file_handler.setFormatter(log_formatter)
            logger.addHandler(file_handler)
            logger.info(f"Logging to file: {settings.LOG_FILENAME}")
        except Exception as e:
            logger.error(f"Failed to configure file logger for {settings.LOG_FILENAME}: {e}", exc_info=True)


    # Configure other library loggers if needed (e.g., uvicorn, httpx)
    # Example: Quieting overly verbose libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    # logging.getLogger("uvicorn.access").setLevel(logging.WARNING) # Already handled by our middleware usually

    logger.info(f"Logging setup complete. Application log level set to: {log_level_name}")

# Call setup_logging() when this module is imported if you want it to configure immediately.
# However, it's often better to call it explicitly from main.py or app.__init__.
# For now, it's a function to be called.
# Example: In main.py:
# from src.utils.logger import setup_logging
# setup_logging()

# To get a logger instance elsewhere:
# import logging
# from src.core.config import settings
# logger = logging.getLogger(settings.APP_NAME)
# logger.info("This is a test log message.")
