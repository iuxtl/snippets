import logging
import traceback
import boto3
import watchtower
from aws_xray_sdk.core import patch_all, xray_recorder

class CloudwatchLogger:
    _loggers = {}  # Class-level dictionary to track initialized loggers

    def __new__(cls, level=logging.INFO, name=__name__):
        # Ensure only one logger instance per name
        if name not in cls._loggers:
            cls._loggers[name] = super().__new__(cls)
            cls._loggers[name]._initialized = False
        return cls._loggers[name]

    def __init__(self, level=logging.INFO, name=__name__):
        if self._initialized:
            return
        self._initialized = True

        self.logger = logging.getLogger(name)
        self.set_log_level(level)
        self.logger.setLevel(logging.DEBUG)
        self.logger_name = name
        xray_recorder.configure(sampling=True)
        patch_all()

        # Set up a basic configuration for the logging
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

            # Configure AWS logging handler
            try:
                cloudwatch_logs = boto3.client("logs")
                aws_handler = watchtower.CloudWatchLogHandler(
                    log_group="ReefInfoIngestionLogs", boto3_client=cloudwatch_logs
                )
                self.logger.addHandler(aws_handler)
            except Exception as e:
                self.logger.error(
                    f"Failed to configure AWS logging for the service {name}, defaulting to local log: {e}"
                )

            # Configure file handler
            file_handler = logging.FileHandler("Slack_Assistant.log")
            file_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)

        self.logger.propagate = False  # Prevent the log messages from being printed twice

    def set_log_level(self, level):
        if isinstance(level, str):
            level = level.upper()
            level = getattr(logging, level, logging.INFO)
        self.logger.setLevel(level)

    def log(self, level, message):
        if isinstance(level, str):
            level = level.upper()
            level = getattr(logging, level, logging.INFO)
        self.logger.log(level, message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)

    def critical(self, message):
        self.logger.critical(message)

    def warning(self, message):
        self.logger.warning(message)

    def analytics(self, start_time, end_time, message):
        analytics_object = {
            "start_time": start_time,
            "end_time": end_time,
            "execution_time": end_time - start_time,
            "message": message,
        }
        self.logger.info(analytics_object)

    def log_exception(self, message="An exception occurred"):
        """
        Logs an exception with its traceback.
        """
        self.logger.error(message)
        self.logger.error(traceback.format_exc())

    def trace_segment(self, segment_name, annotations=None):
        """Create a new segment in AWS X-Ray."""
        segment = xray_recorder.begin_segment(segment_name)
        if annotations:
            for key, value in annotations.items():
                segment.put_annotation(key, value)
        return segment

    def end_trace_segment(self):
        """End the current segment in AWS X-Ray."""
        if xray_recorder.current_segment():
            xray_recorder.end_segment()
