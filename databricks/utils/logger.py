# logger.py
# Simple structured logger that writes pipeline audit records to Delta.
# Each step gets timed and logged with success/failure status.

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)


class PipelineLogger:
    """
    Writes audit log entries to a Delta table.
    Tracks start/end times, row counts, and errors for each pipeline step.
    Falls back to stdout if Delta writes fail.
    """

    LOG_SCHEMA = StructType([
        StructField("pipeline_name", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("step_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("row_count", IntegerType(), True),
        StructField("error_message", StringType(), True),
        StructField("start_time", TimestampType(), False),
        StructField("end_time", TimestampType(), True),
        StructField("duration_sec", IntegerType(), True),
        StructField("layer", StringType(), True),
    ])

    def __init__(self, spark, log_path, pipeline_name, run_id=None):
        self.spark = spark
        self.log_path = log_path
        self.pipeline_name = pipeline_name
        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self._entries = []

    def log_start(self, step_name, layer=""):
        """Mark the beginning of a step so we can time it later."""
        entry = {
            "step_name": step_name,
            "start_time": datetime.now(),
            "layer": layer,
        }
        self._entries.append(entry)
        print(f"[LOG] [{self.pipeline_name}] START: {step_name}")
        return entry

    def log_success(self, step_name, row_count=None, layer=""):
        """Log a successful step completion."""
        entry = self._find_entry(step_name)
        end_time = datetime.now()

        log_record = {
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "step_name": step_name,
            "status": "SUCCESS",
            "row_count": row_count,
            "error_message": None,
            "start_time": entry.get("start_time", end_time) if entry else end_time,
            "end_time": end_time,
            "duration_sec": int((end_time - entry.get("start_time", end_time)).total_seconds()) if entry else 0,
            "layer": layer,
        }

        self._write_log(log_record)
        print(f"[LOG] [{self.pipeline_name}] SUCCESS: {step_name} ({row_count} rows)")

    def log_failure(self, step_name, error, layer=""):
        """Log a step that blew up."""
        entry = self._find_entry(step_name)
        end_time = datetime.now()

        log_record = {
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "step_name": step_name,
            "status": "FAILED",
            "row_count": None,
            "error_message": str(error)[:500],
            "start_time": entry.get("start_time", end_time) if entry else end_time,
            "end_time": end_time,
            "duration_sec": int((end_time - entry.get("start_time", end_time)).total_seconds()) if entry else 0,
            "layer": layer,
        }

        self._write_log(log_record)
        print(f"[LOG] [{self.pipeline_name}] FAILED: {step_name} — {error[:100]}")

    def log_warning(self, step_name, message, layer=""):
        """Log something that's not an error but worth noting."""
        log_record = {
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "step_name": step_name,
            "status": "WARNING",
            "row_count": None,
            "error_message": message[:500],
            "start_time": datetime.now(),
            "end_time": datetime.now(),
            "duration_sec": 0,
            "layer": layer,
        }
        self._write_log(log_record)
        print(f"[LOG] [{self.pipeline_name}] WARNING: {step_name} — {message[:100]}")

    def _find_entry(self, step_name):
        """Look up when a step was started."""
        for entry in reversed(self._entries):
            if entry["step_name"] == step_name:
                return entry
        return {}

    def _write_log(self, record):
        """Persist a log record to Delta. Falls back to print if that fails."""
        try:
            df = self.spark.createDataFrame([record], self.LOG_SCHEMA)
            df.write.format("delta").mode("append").save(self.log_path)
        except Exception as e:
            # Fallback: print to stdout if Delta write fails
            print(f"[LOG-FALLBACK] Could not write to Delta: {e}")
            print(f"[LOG-FALLBACK] Record: {record}")

    def get_run_summary(self):
        """Summarize how the current run went."""
        try:
            logs_df = self.spark.read.format("delta").load(self.log_path)
            run_logs = logs_df.filter(
                (logs_df.run_id == self.run_id) &
                (logs_df.pipeline_name == self.pipeline_name)
            )

            total = run_logs.count()
            success = run_logs.filter(run_logs.status == "SUCCESS").count()
            failed = run_logs.filter(run_logs.status == "FAILED").count()
            warnings = run_logs.filter(run_logs.status == "WARNING").count()

            return {
                "run_id": self.run_id,
                "total_steps": total,
                "success": success,
                "failed": failed,
                "warnings": warnings,
            }
        except Exception:
            return {"run_id": self.run_id, "error": "Could not read logs"}
