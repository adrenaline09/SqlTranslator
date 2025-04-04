"""
SQL Dialect Converter

A Python package that converts SQL queries between different database dialects
(Oracle, PySpark, MySQL, PostgreSQL).
"""

__version__ = "0.1.0"

from .simple_converter import convert_sql, batch_convert
from .dependency_analyzer import analyze_sql_batch

# Define supported dialects
SUPPORTED_DIALECTS = ["mysql", "postgresql", "oracle", "pyspark"]

def get_supported_dialects():
    """Return a list of supported SQL dialects."""
    return SUPPORTED_DIALECTS

__all__ = ["convert_sql", "batch_convert", "get_supported_dialects", "analyze_sql_batch"]
