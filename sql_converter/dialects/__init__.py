"""
SQL Dialect handlers.

This module provides access to different dialect handlers for SQL conversion.
"""

from typing import Dict, Any, Type

# Import all dialect handlers
from .oracle import OracleDialect
from .pyspark import PySparkDialect
from .mysql import MySQLDialect
from .postgresql import PostgreSQLDialect

# Map of dialect names to dialect handler classes
DIALECT_HANDLERS = {
    'oracle': OracleDialect,
    'pyspark': PySparkDialect,
    'mysql': MySQLDialect,
    'postgresql': PostgreSQLDialect,
    'postgres': PostgreSQLDialect,  # Alias for postgresql
}

def get_dialect_handler(dialect: str):
    """
    Get the dialect handler for the specified dialect.
    
    Args:
        dialect (str): The SQL dialect name
        
    Returns:
        The dialect handler instance
        
    Raises:
        ValueError: If the dialect is not supported
    """
    dialect = dialect.lower()
    
    if dialect not in DIALECT_HANDLERS:
        supported = ", ".join(DIALECT_HANDLERS.keys())
        raise ValueError(f"Unsupported dialect: {dialect}. Supported dialects: {supported}")
    
    return DIALECT_HANDLERS[dialect]()

def get_supported_dialects() -> list:
    """
    Get a list of supported SQL dialects.
    
    Returns:
        list: List of supported dialect names
    """
    # Deduplicate the list (because 'postgresql' and 'postgres' are aliases)
    return sorted(set(DIALECT_HANDLERS.keys()))

__all__ = ['get_dialect_handler', 'get_supported_dialects', 
           'OracleDialect', 'PySparkDialect', 'MySQLDialect', 'PostgreSQLDialect']
